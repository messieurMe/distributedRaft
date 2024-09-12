package raft.state

import raft.Command
import raft.Environment
import raft.Message.*
import raft.PersistentState
import raft.START_LOG_ID
import raft.Timeout.ELECTION_TIMEOUT

class Follower : BaseState {
    private constructor(stateHolder: MainDataHolder, env: Environment) : super(stateHolder, env)

    constructor(following: Int?, term: Int, viaElection: Boolean, writePS: Boolean, prevState: BaseState) : super(
        prevState.mainDataHolder, prevState.env
    ) {
        this.following = following
        if (writePS) {
            persistentState = PersistentState(term, if (viaElection) following else null)
        }
    }

    var following: Int? = null

    init {
        resetTimeout()
    }

    companion object {
        fun init(stateHolder: MainDataHolder, env: Environment): Follower = Follower(stateHolder, env)
    }

    private fun resetTimeout() {
        env.startTimeout(ELECTION_TIMEOUT)
    }

    override fun onTimeout() {
        mainDataHolder.state = Candidate(this)
    }

    private fun sendCommands() {
        if (following == null) return
        with(mainDataHolder.commandsToCommit) {
            forEach { env.send(following!!, ClientCommandRpc(term, it)) }.also { clear() }
        }
    }

    override fun onClientCommand(command: Command) {
        if (following != null) {
            env.send(following!!, ClientCommandRpc(term, command))
        } else {
            mainDataHolder.commandsToCommit.add(command)
        }
    }

    override fun handleAppendEntryResult(srcId: Int, message: AppendEntryResult) = Unit /* ignore */

    override fun handleAppendEntryRpc(srcId: Int, message: AppendEntryRpc) {
        when {
            message.term > term -> {
                changeState(Follower(srcId, message.term, false, true, this)).apply {
                    handleAppendEntryRpc(srcId, message)
                }
            }
            message.term < term -> {
                env.send(srcId, AppendEntryResult(term, null))
            }
            message.term == term -> {
                resetTimeout()

                if (following == null) {
                    following = srcId
                }

                val messagePrevLog = message.prevLogId.index
                val log = storage.readLog(messagePrevLog)?.id ?: if (messagePrevLog == 0) START_LOG_ID else null
                val logMatches = log?.let { it == message.prevLogId } ?: false

                if (logMatches) {
                    message.entry?.let { storage.appendLogEntry(it) }

                    while (commitIndex < message.leaderCommit && lastLogId.index >= message.leaderCommit) {
                        commitIndex++
                        val toCommit = storage.readLog(commitIndex) ?: error("Inconsistent state")
                        machine.apply(toCommit.command)
                    }
                }
                env.send(srcId, AppendEntryResult(term, if (logMatches) lastLogId.index else null))

                with(mainDataHolder.commandsToCommit) {
                    forEach { env.send(srcId, ClientCommandRpc(term, it)) }.also { clear() }
                }
            }
        }
    }

    override fun handleClientCommandResult(srcId: Int, message: ClientCommandResult) {
        env.onClientCommandResult(message.result)
        if (following == null && term != message.term) {
            changeState(Follower(srcId, message.term, false, true, this)).apply {
                sendCommands()
            }
        }
    }

    override fun handleClientCommandRpc(srcId: Int, message: ClientCommandRpc) = onClientCommand(message.command)

    override fun handleRequestVoteResult(srcId: Int, message: RequestVoteResult) = Unit /* ignore */
}
