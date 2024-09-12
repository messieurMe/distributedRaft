package raft.state

import raft.*
import raft.Message.*
import java.lang.Integer.max

sealed class BaseState(val mainDataHolder: MainDataHolder, environment: Environment) {

    fun <T : BaseState> changeState(newState: T): T {
        mainDataHolder.state = newState
        return newState
    }

    fun <T> T.ignore() {}

    val env: Environment = environment
    val storage: Storage = env.storage
    val machine: StateMachine = env.machine

    val lastLogId: LogId
        get() = storage.readLastLogId()
    val lastIndex
        get() = lastLogId.index

    var commitIndex by mainDataHolder::commitIndex
    var commandsToCommit by mainDataHolder::commandsToCommit

    var persistentState = storage.readPersistentState()
        set(value) {
            storage.writePersistentState(value)
            field = value
        }

    val term
        get() = persistentState.currentTerm
    val votedFor
        get() = persistentState.votedFor


    abstract fun onTimeout()
    abstract fun onClientCommand(command: Command)
    abstract fun handleAppendEntryResult(srcId: Int, message: AppendEntryResult)
    abstract fun handleAppendEntryRpc(srcId: Int, message: AppendEntryRpc)
    abstract fun handleClientCommandResult(srcId: Int, message: ClientCommandResult)
    abstract fun handleClientCommandRpc(srcId: Int, message: ClientCommandRpc)
    abstract fun handleRequestVoteResult(srcId: Int, message: RequestVoteResult)

    fun handleRequestVoteRpc(srcId: Int, message: RequestVoteRpc) {
        val termNewer = message.term > term
        val logUpToDate = message.lastLogId >= lastLogId
        val voteMatches = votedFor?.let { it == srcId } ?: false

        val myVote = when {
            termNewer && logUpToDate && !voteMatches -> {
                changeState(Follower(srcId, message.term, true, true, this))
                true
            }
            termNewer && !logUpToDate -> {
                changeState(Follower(null, message.term, true, true, this))
                false
            }
            voteMatches -> true
            else -> false
        }
        env.send(srcId, RequestVoteResult(max(term, message.term), myVote))
    }
}