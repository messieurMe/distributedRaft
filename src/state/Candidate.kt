package raft.state

import raft.Command
import raft.Message.*
import raft.PersistentState
import raft.Timeout

class Candidate(prevState: BaseState) : BaseState(prevState.mainDataHolder, prevState.env) {

    private var votesGranted: Int = 1
    private val quorum = env.nProcesses
    private val elected
        get() = votesGranted > quorum / 2

    private val voters = (1..env.nProcesses) - env.processId

    private fun startElection() {
        persistentState = PersistentState(term + 1, env.processId)
        voters.forEach { i -> env.send(i, RequestVoteRpc(term, lastLogId)) }
        env.startTimeout(Timeout.ELECTION_TIMEOUT)
    }

    init {
        startElection()
    }

    override fun onTimeout() = changeState(Candidate(this)).ignore()

    override fun onClientCommand(command: Command) = commandsToCommit.add(command).ignore()

    override fun handleAppendEntryResult(srcId: Int, message: AppendEntryResult) = Unit /* ignore */

    override fun handleAppendEntryRpc(srcId: Int, message: AppendEntryRpc) {
        if (message.term >= term && message.prevLogId >= lastLogId) {
            changeState(Follower(srcId, message.term, false, false, this)).apply {
                handleAppendEntryRpc(srcId, message)
            }
        } else {
            env.send(srcId, AppendEntryResult(term, null))
        }
    }

    override fun handleClientCommandResult(srcId: Int, message: ClientCommandResult) =
        env.onClientCommandResult(message.result)

    override fun handleClientCommandRpc(srcId: Int, message: ClientCommandRpc) =
        commandsToCommit.add(message.command).ignore()

    override fun handleRequestVoteResult(srcId: Int, message: RequestVoteResult) {
        if (message.voteGranted) {
            votesGranted++
            if (elected) {
                changeState(Leader(this))
            }
        } else if (message.term > term) {
            changeState(Follower(null, message.term, true, true, this))
        }
    }
}