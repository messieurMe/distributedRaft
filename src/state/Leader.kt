package raft.state

import raft.*
import raft.Message.*
import raft.Timeout.LEADER_HEARTBEAT_PERIOD
import java.util.*

class Leader : BaseState {
    constructor(prevState: BaseState) : super(prevState.mainDataHolder, prevState.env)

    private val nextIndex = HashMap<Int, Int>(env.nProcesses).apply {
        (1..env.nProcesses).forEach { i -> if (i != env.processId) put(i, lastIndex) }
    }
    private val matchIndex = TreeMap<Int, Int?>().apply {
        (1..env.nProcesses).forEach { i -> if (i != env.processId) put(i, null) }
    }

    private var initIndex = storage.readLastLogId()

    init {
        commandsToCommit.forEach { storage.appendLogEntry(LogEntry(LogId(lastLogId.index + 1, term), it)) }
        storage.writePersistentState(PersistentState(term, env.processId))

        for (i in 1..env.nProcesses) if (i != env.processId) {
            env.send(i, AppendEntryRpc(term, lastLogId, commitIndex, null))
        }
        env.startTimeout(LEADER_HEARTBEAT_PERIOD)
    }

    private var waitingForResponse = false

    private fun leaderBroadcast() {
        nextIndex.forEach { (pId, pLastIndex) ->
            val message = if (pLastIndex == lastIndex) {
                null
            } else {
                storage.readLog(pLastIndex + 1)
            }
            val prevLog = storage.readLog(pLastIndex)?.id ?: START_LOG_ID

            env.send(pId, AppendEntryRpc(term, prevLog, commitIndex, message))
        }
    }

    override fun onTimeout() {
        leaderBroadcast()
        env.startTimeout(LEADER_HEARTBEAT_PERIOD)
    }

    override fun onClientCommand(command: Command) {
        val newLogId = LogId(lastIndex + 1, term)
        storage.appendLogEntry(LogEntry(newLogId, command))
        if (!waitingForResponse) {
            leaderBroadcast().also { waitingForResponse = true }
        }
    }

    override fun handleAppendEntryResult(srcId: Int, message: AppendEntryResult) {
        if (message.term > term) {
            changeState(Follower(null, message.term, false, true, this))
            return
        }

        if (message.lastIndex == null) {
            nextIndex[srcId] = nextIndex[srcId]!! - 1

            val srcIdIndex = nextIndex[srcId]!!
            val newMessage = storage.readLog(srcIdIndex + 1)
            val prevLog = storage.readLog(srcIdIndex)?.id ?: START_LOG_ID
            env.send(srcId, AppendEntryRpc(term, prevLog, commitIndex, newMessage))
        } else {
            if (matchIndex[srcId] == null) {
                matchIndex[srcId] = message.lastIndex
                nextIndex[srcId] = message.lastIndex
            } else {
                nextIndex[srcId] = message.lastIndex
                matchIndex[srcId] = message.lastIndex
                newCommit()
            }
        }
    }

    private fun newCommit() {
        val isInCurrTerm = lastLogId.index > initIndex.index
                && (matchIndex.count { (_, v) -> (v ?: -1) > initIndex.index } >= env.nProcesses / 2)
        while (isInCurrTerm && matchIndex.count { (_, v) -> (v ?: -1) > commitIndex } >= (env.nProcesses / 2)) {
            commitIndex += 1
            val logToCommit = storage.readLog(commitIndex)!!
            val result = machine.apply(logToCommit.command)
            waitingForResponse = false
            if (storage.readLog(commitIndex)!!.id.term == term) {
                if (env.processId == logToCommit.command.processId) {
                    env.onClientCommandResult(result)
                } else {
                    env.send(logToCommit.command.processId, ClientCommandResult(term, result))
                }
            }
        }
    }

    override fun handleAppendEntryRpc(srcId: Int, message: AppendEntryRpc) {
        val isNewLeader = message.term > term && message.prevLogId >= lastLogId
        if (isNewLeader) {
            changeState(Follower(srcId, message.term, false, true, this)).apply {
                handleAppendEntryRpc(srcId, message)
            }
        } else {
            env.send(srcId, AppendEntryResult(term, null))
        }
    }

    override fun handleClientCommandResult(srcId: Int, message: ClientCommandResult) {
        if (message.term <= term) {
            env.onClientCommandResult(message.result)
        } else {
            changeState(Follower(null, message.term, false, true, this))
        }
    }

    override fun handleClientCommandRpc(srcId: Int, message: ClientCommandRpc) = onClientCommand(message.command)

    override fun handleRequestVoteResult(srcId: Int, message: RequestVoteResult) = Unit /* ignore */
}