package raft

import raft.Message.*
import raft.state.Follower
import raft.state.MainDataHolder

/**
 * Raft algorithm implementation.
 * All functions are called from the single main thread.
 *
 * @author Aslan Kuliev
 */
class ProcessImpl(private val env: Environment) : Process {
    private val storage = env.storage
    private val machine = env.machine

    private var stateHolder: MainDataHolder = MainDataHolder().apply {
        state = Follower.init(this, env)
    }

    override fun onTimeout() = stateHolder.state.onTimeout()

    override fun onMessage(srcId: Int, message: Message) = with(stateHolder.state) {
        when (message) {
            is AppendEntryResult -> handleAppendEntryResult(srcId, message)
            is AppendEntryRpc -> handleAppendEntryRpc(srcId, message)
            is ClientCommandResult -> handleClientCommandResult(srcId, message)
            is ClientCommandRpc -> handleClientCommandRpc(srcId, message)
            is RequestVoteResult -> handleRequestVoteResult(srcId, message)
            is RequestVoteRpc -> handleRequestVoteRpc(srcId, message)
        }
    }

    override fun onClientCommand(command: Command) = stateHolder.state.onClientCommand(command)
}