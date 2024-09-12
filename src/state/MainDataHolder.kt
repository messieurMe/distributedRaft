package raft.state

import raft.Command
import java.util.*

class MainDataHolder {
    lateinit var state: BaseState
    var commandsToCommit = LinkedList<Command>()
    var commitIndex: Int = 0
}