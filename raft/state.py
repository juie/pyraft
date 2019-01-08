class RaftIdentityState:
    LEADER = "leader"
    CANDIDATE = "candidate"
    FOLLOWER = "follower"


class RaftRunningState:
    STOPPED = "stopped"
    RUNNING = "running"
    SNAPSHOTTING = "snapshotting"
