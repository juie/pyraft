import protobuf.raft_pb2 as store
import protobuf.raft_pb2_grpc as service
from log import logger


class Service(service.DistributeServicer):
    def __init__(self, linker):
        self.linker = linker

        self._log = logger
        super(Service, self).__init__()

    def ping(self, request, context):
        return store.Pong()

    def request_vote(self, request, context):
        _term = request.term
        _candidateId = request.candidateId
        _lastLogIndex = request.lastLogIndex
        _lastLogTerm = request.lastLogTerm
        self._log.debug("GRPC: 来自 [{}] 的请求投票, TERM [{}]".format(_candidateId, _term))
        if _term < self.linker.currentTerm:
            return store.Vote(term=self.linker.currentTerm, voteGranted=False)
        elif _term > self.linker.currentTerm:
            self.linker.plugin.become_follower(None, _term)
            self.linker.voteFor = _candidateId
            return store.Vote(term=_term, voteGranted=True)
        else:
            if _lastLogIndex >= self.linker.lastLogIndex and _lastLogTerm >= self.linker.lastLogTerm and (
                    self.linker.voteFor is None or self.linker.voteFor == _candidateId):
                self.linker.voteFor = _candidateId
                return store.Vote(term=self.linker.currentTerm, voteGranted=True)
            else:
                return store.Vote(term=self.linker.currentTerm, voteGranted=False)

    def append_entries(self, request, context):
        _term = request.term
        _leaderId = request.leaderId
        entries = request.entries
        self._log.debug("GRPC: 来自 [{}] 的心跳".format(_leaderId))
        self.linker.plugin.stop_election()
        self.linker.plugin.become_follower(_leaderId, _term)
        return store.ResAppendEntries(term=_term, success=True)
