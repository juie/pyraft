from functools import wraps

import raft.protobuf.raft_pb2 as struct
import raft.protobuf.raft_pb2_grpc as service
from raft.state import RaftIdentityState


def capture_exception():
    def capture(func):
        @wraps(func)
        def captured_function(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                print("Rpc:   [ {} ]请求服务器出错".format(func.__name__))

        return captured_function

    return capture


class RpcService(service.ServeRaftServicer):
    def __init__(self, server):
        self.s = server

    def ping(self, request, context):
        return struct.Pong()

    def request_vote(self, request, context):
        _term = request.term
        _candidateId = request.candidateId
        _lastLogIndex = request.lastLogIndex
        _lastLogTerm = request.lastLogTerm
        with self.s.lock:
            if _term > self.s.current_term:
                self.s.current_term = _term
                self.s.state = RaftIdentityState.FOLLOWER
                self.s.vote_for = _candidateId
                return struct.Vote(term=_term, voteGranted=True)
            elif self.s.vote_for is not None or self.s.vote_for != _candidateId:
                return struct.Vote(term=self.s.current_term, voteGranted=False)
            last_log_index, last_log_term = self.s.get_last_info()
            if _lastLogIndex < last_log_index or _lastLogTerm < last_log_term:
                return struct.Vote(term=self.s.current_term, voteGranted=False)
            self.s.vote_for = _candidateId
            return struct.Vote(term=_term, voteGranted=True)

    def append_entries(self, request, context):
        _term = request.term
        _leaderId = request.leaderId
        entries = request.entries
        with self.s.lock:
            self.s.leader = _leaderId
            self.s.current_term = _term
            self.s.state = RaftIdentityState.FOLLOWER
        return struct.ResAppendEntries(term=_term, success=True)
