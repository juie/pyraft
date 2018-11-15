import grpc
import time
from concurrent import futures
from threading import Lock

from state import State
from raft import RaftPlugin
from services import Service, service, store
from log import logger


class Node(object):
    def __init__(self, identity, peers, timeout=3):
        self._id = identity
        self._peers = peers
        self._state = State.FOLLOWER
        self._leader = None
        self._vote_for = None
        self._curr_term = 0
        self._last_log_index = 0
        self._last_log_term = 0
        self._timeout = timeout
        self._log = logger

        self._state_lock = Lock()

        self.plugin = RaftPlugin(self)

    @property
    def identity(self):
        return self._id

    @property
    def peers(self):
        return self._peers

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, _state):
        self._state = _state

    @property
    def leader(self):
        return self._leader

    @leader.setter
    def leader(self, _leader):
        self._leader = _leader

    @property
    def voteFor(self):
        return self._vote_for

    @voteFor.setter
    def voteFor(self, _vote_for):
        self._vote_for = _vote_for

    @property
    def currentTerm(self):
        return self._curr_term

    @currentTerm.setter
    def currentTerm(self, _term):
        self._curr_term = _term

    @property
    def lastLogTerm(self):
        return self._last_log_term

    @property
    def lastLogIndex(self):
        return self._last_log_index

    @property
    def timeout(self):
        return self._timeout

    @property
    def stateLock(self):
        return self._state_lock

    def check_peers(self):
        for p in self._peers:
            peer_id = p["peer_id"]
            channel = grpc.insecure_channel(peer_id)
            stub = service.DistributeStub(channel)
            while True:
                try:
                    stub.ping(store.Ping())
                    break
                except Exception as e:
                    self._log.error("[ {} ]连接失败，重连".format(peer_id))
                    time.sleep(1)
                    continue
            # p["peer_id"] = peer_id
            p["stub"] = stub
            self._log.info("[{}]连接成功".format(peer_id))

    def run(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        service.add_DistributeServicer_to_server(Service(self), server)
        server.add_insecure_port(self.identity)
        self._log.debug("开启GRPC: {}".format(self.identity))
        server.start()
        self.check_peers()
        self.plugin.election()
