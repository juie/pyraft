import os
from threading import Lock, Thread, RLock
from queue import Queue
from logging import getLogger

from raft.state import RaftIdentityState, RaftRunningState
from raft.command import *
from raft.role import Follower, Leader, Candidate
from raft.peer import Peer
from raft.rpc import RpcService, service
from raft.error import *


class Server(object):
    def __init__(self, name, path, transporter, election_timeout, heartbeat_interval, connection_string, log=None,
                 current_term=0, peers=None, state=RaftIdentityState.FOLLOWER):
        self.name = name
        self.path = path
        self.state = self._pre_state = state
        self.connection_string = connection_string

        # Communicate with other peers
        self.transporter = transporter

        self.current_term = current_term
        self.vote_for = None

        self.leader = None

        # Thread lock
        self.lock = Lock()
        self.Rlock = RLock()

        self.requests_queue = Queue()
        self.thread_queue = Queue()
        # self.resp_queue = Queue()

        self.election_timeout = election_timeout
        self.heartbeat_interval = heartbeat_interval

        self.log = log or getLogger(__file__)

        self.log.debug("New raft server!")

        self.flag = RaftRunningState.STOPPED

        self.peers = self._convert_peers(peers)

        # Action role
        self.layers = self._load_layer()

    def _convert_peers(self, pre_members):
        peers = {}
        if pre_members:
            for n, m in pre_members.items():
                peers[n] = Peer(self, n, m)
        return peers

    def _load_layer(self):
        if self.state == RaftIdentityState.LEADER:
            return Leader(self)
        elif self.state == RaftIdentityState.CANDIDATE:
            return Candidate(self)
        elif self.state == RaftIdentityState.FOLLOWER:
            return Follower(self)
        else:
            raise RaftStateError("Unknown state: [ {} ]!".format(self.state))

    def _state_change(self):
        if self.state == self._pre_state:
            return False
        else:
            self.log.debug("State change: {}, {}".format(self.state, self._pre_state))
            self._pre_state = self.state
            return True

    def promotable(self):
        with self.lock:
            if self.leader:
                self.log.debug("Leader: {}".format(self.leader))
                return False
        return True

    def get_log_path(self):
        return os.path.join(self.path, "log")

    def get_last_info(self):
        return 0, 0

    def get_member_count(self):
        with self.lock:
            return len(self.peers.keys())

    def get_quorum_size(self):
        if self.get_member_count() == 0:
            return 3 / 2 + 1
        return (self.get_member_count() / 2) + 1

    def add_peer(self, name, connection_string):
        self.log.debug("Server: Add peer [{}] [{}]".format(name, connection_string))

        # If peer exist, do nothing
        if self.peers.get(name):
            return None

        # Ignore the peer if it has the same name as the server
        if name != self.name:
            p = Peer(self, name, connection_string)
            if self.state == RaftIdentityState.LEADER:
                p.start_heartbeat()
            self.peers[name] = p
            self.dispatch_event()
        # save config
        self.write_conf()

    def remove_peer(self, name):
        self.log.debug("Server: Remove peer [{}]".format(name))

        if name != self.name:
            peer = self.peers.get(name)
            if not peer:
                return "Peer Not Exist"
            else:
                if self.state == RaftIdentityState.LEADER:
                    # stop heartbeat
                    peer.stop_heartbeat()
                del self.peers[name]
                # send event to other peers
                self.dispatch_event()
        self.write_conf()

    def dispatch_event(self):
        pass

    def write_conf(self):
        pass

    def process_vote_resp(self, resp):
        if resp.voteGranted and resp.term == self.current_term:
            return True
        elif resp.term > self.current_term:
            with self.lock:
                self.current_term = resp.term
                self.state = RaftIdentityState.FOLLOWER
        else:
            return False

    def process_command(self, command):
        if isinstance(command, NOPCommand):
            self.log.debug("Execute NOP command!")
            for p in self.peers.values():
                p.start_heartbeat()
            return True
        elif isinstance(command, StopLeader):
            self.log.debug("Execute StopLeader command!")
            for p in self.peers.values():
                p.stop_heartbeat()
            return True
        elif isinstance(command, DefaultJoinCommand):
            self.log.debug("Execute DefaultJoin command!")
            with self.lock:
                if self.state == RaftIdentityState.LEADER:
                    return self.add_peer(command.name, command.connect_string)
                elif self.leader:
                    return self.peers[self.leader].connection_string
                else:
                    return False
        elif isinstance(command, SelfJoinCommand):
            self.log.debug("Execute SelfJoin command!")
            with self.lock:
                if self.state != RaftIdentityState.LEADER and self.leader is None:
                    self.state = RaftIdentityState.LEADER
            return True
        else:
            self.log.error("Unknown Command!")
            return False

    # Execute a command and replicate it.
    # This function will block until the command has been successfully committed or an error has occurred.
    def execute(self, command):
        if not isinstance(command, Command):
            return ""
        return self.process_command(command)

    def _serve(self):
        service.add_ServeRaftServicer_to_server(RpcService(self), self.transporter)
        self.transporter.add_insecure_port(self.connection_string)
        self.transporter.start()
        while self.flag != RaftRunningState.STOPPED:
            if self._state_change():
                self.layers.stop_loop()
                self.layers = self._load_layer()
            self.layers.start_loop()

    def start(self):
        self.log.info("Start raft server!")
        with self.lock:
            if self.flag != RaftRunningState.STOPPED:
                return
            self.flag = RaftRunningState.RUNNING
            thread = Thread(target=self._serve, )
            thread.daemon = True
            thread.start()

    def stop(self):
        with self.lock:
            if self.flag == RaftRunningState.STOPPED:
                return
            while not self.thread_queue.empty():
                t = self.thread_queue.get()
                self.thread_queue.task_done()
                t.join()
            self.requests_queue = Queue()

            self.flag = RaftRunningState.STOPPED
