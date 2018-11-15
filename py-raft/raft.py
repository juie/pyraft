from queue import Queue
from threading import Timer
import random
import time

from util import spawn
from log import logger
from state import State
from protobuf import raft_pb2 as store


class VoteTicket(object):
    def __init__(self, vote_for, vote_from, vote_term):
        self.voteFor = vote_for
        self.voteFrom = vote_from
        self.voteTerm = vote_term


class RaftPlugin(object):
    ELECTION_SUCCESS = 100
    ELECTION_FAILED = 101
    ELECTION_TIMEOUT = 102
    ELECTION_STOP = 103

    def __init__(self, receiver, debug=True):
        self.r = receiver
        self.debug = debug

        self._log = logger
        self._curr_timer = None
        self._votes_count = 0
        self._valid_peers = []

        self._stop_election_flag = False
        self._timeout_flag = False

        self._receive_vote_queue = Queue()
        self._event_queue = Queue()

    def _gen_timeout(self):
        t_min, t_max = self.r.timeout / 10, self.r.timeout
        t_out = t_min + (t_max - t_min) * random.random()
        self._log.debug("生成随机时间: [ {} ]s".format(t_out))
        return t_out

    def _timeout_stop(self):
        with self.r.stateLock:
            if self._timeout_flag:
                return
            self._timeout_flag = True

    def _check_valid_peers(self):
        self._valid_peers = []
        with self.r.stateLock:
            for p in self.r.peers:
                p_stub = p["stub"]
                try:
                    p_stub.ping(store.Ping())
                    self._valid_peers.append(p)
                except:
                    self._log.error("节点[ {} ]失联".format(p["peer_id"]))

    def _send_req_vote(self):
        def request_vote(_stub, _peer_id):
            res = _stub.request_vote(
                store.ReqVote(term=self.r.currentTerm, candidateId=self.r.identity, lastLogIndex=self.r.lastLogIndex,
                              lastLogTerm=self.r.lastLogTerm))
            if res.voteGranted:
                self._receive_vote_queue.put(
                    VoteTicket(vote_for=self.r.identity, vote_term=res.term, vote_from=_peer_id))
            else:
                if res.term > self.r.currentTerm:
                    self.become_follower(None, res.term)

        for peer in self._valid_peers:
            peer_id = peer["peer_id"]
            stub = peer["stub"]
            t = spawn(target=request_vote, args=(stub, peer_id,))
            self._event_queue.put(t)

    def _send_leader_single(self):
        def send_leader_hb(_stub):
            _stub.append_entries(
                store.ReqAppendEntries(term=self.r.currentTerm, leaderId=self.r.identity, prevLogIndex=0, prevLogTerm=0,
                                       leaderCommit=0))

        while True:
            for peer in self._valid_peers:
                # peer_id = peer["peer_id"]
                stub = peer["stub"]
                spawn(target=send_leader_hb, args=(stub,))
            time.sleep(1)

    def _process_vote(self):
        if not self._receive_vote_queue.empty():
            t = self._receive_vote_queue.get()
            if t.voteTerm == self.r.currentTerm and t.voteFor == self.r.identity:
                self._log.debug("收到来自[ {} ]的选票".format(t.voteFrom))
                self._votes_count += 1

    def _process_votes_res(self):
        _res = False
        while True:
            if self._stop_election_flag:
                _res = self.ELECTION_STOP
                break
            elif self._timeout_flag:
                _res = self.ELECTION_TIMEOUT
                break
            else:
                self._process_vote()
                if self._votes_count > ((len(self._valid_peers) + 1) / 2):
                    _res = self.ELECTION_SUCCESS
                    break
        self._log.debug("当前 [ {} ]轮选举结束".format(self.r.currentTerm))
        return _res

    def _start_one_round_election(self):
        with self.r.stateLock:
            self.r.currentTerm += 1
            self.r.leader = None
            self.r.voteFor = self.r.identity
            self._votes_count += 1
            t = Timer(self._gen_timeout(), self._timeout_stop)
            self._curr_timer = t
        self._check_valid_peers()
        self._log.debug(
            "发起一轮选举: CurrentTerm [ {} ], State [ {} ], ValidPeers [ {} ]".format(self.r.currentTerm, self.r.state,
                                                                                 self._valid_peers))
        self._send_req_vote()
        t.start()
        return self._process_votes_res()

    def become_candidate(self):
        self._log.debug("开始转为 CANDIDATE")
        with self.r.stateLock:
            self.r.state = State.CANDIDATE
            self._votes_count = 0

    def become_leader(self):
        self._log.debug("开始转为 LEADER")
        with self.r.stateLock:
            self.r.state = State.LEADER
            self.r.leader = self.r.identity
            self.r.voteFor = None
            self._votes_count = 0
            self._send_leader_single()

    def become_follower(self, leader_id, term):
        self._log.debug("开始转为 FOLLOWER")
        with self.r.stateLock:
            self.r.leader = leader_id
            self.r.currentTerm = term
            self.r.state = State.FOLLOWER

    def election(self):
        while not self._stop_election_flag:
            if self.r.state == State.FOLLOWER:
                time.sleep(self._gen_timeout())
                if not self._stop_election_flag:
                    self.become_candidate()
                else:
                    return
            election_res = self._start_one_round_election()
            if election_res == self.ELECTION_SUCCESS:
                self.become_leader()
                break

    def stop_election(self):
        with self.r.stateLock:
            if self._stop_election_flag:
                return

            self._stop_election_flag = True

            for q in (self._event_queue,):
                while not q.empty():
                    t = q.get()
                    t.stop()

            # clear queue
            if self._curr_timer:
                self._curr_timer.cancel()
            self._receive_vote_queue = Queue()
            # recover var
            self._curr_timer = None
            self._votes_count = 0
            self._valid_peers = []
