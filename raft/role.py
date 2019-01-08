import random
from threading import Timer
from queue import Queue
from threading import Thread

from raft.state import RaftIdentityState
from raft.command import NOPCommand, StopLeader


def set_timeout(q):
    q.put(True)


def gen_timeout(t):
    t_min, t_max = t / 10, t
    t_out = t_min + (t_max - t_min) * random.random()
    return t_out


class Leader(object):
    def __init__(self, server):
        self.s = server

        self._stop = True

    def _loop(self):
        self.s.execute(NOPCommand())
        while self.s.state == RaftIdentityState.LEADER:
            if self._stop:
                # stop all peers
                return
            else:
                if not self.s.requests_queue.empty():
                    req = self.s.requests_queue.get()

    def start_loop(self):
        self.s.log.debug("Leader loop start!")
        with self.s.lock:
            if not self._stop:
                return
            self._stop = False
        self._loop()

    def stop_loop(self):
        with self.s.lock:
            if self._stop:
                return

            self._stop = True
            self.s.execute(StopLeader())


class Follower(object):
    def __init__(self, server):
        self.s = server

        self._stop = True
        self.timer = None

    def _loop(self):
        timeout = Queue(1)
        retry = False
        self.timer = Timer(gen_timeout(self.s.election_timeout), set_timeout, args=(timeout,))
        self.timer.start()
        while self.s.state == RaftIdentityState.FOLLOWER:
            if self._stop:
                if self.timer:
                    self.timer.cancel()
                return
            else:
                if not self.s.requests_queue.empty():
                    req = self.s.requests_queue.get()
                if not timeout.empty():
                    timeout.get()
                    timeout.task_done()
                    if self.s.promotable():
                        self.s.state = RaftIdentityState.CANDIDATE
                    else:
                        retry = True
            if retry:
                # self.s.log.debug("Election timeout!")
                self.timer = Timer(gen_timeout(self.s.election_timeout), set_timeout, args=(timeout,))
                self.timer.start()
                retry = False

    def start_loop(self):
        self.s.log.debug("Follower loop start!")
        with self.s.lock:
            if not self._stop:
                return
            self._stop = False
        self._loop()

    def stop_loop(self):
        with self.s.lock:
            if self._stop:
                return

            self._stop = True


class Candidate(object):
    def __init__(self, server):
        self.s = server

        self._stop = True
        self.timer = None

    def _loop(self):
        prev_leader = self.s.leader
        self.s.leader = None
        if prev_leader != self.s.leader:
            # change event
            pass
        last_log_index, last_log_term = self.s.get_last_info()
        do_vote = True
        timeout = Queue(1)
        resp_queue = Queue()
        votes_granted = 0

        while self.s.state == RaftIdentityState.CANDIDATE:
            if self._stop:
                if self.timer:
                    self.timer.cancel()
                return
            else:
                if do_vote:
                    self.s.current_term += 1
                    self.s.vote_for = self.s.name

                    # Send RequestVote RPCs to all other peers.
                    for peer in self.s.peers.values():
                        t = Thread(target=peer.send_vote_request,
                                   args=(self.s.current_term, last_log_index, last_log_term, resp_queue,))
                        t.setDaemon(True)
                        t.start()
                        self.s.thread_queue.put(t)
                    votes_granted = 1
                    self.timer = Timer(gen_timeout(self.s.election_timeout), set_timeout, args=(timeout,))
                    self.timer.start()
                    do_vote = False

                if votes_granted >= self.s.get_quorum_size():
                    self.s.log.debug("Do be leader at {} term".format(self.s.current_term))
                    with self.s.lock:
                        self.s.leader = self.s.name
                        self.s.state = RaftIdentityState.LEADER

                    return
                if not resp_queue.empty():
                    success = self.s.process_vote_resp(resp_queue.get())
                    if success:
                        votes_granted += 1
                    elif success is None:
                        return
                    else:
                        continue
                if not timeout.empty():
                    timeout.get()
                    timeout.task_done()
                    do_vote = True

    def start_loop(self):
        self.s.log.debug("Candidate loop start!")
        with self.s.lock:
            if not self._stop:
                return
            self._stop = False
        self._loop()

    def stop_loop(self):
        with self.s.lock:
            if self._stop:
                return

            self._stop = True
