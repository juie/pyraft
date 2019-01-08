import grpc
import time
from threading import Thread
from queue import Queue

import raft.protobuf.raft_pb2_grpc as service
import raft.protobuf.raft_pb2 as struct


class Peer(object):
    def __init__(self, server, name, connection_string):
        self.s = server
        self.name = name
        self.connection_string = connection_string

        self.pre_log_index = 0

        self.heartbeat_interval = self.s.heartbeat_interval

        self.log = self.s.log

        self.lock = self.s.lock

        self.stop_chan = Queue(1)

        self.last_active_time = time.time()

    def send_vote_request(self, current_term, last_log_index, last_log_term, resp_queue):
        channel = grpc.insecure_channel(self.connection_string)
        stub = service.ServeRaftStub(channel)
        try:
            resp = stub.request_vote(
                struct.ReqVote(candidateId=self.s.name, term=current_term, lastLogIndex=last_log_index,
                               lastLogTerm=last_log_term))
        except Exception as exc:
            self.log.error("Grpc Error!")
            return
        resp_queue.put(resp)

    def heartbeat(self):
        while True:
            if not self.stop_chan.empty():
                self.stop_chan.get()
                self.stop_chan.task_done()
                return
            self.log.error("heartbeat")
            channel = grpc.insecure_channel(self.connection_string)
            stub = service.ServeRaftStub(channel)
            stub.append_entries(
                struct.ReqAppendEntries(term=self.s.current_term, leaderId=self.s.name, prevLogIndex=0,
                                        prevLogTerm=0, leaderCommit=0))
            self.last_active_time = time.time()
            time.sleep(self.heartbeat_interval)

    def start_heartbeat(self):
        t = Thread(target=self.heartbeat)
        t.setDaemon(True)
        t.start()
        self.s.thread_queue.put(t)

    def stop_heartbeat(self):
        self.stop_chan.put(True)
