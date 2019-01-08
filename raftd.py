import grpc
import os
import time
import logging
import requests
import json
from argparse import ArgumentParser
from concurrent import futures
from threading import Thread

from raft.server import Server
from raft.command import SelfJoinCommand, DefaultJoinCommand
from httpserver import SimpleHttpServer, request, jsonify


class Node(object):
    def __init__(self, host, port, name, path, election_timeout, heartbeat_interval, debug, init_members=None):
        self.host = host
        self.port = port
        self.name = name
        self.path = path
        self.election_timeout = election_timeout
        self.heartbeat_interval = heartbeat_interval
        self.init_members = init_members or {}

        self.log = logging.getLogger(__file__)
        if debug:
            logging.basicConfig(level=logging.DEBUG)

        self.raft_server = self._new_raft_server()

    def _new_raft_server(self):
        transporter = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        return Server(self.name, self.path, transporter, self.election_timeout, self.heartbeat_interval,
                      "{}:{}".format(self.host, self.port), peers=self.init_members, log=self.log)

    def join_self(self):
        self.raft_server.execute(SelfJoinCommand())

    def join_cluster(self, address):
        resp = json.loads(
            requests.get(
                "http://{}/join?name={}&address={}:{}".format(address, self.name, self.host, self.port)).content)
        if resp["success"]:
            resp = json.loads(
                requests.get("http://{}/join?name={}&address={}:{}".format(resp["leaderAddress"], self.name, self.host,
                                                                           self.port)).content)

        return resp["success"]

    def handle_join(self):
        _address = request.args.get("address")
        _name = request.args.get("name")
        resp = self.raft_server.execute(DefaultJoinCommand(_name, _address))
        if resp:
            return jsonify(success=1, leaderAddress=resp)
        else:
            return jsonify(success=0)

    def start_http_server(self, *args, **kwargs):
        s = SimpleHttpServer()
        s.add_route("/join", "handle_join", self.handle_join)
        t = Thread(target=s.listen_and_serve, args=args, kwargs=kwargs)
        t.setDaemon(True)
        t.start()

    def start(self):
        self.raft_server.start()
        self.start_http_server(port=self.port * 10)

    def stop(self):
        self.raft_server.stop()


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("--host", help="The GRPC address", default="localhost")
    parser.add_argument("-p", "--port", help="The port which GRPC run", default=8013, type=int)
    parser.add_argument("--data", help="Data dir", default=os.path.abspath(__file__))
    parser.add_argument("-n", "--name", help="Unique identifier", type=str)
    parser.add_argument("--cluster", help="Cluster members")
    parser.add_argument("-d", "--debug", help="Raft debugging", default=False, action="store_true")
    parser.add_argument("-j", "--join", help="Join cluster", type=str)
    args = parser.parse_args()
    self_name = args.name or "{}:{}".format(args.host, args.port)
    members = {}
    if args.cluster:
        for m in args.cluster.split(","):
            n, c = m.split("=")
            members[n] = c
    n = Node(args.host, args.port, self_name, args.data, 3, 3, args.debug, members)
    n.start()
    if args.join:
        if args.join == "{}:{}".format(args.host, args.port):
            n.join_self()
        else:
            n.log.debug(n.join_cluster(args.join))
    try:
        while True:
            time.sleep(300)
    except KeyboardInterrupt:
        n.stop()
