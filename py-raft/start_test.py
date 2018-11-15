from argparse import ArgumentParser

from node import Node

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-p", "--port", required=True, help="the port which grpc run")
    parser.add_argument("-m", "--members", required=True, help="cluster members")
    args = parser.parse_args()
    self_id = "127.0.0.1:{}".format(args.port)
    peers = []
    for i in args.members.split(","):
        if i != self_id:
            peers.append({"peer_id": i})
    print(peers)
    node = Node(self_id, peers)
    try:
        node.run()
        print("Leader: [ {} ], Term: [ {} ]".format(node.leader, node.currentTerm))
    except KeyboardInterrupt:
        exit(0)
