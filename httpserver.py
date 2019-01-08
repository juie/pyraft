from flask import Flask, request, jsonify


class SimpleHttpServer(object):
    def __init__(self):
        self.http_server = Flask(__name__)

    def add_route(self, *args, **kwargs):
        self.http_server.add_url_rule(*args, **kwargs)

    def listen_and_serve(self, host="localhost", port=8080, **kwargs):
        self.http_server.run(host, port, **kwargs)
