"""Our key-value store server, which listens to requests and interacts
with other servers."""
import pickle
import socket
import time
from collections import namedtuple
from threading import Thread, Lock

from Queue import Queue

from kvstore import KVStore

GetRequest = namedtuple('GetRequest', 'key')
GetResponse = namedtuple('GetResponse', 'key value timestamp')

InsertRequest = namedtuple('InsertRequest', 'key value')
InsertResponse = namedtuple('InsertResponse', 'key result')

UpdateRequest = namedtuple('UpdateRequest', 'key value')
UpdateResponse = namedtuple('UpdateResponse', 'key result')

DeleteRequest = namedtuple('DeleteRequest', 'key')
DeleteResponse = namedtuple('DeleteResponse', 'key result')

NUM_REPLICAS = 3


class Server(object):

    'A server for the key-value store'

    def __init__(self, addr, addrs):
        self.addr = addr
        self.addrs = sorted(addrs)
        assert self.addr in addrs

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        self.data = KVStore()

    def start(self):
        thread = Thread(target=self.run)
        thread.daemon = True
        thread.start()

    def send_message(self, q, msg, peer):
        'Send `msg` to `peer` and wait for a response'
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(peer)

        self.send(msg, sock)
        q.put(self.receive(sock))

    def run(self):
        '''Listening for incoming connections and process them in a
        seperate thread.'''
        self.sock.bind(self.addr)
        self.sock.listen(5)

        while 1:
            conn, addr = self.sock.accept()
            handler = Thread(target=self.handle_connection, args=(conn, addr))
            handler.daemon = True
            handler.start()

    def replicas(self, key):
        h = hash(key) % len(self.addrs)
        return set(self.addrs[(h + i) % len(self.addrs)] for i in range(NUM_REPLICAS))

    @classmethod
    def send(cls, obj, sock):
        'Send the object `obj` over the socket `sock`'
        sock.send(pickle.dumps(obj))

    @classmethod
    def receive(cls, sock):
        'Receive a pickle-serialized object from the socket `sock`'
        data = ''
        while 1:
            data += sock.recv(1024)
            try:
                return pickle.loads(data)
            except EOFError:
                pass  # read more data

    def handle_connection(self, conn, addr):
        while 1:
            msg = self.receive(conn)
            if isinstance(msg, GetRequest):
                value, timestamp = self.data.get(msg.key)
                self.send(GetResponse(msg.key, value, timestamp), conn)
            elif isinstance(msg, InsertRequest):
                result = self.data.insert(msg.key, msg.value)
                self.send(InsertResponse(msg.key, result), conn)
            elif isinstance(msg, UpdateRequest):
                result = self.data.update(msg.key, msg.value)
                self.send(UpdateResponse(msg.key, result), conn)
            elif isinstance(msg, DeleteRequest):
                result = self.data.delete(msg.key)
                self.send(DeleteResponse(msg.key, result), conn)
            else:
                print "Unknown message type: {}".format(msg)

    def get(self, key, level):
        q = Queue()
        for replica in self.replicas(key):
            t = Thread(target=self.send_message,
                       args=(q, GetRequest(key), replica))
            t.start()
        if level == 'one':
            wait_for = 1
        else:
            wait_for = NUM_REPLICAS
        responses = []
        while len(responses) < wait_for:
            resp = q.get()
            assert isinstance(resp, GetResponse)
            assert resp.key == key
            responses.append(resp)
        return max(responses, key=lambda r: r.timestamp).value

    def insert(self, key, value, level):
        q = Queue()
        for replica in self.replicas(key):
            t = Thread(target=self.send_message,
                       args=(q, InsertRequest(key, value), replica))
            t.start()

        if level == 'one':
            wait_for = 1
        else:
            wait_for = NUM_REPLICAS
        responses = []
        while len(responses) < wait_for:
            resp = q.get()
            assert isinstance(resp, InsertResponse)
            assert resp.key == key
            responses.append(resp)
        return all(r.result for r in responses)

    def update(self, key, value, level):
        q = Queue()
        for replica in self.replicas(key):
            t = Thread(target=self.send_message,
                       args=(q, UpdateRequest(key, value), replica))
            t.start()

        if level == 'one':
            wait_for = 1
        else:
            wait_for = NUM_REPLICAS
        responses = []
        while len(responses) < wait_for:
            resp = q.get()
            assert isinstance(resp, UpdateResponse)
            assert resp.key == key
            responses.append(resp)
        return all(r.result for r in responses)

    def delete(self, key):
        q = Queue()
        for replica in self.replicas(key):
            t = Thread(target=self.send_message,
                       args=(q, DeleteRequest(key), replica))
            t.start()

        responses = []
        while len(responses) < NUM_REPLICAS:
            resp = q.get()
            assert isinstance(resp, UpdateResponse)
            assert resp.key == key
            responses.append(resp)
        return all(r.result for r in responses)

    def items(self):
        for key, (value, timestamp) in self.data.items():
            yield key, value

    def owners(self, key):
        'Return a list of servers responsible for `key`'
        return self.replicas(key)
