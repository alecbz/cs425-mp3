"""Our key-value store server, which listens to requests and interacts
with other servers."""
import pickle
import socket
import time
from collections import deque, namedtuple
from threading import Thread, Lock, Condition

GetRequest = namedtuple('GetRequest', 'key')
GetResponse = namedtuple('GetResponse', 'key value timestamp')


class Server(object):

    def __init__(self, addr, addrs):
        self.addr = addr
        self.addrs = sorted(addrs)
        assert self.addr in addrs

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        self.data = {}
        self.data_lock = Lock()

    def start(self):
        t = Thread(target=self.run)
        t.daemon = True
        t.start()

    def send_message(self, msg, peer):
        'Send `msg` to `peer` and wait for a response'
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(peer)

        self.send(msg, sock)
        return self.receive(sock)

    def run(self):
        self.sock.bind(self.addr)
        self.sock.listen(5)

        while 1:
            conn, addr = self.sock.accept()
            t = Thread(target=self.handle_connection, args=(conn, addr))
            t.daemon = True
            t.start()

    def hash_key(self, key):
        return hash(key) % len(self.addrs)

    def send(self, obj, sock):
        'Send the object `obj` over the socket `sock`'
        sock.send(pickle.dumps(obj))

    
    def receive(self, sock):
        'Receive a pickle-serialized object from the socket `sock`'
        data = ''
        while 1:
            data += sock.recv(1024)
            try:
                return pickle.loads(data)
            except EOFError:
                pass  # read more data

    def handle_connection(self, conn, addr):
        print "new connection from {}".format(addr)
        while 1:
            msg = self.receive(conn)
            if isinstance(msg, GetRequest):
                key = msg.key
                with self.data_lock:
                    if key not in self.data:
                        msg = GetResponse(key, value=None, timestamp=None)
                    else:
                        value, timestamp = self.data[key]
                        msg = GetResponse(key, value, timestamp)
                self.send(msg, conn)

    def get(self, key, level):
        iden = self.hash_key(key)
        handler = self.addrs[iden]
        if handler == self.addr:
            # I'm responsible for this key
            print "I'm responsible for this key"
            with self.data_lock:
                try:
                    value, timestamp = self.data[key]
                    return value
                except KeyError:
                    return None
        else:
            print "I'm NOT responsible for this key"
            resp = self.send_message(GetRequest(key), handler)
            assert isinstance(resp, GetResponse)
            assert resp.key == key
            return resp.value

    def insert(self, key, value, level):
        with self.data_lock:
            if key in self.data:
                return False
            self.data[key] = (value, time.time())
            return True

    def update(self, key, value, level):
        with self.data_lock:
            if key not in self.data:
                return False
            self.data[key] = (value, time.time())
            return True

    def delete(self, key):
        with self.data_lock:
            if key not in self.data:
                return False
            del self.data[key]
            return True
