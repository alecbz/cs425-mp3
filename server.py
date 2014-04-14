import pickle
import socket
import time
from collections import deque, namedtuple
from threading import Thread, Lock, Condition

Get = namedtuple('Get', 'seq key')
Return = namedtuple('Return', 'seq key value timestamp')


class Server:

    def __init__(self, addr, peers=[]):
        self.addr = addr
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        self.peers = peers
        self.addresses = sorted(peers + [addr])
        self.me = self.addresses.index(self.addr)

        self.socks = {}
        self.socks_locks = [Lock() for sock in self.socks]

        self.data = {}
        self.data_lock = Lock()

        self.seq = 0
        self.seq_lock = Lock()

        self.responses = deque()
        self.responses_cond = Condition()

    def start(self):
        t = Thread(target=self.run)
        t.daemon = True
        t.start()
        self.connect_to_peers()

    def connect_to_peers(self):
        for addr in self.peers:
            self.socks[addr] = socket.socket(
                socket.AF_INET, socket.SOCK_STREAM)
            self.socks[addr].connect(addr)

    def send_to(self, obj, peer):
        'Send the python object `obj` to the specified peer'
        with self.socks_locks[peer]:
            self.socks[peer].send(pickle.dumps(obj))

    def run(self):
        self.sock.bind(self.addr)
        self.sock.listen(5)

        while 1:
            conn, addr = self.sock.accept()
            t = Thread(target=self.handle_connection, args=(conn, addr))
            t.daemon = True
            t.start()

    def hash_key(self, key):
        return hash(key) % len(self.addresses)

    def receive_object(self, conn):
        'Receive a pickle-serialized object from the socket `conn`'
        data = ''
        while 1:
            data += conn.recv(1024)
            try:
                return pickle.loads(data)
            except EOFError:
                pass  # read more data

    def handle_connection(self, conn, addr):
        print "new connection from {}".format(addr)
        while 1:
            msg = self.receive_object(conn)
            if isinstance(msg, Get):
                with self.seq_lock, self.data_lock:
                    if key not in self.data:
                        msg = Return(self.seq, key, None, None)
                    else:
                        value, timestamp = self.data[key]
                        msg = Return(self.seq, key, value, timestamp)
                    self.seq += 1
                self.send_to(msg, addr)
            elif isinstance(msg, Return):
                with self.responses_cond:
                    self.responses.append(msg)
                    self.responses_cond.notify_all()

    def get_response(self, seq):
        with self.responses_cond:
            while 1:
                if self.responses and self.responses[0].seq == seq:
                    return self.responses.popleft()
                else:
                    self.responses_cond.wait()

    def get(self, key, level):
        iden = self.hash_key(key)
        if iden == self.me:
            # I'm responsible for this key
            with self.data_lock:
                if key in self.data:
                    return None
                else:
                    value, timestamp = self.data[key]
                    return value
        else:
            with self.seq_lock:
                msg = Get(self.seq, key)
                self.seq += 1

            peer = self.addresses[iden]
            self.send_to(msg, peer)

            resp = self.get_response(msg.seq)
            assert isinstance(resp, Return)
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
