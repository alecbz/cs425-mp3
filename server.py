import socket
from threading import Thread, Lock

class Server:
    def __init__(self, addr, peers=[]):
        self.addr = addr
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        self.peers = peers
        self.addresses = sorted(peers + [addr])
        self.socks = {}

        self.data = {}
        self.lock = Lock()

    def start(self):
        t = Thread(target=self.run)
        t.daemon = True
        t.start()
        self.connect_to_peers()

    def connect_to_peers(self):
        for addr in self.peers:
            self.socks[addr] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socks[addr].connect(addr)

    def run(self):
        self.sock.bind(self.addr) 
        self.sock.listen(5)

        while 1:
            conn, addr = self.sock.accept()
            t = Thread(target=self.handle_connection, args=(conn, addr))
            t.daemon = True
            t.start()


    def handle_connection(self, conn, addr):
        print "new connection from {}".format(addr)
        while 1:
            data = conn.recv(1024)
            print "got '{}'".format(data)

    def get(self, key, level):
        with self.lock:
            return self.data.get(key, None)

    def insert(self, key, value, level):
        with self.lock:
            if key in self.data:
                return False
            self.data[key] = value
            return True

    def update(self, key, value, level):
        with self.lock:
            if key not in self.data:
                return False
            self.data[key] = value
            return True

    def delete(self, key):
        with self.lock:
            if key not in self.data:
                return False
            del self.data[key]
            return True
