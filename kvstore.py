import time
from threading import Lock


class KVStore(object):

    'A simple thread-safe timestamped key-value store.'

    def __init__(self):
        self.data = {}
        self.lock = Lock()

    def get(self, key):
        with self.lock:
            if key in self.data:
                return self.data[key]
            else:
                return None, None

    def insert(self, key, value):
        with self.lock:
            if key in self.data:
                return False
            self.data[key] = (value, time.time())
            return True
  
    def update(self, key, value, timestamp=time.time()):
        with self.lock:
            if key not in self.data:
                return False
            self.data[key] = (value, timestamp)
            return True

    def delete(self, key):
        with self.lock:
            if key not in self.data:
                return False
            del self.data[key]
            return True

    def keys(self):
        with self.lock:
            return self.data.keys()

    def items(self):
        with self.lock:
            return self.data.items()
