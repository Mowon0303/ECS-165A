from collections import defaultdict
import threading

class lock:
    def __init__(self):
        self.locks = defaultdict(ReadLock)
        pass 

    def acquire_reader(self,rid):
        return self.locks[rid].acquire_read()

    def release_reader(self,rid):
        return self.locks[rid].release_read()
    
    def acquire_writer(self,rid):
        return self.locks[rid].acquire_write()
    
    def release_writer(self,rid):
        return self.locks[rid].release_write()


class ReadLock:

    def __init__(self):
        # Avoid race condition on reader and writer counter 
        self._rw_ready = threading.Lock()
        # counts the number of readers who are currently in the read-write lock (initially zero)
        self._readers = 0
        self._writers = False

    def acquire_read(self):
        self._rw_ready.acquire()
        if self._writers:
            self._rw_ready.release()
            return False
        else:
            self._readers += 1
            self._rw_ready.release()
            return True

    def release_read(self):
        self._rw_ready.acquire()
        try:
            self._readers -= 1
        finally:
            self._rw_ready.release()

    def acquire_write(self):
        self._rw_ready.acquire()
        if self._readers != 0 :
            self._rw_ready.release()
            return False
        elif self._writers: 
            self._rw_ready.release()
            return False
        else:   
            self._writers = True
            self._rw_ready.release()
            return True

    def release_write(self):
        self._rw_ready.acquire()
        try:
            self._writers = False 
        finally:
            self._rw_ready.release()
