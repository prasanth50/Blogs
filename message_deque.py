import threading
from collections import deque

class MessageQueue:
    def __init__(self):
        self._queue = deque()
        self._lock = threading.Lock()

    def enqueue(self, message):
        with self._lock:
            self._queue.append(message)

    def dequeue(self):
        with self._lock:
            if self.is_empty():
                return None
            return self._queue.popleft()

    def is_empty(self):
        return len(self._queue) == 0

    def size(self):
        return len(self._queue)

if __name__ == "__main__":
    mq = MessageQueue()

    # Enqueue messages
    mq.enqueue("Hello Prashanth")
    mq.enqueue("Wecome to blogging!!!")
    mq.enqueue("This is a blocg post on building a Simple Message Queue in Python")

    # Dequeue and print messages
    while not mq.is_empty():
        print(mq.dequeue())
