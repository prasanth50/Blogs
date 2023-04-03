import json
import time
import uuid
from collections import deque
import threading

# Step 1, 2, 3: Message format
def create_message(payload, message_type=None, priority=None):
    message = {
        "id": str(uuid.uuid4()),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "payload": payload,
    }

    if message_type:
        message["message_type"] = message_type

    if priority:
        message["priority"] = priority

    return json.dumps(message)


# Step 4: Component functions
class Producer:
    def __init__(self, queue):
        self.queue = queue

    def publish_msg(self, payload, message_type=None, priority=None):
        message = create_message(payload, message_type, priority)
        self.queue.on_msg_received(message)


class Queue:
    def __init__(self):
        self.storage = deque()
        self.lock = threading.Lock()

    def on_msg_received(self, message):
        with self.lock:
            self.storage.append(message)

    def pull_msg(self):
        with self.lock:
            if not self.storage:
                return None
            return self.storage.popleft()

    def replay_from(self, start_time=None, start_id=None):
        if not start_time and not start_id:
            return []

        with self.lock:
            messages = []
            for message_str in self.storage:
                message = json.loads(message_str)
                if start_time and message["timestamp"] >= start_time:
                    messages.append(message)
                elif start_id and message["id"] == start_id:
                    messages.append(message)
                    break

        return messages


class Consumer:
    def __init__(self, queue):
        self.queue = queue

    def pull_msg(self):
        message_str = self.queue.pull_msg()
        if message_str:
            message = json.loads(message_str)
            # Process the message
            print(f"Consumed message: {message}")
            # Acknowledge the message (in this case, it's automatically removed from the queue)
        else:
            print("No message to consume")


if __name__ == "__main__":
    # Instantiate a Queue
    message_queue = Queue()

    # Instantiate a Producer and publish messages
    producer = Producer(message_queue)
    producer.publish_msg("Hello Prashanth", "type_a", 1)
    producer.publish_msg("Wecome to blogging!!!", "type_b", 2)
    producer.publish_msg("This is a blocg post on building a Simple Message Queue in Python", "type_a", 1)

    # Instantiate a Consumer and consume messages
    consumer = Consumer(message_queue)
    consumer.pull_msg()
    consumer.pull_msg()

    # Replay messages from a specific timestamp
    replay_start_time = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    time.sleep(2)
    producer.publish_msg("Message 4", "type_c", 3)
    replayed_messages = message_queue.replay_from(start_time=replay_start_time)
    print(f"Replayed messages: {replayed_messages}")

    # Consume remaining messages
    consumer.pull_msg()
    consumer.pull_msg()
