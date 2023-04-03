# Blogs

## **Introduction**

Message queues are a fundamental component in distributed systems, enabling asynchronous communication between different components. They can help decouple producers and consumers, allowing them to operate independently and scale as needed. In this blog post, we will design and implement a simple message queue in Python, taking you through each step of the process.

**Design image for Simple Message Queue**

![Design image for Simple Message Queue](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/2uxmelw40bjtuibiwic4.jpg)

**Detailed steps for design image analysis:**
- **Step 1: Define message format for transmission**

The message format for transmission should be simple, lightweight, and easily parsable. JSON is a popular choice because it is human-readable and widely supported in many programming languages.
json

`{
  "id": "unique_message_id",
  "timestamp": "2023-04-03T10:30:00Z",
  "payload": "your_message_content_here"
}
`
- **Step 2: Define message format for persistence**

The message format for persistence can be the same as the transmission format, or you can add additional fields to store metadata related to persistence, like storage location, status, etc.

`{
  "id": "unique_message_id",
  "timestamp": "2023-04-03T10:30:00Z",
  "payload": "your_message_content_here",
  "status": "unprocessed"
}
`

- **Step 3: Define message format for consumption**

The message format for consumption can also be the same as the transmission format. If needed, you can include additional fields to facilitate consumption, like message type, priority, etc.

`{
  "id": "unique_message_id",
  "timestamp": "2023-04-03T10:30:00Z",
  "payload": "your_message_content_here",
  "message_type": "example_type",
  "priority": 1
}
`

- **Step 4: Define the function of each component**

`Producer:`

publishMsg(): Publishes a message to the queue, including message persistence for durability and eventual consumption by consumers.

`Queue:`

onMsgReceived(): Handles incoming messages from producers, including storing them for future consumption.
replayFrom(): Replays messages from a specific point in time or from a specific message ID to support consumer recovery or replay scenarios.

`Consumer:`

pullMsg(): Pulls messages from the queue for consumption, including support for acknowledging messages to remove them from the queue and avoid re-consumption.

- **Step 5: Sequence diagram for the pub-sub process**

`[Producer] -> publishMsg() -> [Queue]
  [Queue] -> onMsgReceived() -> [Storage]
  [Consumer] -> pullMsg() -> [Queue]
  [Queue] -> retrieve message from [Storage] -> [Consumer]
  [Consumer] -> acknowledge message -> [Queue]
`

To ensure messages are not lost, the Queue should store messages in a persistent storage system (e.g., a database or file storage) before they are consumed by the Consumer. This persistence should happen within the onMsgReceived() function.



**Table of Contents for implementation**

- Designing the Message Queue
- Implementing the Message Queue
- Using the Message Queue
- Extending the Message Queue
- Conclusion

**1. Designing the Message Queue**

Before diving into the implementation, let's outline the design of our message queue. Our message queue will have the following components:

`Producer`: Publishes messages to the queue
`Queue`: Stores and manages messages
`Consumer`: Pulls messages from the queue for processing

We will define three message formats for transmission, persistence, and consumption. For simplicity, we will use JSON as the message format.

**2. Implementing the Message Queue**

To implement the message queue, we will use Python's built-in `deque` from the `collections` module for message storage, and threading.Lock() for thread safety. 

**3. Using the Message Queue**

To use the message queue, instantiate a Queue object, and then create Producer and Consumer instances, passing the Queue object as an argument. Producers can publish messages with the publish_msg() method, and consumers can pull messages with the pull_msg() method.

**4. Extending the Message Queue**

The current implementation is simple and suitable for basic use cases. However, there are several ways to extend and improve the message queue for more advanced scenarios:

`Implement persistent storage`: To ensure message durability, replace the in-memory deque with a persistent storage solution like a database or a filesystem.
`Add support for message priorities`: Modify the Queue class to handle messages with different priorities and deliver them accordingly.
`Implement message acknowledgment`: Allow consumers to acknowledge messages after processing, ensuring that they are removed from the queue and not re-consumed.
`Add support for multiple consumers`: Modify the Queue class to support multiple consumers, allowing for better scalability and load balancing.

** Implementation in Python **

`import json
import time
import uuid
from collections import deque
import threading

## Step 1, 2, 3: Message format

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


## Step 4: Component functions

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
    producer.publish_msg("Message 1", "type_a", 1)
    producer.publish_msg("Message 2", "type_b", 2)
    producer.publish_msg("Message 3", "type_a", 1)

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
`

**Conclusion**

In this blog post, we designed and implemented a simple message queue in Python, walking through each step of the process. While the implementation provided here is basic, it serves as a foundation for understanding message queue concepts and can be extended to suit more advanced use cases.

For production environments, consider using established message queue services like RabbitMQ, Apache Kafka, or Amazon SQS, which offer more robust features and scalability.
