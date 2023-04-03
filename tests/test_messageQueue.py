# Test fille for messageQueue.
import json
import time
from messageQueue import create_message, Producer, Queue, Consumer

def test_create_message():
    payload = "Hello, World!"
    message_type = "greeting"
    priority = 1
    message_str = create_message(payload, message_type, priority)
    message = json.loads(message_str)

    assert "id" in message
    assert "timestamp" in message
    assert message["payload"] == payload
    assert message["message_type"] == message_type
    assert message["priority"] == priority

def test_producer_publish_msg():
    queue = Queue()
    producer = Producer(queue)

    payload = "Sample message"
    message_type = "sample"
    priority = 1
    producer.publish_msg(payload, message_type, priority)

    assert len(queue.storage) == 1
    message = json.loads(queue.storage[0])
    assert message["payload"] == payload
    assert message["message_type"] == message_type
    assert message["priority"] == priority

def test_queue_pull_msg():
    queue = Queue()
    message_str = create_message("Test message")
    queue.storage.append(message_str)

    pulled_message_str = queue.pull_msg()
    assert pulled_message_str == message_str
    assert len(queue.storage) == 0

def test_queue_replay_from():
    queue = Queue()
    producer = Producer(queue)

    producer.publish_msg("Message 1")
    replay_start_time = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    time.sleep(2)
    producer.publish_msg("Message 2")

    replayed_messages = queue.replay_from(start_time=replay_start_time)
    assert len(replayed_messages) == 1
    assert replayed_messages[0]["payload"] == "Message 2"

def test_consumer_pull_msg(mocker):
    queue = Queue()
    producer = Producer(queue)
    consumer = Consumer(queue)

    producer.publish_msg("Hello, Consumer!")

    with mocker.patch("builtins.print") as mocked_print:
        consumer.pull_msg()

        assert len(queue.storage) == 0
        mocked_print.assert_called_once_with("Consumed message: {'id': ANY, 'timestamp': ANY, 'payload': 'Hello, Consumer!'}")

if __name__ == "__main__":
    test_create_message()
    test_producer_publish_msg()
    test_queue_pull_msg()
    test_queue_replay_from()
    test_consumer_pull_msg()
