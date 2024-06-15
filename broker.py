import socket
import threading
import logging

class MessageBroker:
    def __init__(self, host='localhost', port=42060):
        self.host = host
        self.port = port
        self.subscriptions = {}
        self.lock = threading.Lock()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.host, self.port))
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

    def start(self):
        logging.info(f"Message Broker started on {self.host}:{self.port}")
        threading.Thread(target=self.listen).start()

    def listen(self):
        while True:
            data, addr = self.sock.recvfrom(65507)
            self.handle_message(data, addr)

    def handle_message(self, data, addr):
        message_type, topic, message = self.parse_message(data)
        
        if message_type == 'subscribe':
            self.subscribe(addr, topic)
        elif message_type == 'unsubscribe':
            self.unsubscribe(addr, topic)
        elif message_type == 'publish':
            self.publish(topic, message)
        else:
            logging.warning(f"Unknown message type: {message_type}")

    def parse_message(self, data):
        message_type = data[:10].strip().decode('utf-8')
        topic_len = int(data[10:12].decode('utf-8'))
        topic = data[12:12+topic_len].decode('utf-8')
        message = data[12+topic_len:].decode('utf-8')
        return message_type, topic, message

    def subscribe(self, addr, topic):
        with self.lock:
            if topic not in self.subscriptions:
                self.subscriptions[topic] = set()
            self.subscriptions[topic].add(addr)
            logging.info(f"{addr} subscribed to {topic}")

    def unsubscribe(self, addr, topic):
        with self.lock:
            if topic in self.subscriptions and addr in self.subscriptions[topic]:
                self.subscriptions[topic].remove(addr)
                if not self.subscriptions[topic]:
                    del self.subscriptions[topic]
            logging.info(f"{addr} unsubscribed from {topic}")
            logging.debug(f"Current subscriptions: {self.subscriptions}")

    def publish(self, topic, message):
        with self.lock:
            if topic in self.subscriptions:
                for subscriber in self.subscriptions[topic]:
                    self.sock.sendto(message.encode('utf-8'), subscriber)
                logging.info(f"Message published to {topic}: {message}")
            else:
                logging.info(f"No subscribers to publish to for topic {topic}")

if __name__ == "__main__":
    broker = MessageBroker()
    broker.start()
