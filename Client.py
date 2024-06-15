import socket
import threading

class PubSubClient:
    def __init__(self, broker_host='localhost', broker_port=42060):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('localhost', 0))  # Binde an localhost und einen zufälligen Port

    def subscribe(self, topic):
        message = self.create_message('subscribe', topic, '')
        self.sock.sendto(message, (self.broker_host, self.broker_port))
        print(f"Subscribed to topic: {topic}")

    def unsubscribe(self, topic):
        message = self.create_message('unsubscribe', topic, '')
        self.sock.sendto(message, (self.broker_host, self.broker_port))
        print(f"Unsubscribed from topic: {topic}")

    def create_message(self, message_type, topic, message):
        message_type = message_type.ljust(10).encode('utf-8')
        topic_len = len(topic)
        topic_len_str = str(topic_len).zfill(2).encode('utf-8')
        topic = topic.encode('utf-8')
        message = message.encode('utf-8')
        return message_type + topic_len_str + topic + message

    def receive(self):
        while True:
            data, addr = self.sock.recvfrom(65507)
            print(f"Received message: {data.decode('utf-8')}")

if __name__ == "__main__":
    client = PubSubClient()
    threading.Thread(target=client.receive).start()

    while True:
        action = input("Enter action (subscribe/unsubscribe/exit): ").strip().lower()
        if action == "subscribe":
            topic = input("Enter topic to subscribe: ").strip()
            client.subscribe(topic)
        elif action == "unsubscribe":
            topic = input("Enter topic to unsubscribe: ").strip()
            client.unsubscribe(topic)
        elif action == "exit":
            print("Exited")
            break
        else:
            print("Unknown action. Please enter subscribe, unsubscribe, or exit.")
