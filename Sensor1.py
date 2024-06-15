import socket

class PubSubClient:
    def __init__(self, broker_host='localhost', broker_port=42060):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('localhost', 0))  # Binde an localhost und einen zufälligen Port

    def publish(self, topic, message):
        message = self.create_message('publish', topic, message)
        self.sock.sendto(message, (self.broker_host, self.broker_port))
        print(f"Published message to topic {topic}: {message}")

    def create_message(self, message_type, topic, message):
        message_type = message_type.ljust(10).encode('utf-8')
        topic_len = len(topic)
        topic_len_str = str(topic_len).zfill(2).encode('utf-8')
        topic = topic.encode('utf-8')
        message = message.encode('utf-8')
        return message_type + topic_len_str + topic + message

if __name__ == "__main__":
    client = PubSubClient()

    while True:
        action = input("Enter action (publish/exit): ").strip().lower()
        if action == "publish":
            topic = input("Enter topic to publish: ").strip()
            message = input("Enter message to publish: ").strip()
            client.publish(topic, message)
        elif action == "exit":
            print("Exited")
            break
        else:
            print("Unknown action. Please enter publish or exit.")
