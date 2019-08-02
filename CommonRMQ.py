
import pika


class CommonRMQ(object):
    """Class to work with queues"""
    def __init__(self):
        self.queue = 'test_output_queue'
        self.rmq_server = 'localhost'
        self.expected_message_count = 1
        self.message_count = 0

        self.credentials = pika.PlainCredentials('admin',
                                                 '')
        self._setup_queue()
        print("INit done")

    def _setup_queue(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(self.rmq_server, credentials=self.credentials))

        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue)
        self.tag = self.channel.basic_consume(queue=self.queue,
                                              auto_ack=True,
                                              on_message_callback=self.callback)

    def start_consume(self):
        self.channel.start_consuming()

    def callback(self, ch, method, properties, body):
        print(" [x] Received %r" % body)
        if self.message_count >= self.expected_message_count:
            self.connection.close()
            print("Consume is done")
        else:
            self.message_count += 1

    def send_message(self):
        pass

    def save_message(self):
        pass

    def print_message(self):
        pass


if __name__ == "__main__":

    rmq = CommonRMQ()
    rmq.start_consume()
