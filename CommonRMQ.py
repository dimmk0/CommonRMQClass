import configparser
import pika
import json
import os


class CommonRMQ(object):
    """Class to work with queues"""

    def __init__(self, config_file='./CommonRMQ.conf', **kwargs):

        self.config = configparser.ConfigParser()
        self.config.read(config_file)

        self.rmq_server = self.config.get('RMQ', 'server')
        self.rmq_user = self.config.get('RMQ', 'rmq_user')
        self.rmq_pass = self.config.get('RMQ', 'rmq_password')
        self.queue = self.config.get('RMQ', 'read_queue')
        self.output_dir = self.config.get('COMMON', 'output_dir')
        self.json_expected = self.config.get('COMMON', 'is_json_expected')
        self.expected_message_count = \
            int(kwargs.get('count_messages_to_read',
                           self.config.get('COMMON',
                                           'count_messages_to_read')))
        self.message_count = 1
        self.create_dump = \
            kwargs.get('dump_messages',
                       self.config.get('COMMON', 'dump_messages'))

        self.credentials = pika.PlainCredentials(self.rmq_user, self.rmq_pass)
        if self.create_dump and not os.path.exists(self.output_dir):
            os.mkdir(self.output_dir)
        self._setup_queue()

    def _setup_queue(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(self.rmq_server,
                                      credentials=self.credentials))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue)

    def start_consume(self):
        self.tag = self.channel.basic_consume(queue=self.queue,
                                              auto_ack=True,
                                              on_message_callback=self.callback)
        print("Starting message consuming.")
        self.channel.start_consuming()

    def callback(self, ch, method, properties, body):
        if self.create_dump:
            self.dump_to_file(body)
        else:
            print(" [x] Received %r" % body)

        if self.message_count >= self.expected_message_count:
            self.channel.close()
            print("Consuming is done")
        else:
            self.message_count += 1

    def dump_to_file(self, msg):
        if self.json_expected:
            msg = self.convert_to_json(msg)
        if msg:
            filename = os.path.join(self.output_dir ,'%05d' % self.message_count)
            with open(filename, mode='w') as dump_file:
                dump_file.write(msg)

        print(" [x] Dumped %r" % msg)

    def convert_to_json(self, msg):
        try:
            msg_json = json.loads(msg)
        except ValueError as e:
            print("Can't convert to json %s", str(e))
            msg_json = None
        return json.dumps(msg_json)

    def send_message(self):
        pass

    def save_message(self):
        pass

    def print_message(self):
        pass


if __name__ == "__main__":
    rmq = CommonRMQ(create_dump=True)
    rmq.start_consume()
