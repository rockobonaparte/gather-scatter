import pika
import threading
from DeferredBlockingConnection import DeferredBlockingConnection
from DeferredBlockingConnection import close_connection_suppressed

__author__ = "Adam Preble"
__copyright__ = "Copyright 2016, Adam Preble"
__credits__ = ["Adam Preble"]
__license__ = "personal"
__version__ = "1.0.0"
__maintainer__ = "Adam Preble"
__email__ = "adam.preble@gmail.com"
__status__ = "Demonstration"

'''
Consolidated helper for handling the gather-scatter demonstration. Various agents subclass RabbitMQService to get the
basic handshaking under control. They then just implement inbound_message and when_starting as they see fit. They
should use async_exec on the channel to schedule new messages.
'''


class RabbitMQService(object):
    """
    Service that can be subclassed to handle general communications for the scatter-gather demonstration. This takes
    care of the basic protocol handshaking and gives the user various useful callbacks. Of note:
    inbound_message: When a new message comes in.
    when_starting: A callback when the service is starting up.
    when_stopping: A callback when the service is shutting down.

    Important fields:
    self.channel: The pika channel to use. Use the async_exec call to schedule communication on it--notable outbound
    messages that needs to go through RabbitMQ.
    self.exchange_name: The name of the exchange to use.
    """

    def __init__(self, exchange_name="gather_scatter"):
        """
        Set up a RabbitMQService helper. The service is not yet started.
        :param exchange_name: The name of the exchange to use. The default is "gather_scatter."
        :return: (constructor)
        """
        self.thread = threading.Thread(target=self._workload_agent)
        self.channel = None
        self.connection = None
        self.exchange_name = exchange_name

    def _inbound_callback(self, ch, method, properties, body):
        """
        Callback for inbound messages. This is a pass-through for the callback to pika's channel.basic_consume.
        :param ch: The inbound channel.
        :param method: The RabbitMQ delivery method used, as interpreted by pika.
        :param properties: The RabbitMQ message properties, as interpreted by pika.
        :param body: The body of the message.
        :return: (nothing)
        """
        body_txt = body.decode("utf-8")
        self.inbound_message(body_txt)

    def inbound_message(self, message):
        """
        Callback for inbound message bodies, converted to UTF-8 format. This is the main message receiver that
        subclasses implement to receive messages.
        :param message: The message received, as a UTF-8 string.
        :return: (nothing)
        """
        pass

    def when_starting(self):
        """
        A callback when this service is starting. The connection has been opened at this point, so it is safe to send
        messages using self.channel.async_exec().
        :return: (nothing)
        """
        pass

    def when_stopping(self):
        """
        A callback when the connection has been shut down. It is NOT SAFE TO CONCLUDE THE CONNECTION IS STILL OPEN.
        Assume the connection is closed.
        :return: (nothing)
        """
        pass

    def _workload_agent(self):
        """
        Internal agent for opening the connection, setting up the exchanges, queues, and channels. This is run from
        the dedicated thread.
        :return: (nothing)
        """
        self.channel.start_consuming()

        self.when_stopping()

    def start(self):
        """
        Starts the thread that runs this service. This will initiate communications.
        :return:
        """
        self.connection = DeferredBlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange=self.exchange_name, type='topic')

        result = self.channel.queue_declare(exclusive=True)
        queue_name = result.method.queue

        self.channel.queue_bind(exchange=self.exchange_name, queue=queue_name, routing_key='*')
        self.channel.basic_consume(self._inbound_callback, queue=queue_name, no_ack=True)

        self.when_starting()

        self.thread.start()

    def stop(self, timeout_s=30):
        """
        Closes the connection, stops the service, and joins the internal thread. This will block until the service
        thread joins.
        :param timeout_s: Timeout in seconds to wait for the service thread to join. The default is 30 seconds.
        :return:
        """
        self.channel.async_exec(lambda: close_connection_suppressed(self.connection))
        self.thread.join(timeout=timeout_s)
