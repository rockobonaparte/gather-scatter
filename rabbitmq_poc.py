import pika
import threading
import time

def run_workload():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    #channel.exchange_declare(exchange='synchronization', type='topic')

    print("Sending a message!")
    channel.basic_publish(exchange='synchronization', routing_key='workload', body="workload ready")
    print("Sent a message!")
    connection.close()


class Gatherer(object):
    def __init__(self):
        self.thread = threading.Thread(target=self._gatherer_agent)
        self.channel = None
        self.workload_ready = False
        self.monitors_ready = False

    def _gatherer_agent(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = connection.channel()

        self.channel.exchange_declare(exchange='synchronization', type='topic')
        result = self.channel.queue_declare(exclusive=True)
        queue_name = result.method.queue

        self.channel.queue_bind(exchange='synchronization', queue=queue_name, routing_key='*')
        self.channel.basic_consume(self.inbound_message, queue=queue_name, no_ack=True)
        print("Gatherer has started consuming")
        self.channel.start_consuming()
        print("Gatherer has stopped consuming")


    def inbound_message(self, ch, method, properties, body):
        print("Gatherer is receiving a message!")
        print(" [x] %r:%r" % (method.routing_key, body))
        if body == b"workload ready":
            self.workload_ready = True
            print("propagating ready signal to monitors")
            self.channel.basic_publish(exchange='synchronization', routing_key='gatherer', body="ready")
        elif body == b"ready":
            self.monitors_ready = True
            if self.workload_ready:
                self.channel.basic_publish(exchange='synchronization', routing_key='gatherer', body="go")
                self.channel.stop_consuming()

    def _monitor_agent(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = connection.channel()

    def start(self):
        self.thread.start()

    def stop(self):
        self.thread.join()


class WorkloadMonitor(object):

    def __init__(self, name):
        self.thread = threading.Thread(target=self._monitor_agent)
        self.channel = None

    def inbound_message(self, ch, method, properties, body):
        print("Monitor is receiving a message!")
        print(" [x] %r:%r" % (method.routing_key, body))
        if body == b"ready":
            self.channel.basic_publish(exchange='synchronization', routing_key='gatherer', body="ready")
        elif body == b"go":
            print("Proceeding!")
            self.channel.stop_consuming()

    def _monitor_agent(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = connection.channel()

        self.channel.exchange_declare(exchange='synchronization', type='topic')
        result = self.channel.queue_declare(exclusive=True)
        queue_name = result.method.queue

        self.channel.queue_bind(exchange='synchronization', queue=queue_name, routing_key='gatherer')
        self.channel.basic_consume(self.inbound_message, queue=queue_name, no_ack=True)
        print("Monitor has started consuming")
        self.channel.start_consuming()
        print("Monitor has stopped consuming")

    def start(self):
        self.thread.start()

    def stop(self):
        self.thread.join()

if __name__ == "__main__":
    gatherer = Gatherer()
    gatherer.start()

    monitor = WorkloadMonitor("agent1")
    monitor.start()

    # TODO: Actually get a signal from the monitor right when its ready to start consuming!
    # TODO: See if we can get the workload and monitor to handle some delay between them...
    # We have a race condition here where a monitor showing up right when the workload is about to
    # signal will get missed. Ideally, this wouldn't really be that big of a deal...
    time.sleep(3)

    run_workload()

    gatherer.stop()
    monitor.stop()
