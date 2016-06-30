import pika
import threading
import time
from DeferredBlockingConnection import DeferredBlockingConnection

class Workload(object):
    def __init__(self):
        self.thread = threading.Thread(target=self._workload_agent)
        self.channel = None
        self.connection = None
        self.received_go = False
        self.go_signal = threading.Condition()

    def inbound_message(self, ch, method, properties, body):
        body_txt = body.decode("utf-8")
        if body_txt == "go":
            print("Workload was given go signal!")
            with self.go_signal:
                self.received_go = True
                self.go_signal.notify()


    def _workload_agent(self):
        self.connection = DeferredBlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()

        result = self.channel.queue_declare(exclusive=True)
        queue_name = result.method.queue

        self.channel.queue_bind(exchange='synchronization', queue=queue_name, routing_key='*')
        self.channel.basic_consume(self.inbound_message, queue=queue_name, no_ack=True)
        self.channel.basic_publish(exchange='synchronization', routing_key='workload', body="workload ready")

        print("Workload has started consuming")
        self.channel.start_consuming()
        print("Workload has stopped consuming")

    def start(self):
        self.thread.start()

    def stop(self):
        #self.async_exec(lambda: self.connection.close())
        print("Workload is closing connection")
        self.channel.async_exec(lambda: self.connection.close())
        print("Workload connection closed")
        self.thread.join()
        print("Workload thread service joined back to main thread")

    # def async_exec(self, callback):
    #     # We are being naughty and calling the private _acquire_event_dispatch. This is taken from start_consuming().
    #     # This should give us a thread-safe break to do whatever other junk we want to do, like get messages sent
    #     # from outside the loop or honor a connection close event from outside the loop.
    #     with self.connection._acquire_event_dispatch() as dispatch_allowed:
    #         if not dispatch_allowed:
    #             raise Exception("Could not acquire connection dispatcher")
    #         else:
    #             callback()

    def wait_for_go(self, timeout_seconds):
        with self.go_signal:
            if not self.received_go:
                self.go_signal.wait(timeout_seconds)
        if not self.received_go:
            raise Exception("Workload did not receive go signal. It is likely something was aborted")

    def send_completed(self):
        print("Workload is issuing stop signal")
        #self.async_exec(lambda: self.channel.basic_publish(exchange='synchronization', routing_key='workload', body="workload completed"))
        self.channel.async_exec(lambda: self.channel.basic_publish(exchange='synchronization', routing_key='workload', body="workload completed"))
        print("Workload issued stop signal")

class Gatherer(object):
    def __init__(self):
        self.thread = threading.Thread(target=self._gatherer_agent)
        self.connection = None
        self.channel = None
        self.workload_ready = False
        self.monitors_ready = False
        self.ready_agents = []

        self.monitor_aliases = []

    def _gatherer_agent(self):
        self.connection = DeferredBlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange='synchronization', type='topic')
        result = self.channel.queue_declare(exclusive=True)
        queue_name = result.method.queue

        self.channel.queue_bind(exchange='synchronization', queue=queue_name, routing_key='*')
        self.channel.basic_consume(self.inbound_message, queue=queue_name, no_ack=True)
        print("Gatherer has started consuming")
        self.channel.start_consuming()
        print("Gatherer has stopped consuming")

    def inbound_message(self, ch, method, properties, body):
        body_txt = body.decode("utf-8")
        print("Gatherer: received %s" % body_txt)
        if body_txt == "workload ready":
            self.workload_ready = True
            print("propagating ready signal to monitors")
            self.channel.basic_publish(exchange='synchronization', routing_key='gatherer', body="ready")

        elif body_txt == "workload completed":
            self.workload_ready = False
            print("propagating stop signal to monitors")
            self.channel.basic_publish(exchange='synchronization', routing_key='gatherer', body="stop")

        elif body_txt.startswith("agent ready"):
            ready_agent = body_txt[12:]
            print("notified that agent %s is ready" % ready_agent)
            print("agents in pool %s" % str(self.ready_agents))

            if ready_agent not in self.ready_agents:
                self.ready_agents.append(ready_agent)
                if len(self.ready_agents) == 2:
                    print("agent %s added to pool, pool now is %s" % (ready_agent, str(self.ready_agents)))
                    self.monitors_ready = True

                # Placing this one level deeper will reduce spurious go signals. It will only send a go on the
                # moment that all agents have reported in. If the agents decided to report they are ready multiple
                # times, that's fine by them, but it won't send another go signal.
                if self.monitors_ready and self.workload_ready:
                    print("propagating go signal to all receivers")
                    self.channel.basic_publish(exchange='synchronization', routing_key='gatherer', body="go")

        elif body_txt.startswith("identify"):
            agent = body_txt[9:]
            print("Agent %s identified" % agent)

        else:
            print("Gatherer is not using the message")

    # def async_exec(self, callback):
    #     # We are being naughty and calling the private _acquire_event_dispatch. This is taken from start_consuming().
    #     # This should give us a thread-safe break to do whatever other junk we want to do, like get messages sent
    #     # from outside the loop or honor a connection close event from outside the loop.
    #     with self.connection._acquire_event_dispatch() as dispatch_allowed:
    #         if not dispatch_allowed:
    #             raise Exception("Could not acquire connection dispatcher")
    #         else:
    #             callback()

    def start(self):
        self.thread.start()

    def stop(self):
        #self.async_exec(lambda: self.connection.close())
        self.channel.async_exec(lambda: self.connection.close())
        self.thread.join()


class WorkloadMonitor(object):

    def __init__(self, name):
        self.thread = threading.Thread(target=self._monitor_agent)
        self.channel = None
        self.name = name

    def inbound_message(self, ch, method, properties, body):
        body_txt = body.decode("utf-8")
        print("Monitor %s received message: %s" % (self.name, body_txt))
        if body_txt == "ready":
            print("Monitor %s is responding that it's ready" % self.name)
            self.channel.basic_publish(exchange='synchronization', routing_key='gatherer', body="agent ready %s" % self.name)
        elif body_txt == "go":
            print("Monitor %s is proceeding!" % self.name)
        elif body_txt == "stop":
            print("Monitor %s is stopping!" % self.name)
            self.channel.stop_consuming()
        else:
            print("Monitor %s is ignoring message: %s" % (self.name, body_txt))

    def _monitor_agent(self):
        connection = DeferredBlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = connection.channel()

        self.channel.exchange_declare(exchange='synchronization', type='topic')
        result = self.channel.queue_declare(exclusive=True)
        queue_name = result.method.queue

        self.channel.queue_bind(exchange='synchronization', queue=queue_name, routing_key='gatherer')
        self.channel.basic_consume(self.inbound_message, queue=queue_name, no_ack=True)
        self.channel.basic_publish(exchange='synchronization', routing_key='gatherer', body="identify %s" % self.name)

        self.channel.start_consuming()
        print("Monitor %s has stopped consuming" % self.name)

    def start(self):
        self.thread.start()

    def stop(self):
        self.thread.join()

if __name__ == "__main__":
    gatherer = Gatherer()
    gatherer.start()

    monitor1 = WorkloadMonitor("agent1")
    monitor1.start()

    monitor2 = WorkloadMonitor("agent2")
    monitor2.start()

    # TODO: Actually get a signal from the monitor right when its ready to start consuming!
    # TODO: See if we can get the workload and monitor to handle some delay between them...
    # We have a race condition here where a monitor showing up right when the workload is about to
    # signal will get missed. Ideally, this wouldn't really be that big of a deal...
    time.sleep(3)

    workload = Workload()
    workload.start()

    print()
    print("=================================")
    print("workload is waiting for go signal")
    print("=================================")
    print()

    workload.wait_for_go(30)
    print("Main program: Workload got go signal and is continuing!")

    time.sleep(1.5)

    print()
    print("=================================")
    print("workload is sending completion   ")
    print("=================================")
    print()
    workload.send_completed()

    print("stopping workload")
    workload.stop()

    print("stopping monitor1")
    monitor1.stop()

    print("stopping monitor2")
    monitor2.stop()

    print("stopping gatherer")
    gatherer.stop()
