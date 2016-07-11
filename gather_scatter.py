from RabbitMQService import RabbitMQService
import threading
import time

__author__ = "Adam Preble"
__copyright__ = "Copyright 2016, Adam Preble"
__credits__ = ["Adam Preble"]
__license__ = "personal"
__version__ = "1.0.0"
__maintainer__ = "Adam Preble"
__email__ = "adam.preble@gmail.com"
__status__ = "Demonstration"

"""
Demonstrates a basic "gather-scatter" scenario. It is the opposite of "scatter-gather." Rather than farming out work
to multiple agents, we are trying to assemble multiple agents together for a crucial operation, and then dispersing them
to do their individual parts. There are three factions:

workloads: This represents an experiment with a critical section that we want all the other factions to understand and
synchronize around. An example here would be that we're trying to run a benchmark as a workload, and the other factions
include things like some equipment to check CPU temperature. We want to insert information to denote when the load
started and stopped so that CPU temperature monitor service can filter and prepare data.

monitors: Beyond the aforements temperature example, a monitor could be measuring energy consumption, or actively
compensation for temperature. In the latter case, it will hang up the entire experiment until its part of of the
scenario is stable and under control.

gatherer: The master service that handles and disperses messages between the agents. It makes sure the workload has
reported in, and that all the monitors are ready to go when it does.
"""

class Workload(RabbitMQService):
    """
    Represents a workload that we would want to synchronize around. A user of this would start this service, and then
    execution wait_for_go() right before the critical section of their experiment. It will then unblock. After the
    critical section completes, they call send_completed as a courtesy to any outside participants to know that they
    are done.
    """
    def __init__(self):
        RabbitMQService.__init__(self)
        self.received_go = False
        self.go_signal = threading.Condition()

    def when_starting(self):
        self.channel.basic_publish(exchange=self.exchange_name, routing_key='workload', body="workload ready")

    def inbound_message(self, ch, method, properties, body):
        body_txt = body.decode("utf-8")
        if body_txt == "go":
            print("Workload was given go signal!")
            with self.go_signal:
                self.received_go = True
                self.go_signal.notify()

    def wait_for_go(self, timeout_seconds):
        """
        Notifies the gatherer that this workload is ready to go. At this point, it will block the timeout period until
        the gatherer gives it the go-ahead to proceed. This allows other, unknown agents to proceed.
        :param timeout_seconds: The time to wait for a go-ahead from the gatherer.
        :return:
        """
        with self.go_signal:
            if not self.received_go:
                self.go_signal.wait(timeout_seconds)
        if not self.received_go:
            raise Exception("Workload did not receive go signal. It is likely something was aborted")

    def send_completed(self):
        """
        Notify the gatherer that workload has finished. It will then pass along the signal to all other agents so that
        they can stop running.
        """
        print("Workload is issuing stop signal")
        self.channel.async_exec(lambda: self.channel.basic_publish(exchange=self.exchange_name, routing_key='workload', body="workload completed"))
        print("Workload issued stop signal")


class Gatherer(RabbitMQService):
    """
    A secondary broker that manages communications between the workload and monitors. Why the extra complexity beyond
    using RabbitMQ and distributed messaging in the first place? This gives us a layer to track actions, potentially
    enforce that a certain volume of agents are expected, and the like. Without this, the workload has to manage all
    the synchronization. There's a chance the machine on which this is running is being put under stress, so we don't
    want to expect it to manage all of this. Hence, having an intermediary looks pretty good.

    One of these should normally be running. It is mandatory to have a gatherer if there really are any monitors out
    there.
    """
    def __init__(self):
        RabbitMQService.__init__(self)
        self.workload_ready = False
        self.monitors_ready = False
        self.ready_agents = []
        self.monitor_aliases = []

    def inbound_message(self, ch, method, properties, body):
        body_txt = body.decode("utf-8")
        print("Gatherer: received %s" % body_txt)
        if body_txt == "workload ready":
            self.workload_ready = True
            print("propagating ready signal to monitors")
            self.channel.basic_publish(exchange=self.exchange_name, routing_key='gatherer', body="ready")

        elif body_txt == "workload completed":
            self.workload_ready = False
            print("propagating stop signal to monitors")
            self.channel.basic_publish(exchange=self.exchange_name, routing_key='gatherer', body="stop")

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
                    self.channel.basic_publish(exchange=self.exchange_name, routing_key='gatherer', body="go")

        elif body_txt.startswith("identify"):
            agent = body_txt[9:]
            print("Agent %s identified" % agent)

        else:
            print("Gatherer is not using the message")


class WorkloadMonitor(RabbitMQService):
    def __init__(self, name):
        RabbitMQService.__init__(self)
        self.name = name

    def when_starting(self):
        self.channel.basic_publish(exchange=self.exchange_name, routing_key='gatherer', body="identify %s" % self.name)

    def inbound_message(self, ch, method, properties, body):
        body_txt = body.decode("utf-8")
        print("Monitor %s received message: %s" % (self.name, body_txt))
        if body_txt == "ready":
            print("Monitor %s is responding that it's ready" % self.name)
            self.channel.basic_publish(exchange=self.exchange_name, routing_key='gatherer', body="agent ready %s" % self.name)
        elif body_txt == "go":
            print("Monitor %s is proceeding!" % self.name)
        elif body_txt == "stop":
            print("Monitor %s is stopping!" % self.name)
            self.channel.stop_consuming()
        else:
            print("Monitor %s is ignoring message: %s" % (self.name, body_txt))


if __name__ == "__main__":
    gatherer = Gatherer()
    gatherer.start()

    print()
    print("=================================")
    print("Monitors are starting")
    print("=================================")
    print()

    monitor1 = WorkloadMonitor("agent1")
    monitor1.start()

    monitor2 = WorkloadMonitor("agent2")
    monitor2.start()

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

    workload.wait_for_go(3)
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
