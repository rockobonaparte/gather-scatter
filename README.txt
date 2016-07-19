This project is some personal experiments with working with RabbitMQ and distributed messaging. I was curious if I could
do certain things with the technology. It looks like I can! It is a basic demonstration and isn't a model example of
perfect, well-honed software. There are a list of open items to make it more robust.

The basic demonstration scenario is for a lab situation I have experienced before. I have wanted to have some
testbench emit control signals when it's about to enter a critical section. Zero or more monitors may be in place that
would make use of that signal, and they might even want to block the critical section from starting for a certain
amount of time. This requires some coordination, but the testbench itself is not in the right position to make this
decision. Just take my word on that. Some central authority coordinating the experiments should handle this.

This demonstration addresses the situation by creating a "gatherer" and implements something I am calling a
"gather-scatter" model. This is the opposite of "scatter-gather." Rather than trying to accumulate the work together
of multiple agents, it is instead trying to fire off multiple agents in the wild at once. In truth, a separate gather
step for all involved agents would probably be necessary for a complete experiment, but I did not want to
overcomplicate things--it's bad enough already.

This could have been done with sockets, but I wanted to see what it would be like to use RabbitMQ--and distributed
messaging in general--to coordinate this kind of thing. Generally, it is less fuss than sockets if you don't play with
sockets or Twisted too much, so it is in a good position for my kind of end user. I have done a similar thing before
using Twisted, and it takes a lot of code to make this all work together.

Usage:
You can just run gather_scatter.py, which runs a demonstration of all agents in one application, but it is
overwhelming. Rather, you should try to run the different major actors in different shells. Run them in this order:

1. run_gatherer. This starts a gatherer service. Note that you'll have to forcibly terminate this at the end.
2. run_monitor. This starts a monitor.
3. run_workload. This starts a workload.

These can take some arguments. To make this situation more interesting, you can instruct the gatherer that it
definitely needs to synchronize on two particular agents.

1. run_gatherer agent1 agent2
2. run_monitor agent1
3. run_workload
4. [in another shell] run_monitor agent2

Nothing will progress until agent2 is launched because the gatherer is waiting on it, and the other agents are waiting
on the gatherer.



Question: What if I wanted to make this more robust?

1. The different services should have a reset capability to run multiple times.
2. Workloads should unblock if there is no gatherer. This means it handshakes with the gatherer. It would imply a small
timeout of some kind.
3. Switch print statements to the logging module.
4. Add a few more unit tests. It is pretty tedious to mock many of the components used for multi-threading, but the
gatherer's logic is particularly getting verbose and could benefit from some testing.
5. Verify security and encryption, but this can probably best be done with some RabbitMQ built-ins.
6. Have the workload outright disconnect when it gets the go signal, and have it reconnect afterwards. This would
eliminate any concerns of it affecting the test by having a resident thread and socket open. This may be necessary if
the test takes down the system and the socket can't be reopened from lower layers of the software stack.