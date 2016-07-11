__author__ = "Adam Preble"
__copyright__ = "Copyright 2016, Adam Preble"
__credits__ = ["Adam Preble"]
__license__ = "personal"
__version__ = "1.0.0"
__maintainer__ = "Adam Preble"
__email__ = "adam.preble@gmail.com"
__status__ = "Demonstration"

'''
Extension of pika's BlockingConnection to provide a blocking opportunity for outside threads to insert communication
operations for it to process. In this way, it is made thread-safe in a basic way. It uses the promises pattern to
handled deferred execution of the activities the other threads need run. The connection uses a queue that it will
process outside of its critical section in order to keep communication operations properly-synchronized.
'''

import pika
import threading
import datetime

class Promise(object):
    def __init__(self, callback):
        self.callback = callback
        self.callback_ran = False
        self.callback_condition = threading.Condition()
        self.exception = None

    def wait_until_run(self, timeout=3):
        start_time = datetime.datetime.now()
        local_callback_ran = False
        while (datetime.datetime.now()-start_time).total_seconds() < timeout and not local_callback_ran:
            with self.callback_condition:
                local_callback_ran = self.callback_ran
                if not local_callback_ran:
                    self.callback_condition.wait(timeout=timeout)

        if self.exception is not None:
            raise self.exception


def close_connection_suppressed(connection):
    try:
        connection.close()
    except pika.exceptions.ConnectionClosed as suppressed:
        pass


class DeferredBlockingConnection(pika.BlockingConnection):
    def __init__(self, parameters=None, _impl_class=None):
        pika.BlockingConnection.__init__(self, parameters, _impl_class)

    def channel(self, channel_number=None):
        """Create a new (deferred blocking) channel with the next available
        channel number or pass in a channel number to use. Must be non-zero
        if you would like to specify but it is recommended that you let
        Pika manage the channel numbers.

        :rtype: pika.synchronous_connection.BlockingChannel
        """
        with pika.adapters.blocking_connection._CallbackResult(self._OnChannelOpenedArgs) as opened_args:
            impl_channel = self._impl.channel(
                on_open_callback=opened_args.set_value_once,
                channel_number=channel_number)

            # Create our proxy channel
            channel = DeferredBlockingChannel(impl_channel, self)

            # Link implementation channel with our proxy channel
            impl_channel._set_cookie(channel)

            # Drive I/O until Channel.Open-ok
            channel._flush_output(opened_args.is_ready)


        return channel


class DeferredBlockingChannel(pika.adapters.blocking_connection.BlockingChannel):
    def __init__(self, channel_impl, connection):
        pika.adapters.blocking_connection.BlockingChannel.__init__(self, channel_impl, connection)
        self.callback_queue = []
        self.callback_queue_lock = threading.Lock()

    def async_exec(self, callback, timeout=3):
        promise = Promise(callback)
        with self.callback_queue_lock:
            self.callback_queue.append(promise)
        promise.wait_until_run(timeout)

    def start_consuming(self):
        """Overrides BlockingChannel.start_consuming. At time of override,
        it was documented as such:

        Processes I/O events and dispatches timers and `basic_consume`
        callbacks until all consumers are cancelled.

        NOTE: this blocking function may not be called from the scope of a
        pika callback, because dispatching `basic_consume` callbacks from this
        context would constitute recursion.

        ADDENDUM: This subclassed one checks for external events and makes sure
        they fire in the inner loop when data processing isn't happen. This
        eliminates thread-un-safe data races... so long as the callbacks
        are appropriately scheduled!

        :raises pika.exceptions.RecursionError: if called from the scope of a
            `BlockingConnection` or `BlockingChannel` callback

        """
        # Check if called from the scope of an event dispatch callback
        with self.connection._acquire_event_dispatch() as dispatch_allowed:
            if not dispatch_allowed:
                raise pika.exceptions.RecursionError(
                    'start_consuming may not be called from the scope of '
                    'another BlockingConnection or BlockingChannel callback')

        # Process events as long as consumers exist on this channel
        while self._consumer_infos:
            with self.callback_queue_lock:
                for promise in self.callback_queue:
                    try:
                        promise.callback()
                        with promise.callback_condition:
                            promise.callback_ran = True
                            promise.callback_condition.notify()
                    except Exception as pass_forward:
                        promise.exception = pass_forward

            if self.connection.is_open:
                self.connection.process_data_events(time_limit=0)

