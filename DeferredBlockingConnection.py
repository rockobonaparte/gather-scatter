import pika
import threading
import datetime

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


class Promise(object):
    """
    A contract with the DeferredBlockingConnection to execute the given callback command when it is safe to do so.
    This will store the function to call. Provide it as a function taking no arguments--lambdas are good for this.
    If there was an exception running the command, the exception property will be set to something other than None.
    When it is necessary to have the callback run before the thread proceeds, using wait_until_run() to block until
    the callback was completed. It will throw any exception encountered along the way.

    Also, you can check the retval property for any return values.
    """

    def __init__(self, callback):
        """
        Creates a new promise based on the given callback.
        :param callback: A function taking no arguments allowed to return what it wants. Use lambdas to massage your
        callback to fit this pattern.
        :return: (constructor)
        """
        self.callback = callback
        self.callback_ran = False
        self.callback_condition = threading.Condition()
        self.exception = None
        self.retval = None

    def wait_until_run(self, timeout=3):
        """
        Blocks this thread until either the callback has been run or the timeout was exceeded.
        :param timeout: Wait timeout in seconds. It defaults to three seconds.
        :return: (nothing)
        :except: TimeoutError if the timeout was exceeded.
                 Exception for any other exception encountered when running the callback.
        """
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
    """
    Helper for closing a connection while disregarding if the connection is already closed.
    :param connection: The pika connection to close.
    :return: (nothing)
    """
    try:
        connection.close()
    except pika.exceptions.ConnectionClosed as suppressed:
        pass


class DeferredBlockingConnection(pika.BlockingConnection):
    """
    Subclass of pika.BlockingConnection that provides a helper for creating a nonblocking channel instead of a regular
    blocking channel. This is used in conjunction with Promises to all other threads to work with this connection in a
    thread-safe way.
    """

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
    """
    Subclass of pika.BlockingChannel that introduces a blocking queue. Participants from other threads can enqueue
    operations to run on this thread, which will be accomplished using Promise objects. This connection will complete
    these commands when it isn't otherwise running internal connection operations.
    """

    def __init__(self, channel_impl, connection):
        pika.adapters.blocking_connection.BlockingChannel.__init__(self, channel_impl, connection)
        self.callback_queue = []
        self.callback_queue_lock = threading.Lock()

    def async_exec(self, callback, timeout=3):
        """
        Schedules a callback to be run at a thread-safe interval within this channel. From the perspective of the
        caller, this is an asynchronous operation, even if it's perfectly-synchronous internally. Use this to utilize
        this channel from outside of the thread that owns it.
        :param callback: The callback to execute.
        :param timeout: The amount of time in seconds to wait for the command to complete.
        :return: The return value from the callback, if there was one. Otherwise, it returns None.
        """
        promise = Promise(callback)
        with self.callback_queue_lock:
            self.callback_queue.append(promise)
        promise.wait_until_run(timeout)
        return promise.retval

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
                        promise.retval = promise.callback()
                        with promise.callback_condition:
                            promise.callback_ran = True
                            promise.callback_condition.notify()
                    except Exception as pass_forward:
                        promise.exception = pass_forward

            if self.connection.is_open:
                self.connection.process_data_events(time_limit=0)

