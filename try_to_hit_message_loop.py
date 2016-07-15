from RabbitMQService import RabbitMQService

class TryToHitReceiveLoop(RabbitMQService):
    def __init__(self, i_want_hello_back=False):
        RabbitMQService.__init__(self)
        self.received_hello = False
        self.wants_hello_back = i_want_hello_back

    def when_starting(self):
        self.channel.basic_publish(exchange=self.exchange_name, routing_key='workload', body="hello")

    def inbound_message(self, ch, method, properties, body):
        body_txt = body.decode("utf-8")
        print("Received: %s" % body_txt)
        if body_txt == "hello" and not self.received_hello:
            self.received_hello = True
            self.channel.basic_publish(exchange=self.exchange_name, routing_key='workload', body="hello back")
        if body_txt == "hello back" and not self.wants_hello_back:
            print("Got hello back")

if __name__ == "__main__":
    tester1 = TryToHitReceiveLoop(True)
    tester1.start()

    tester2 = TryToHitReceiveLoop()
    tester2.start()
