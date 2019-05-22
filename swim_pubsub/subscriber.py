"""
Copyright 2019 EUROCONTROL
==========================================

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the 
following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following 
   disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following 
   disclaimer in the documentation and/or other materials provided with the distribution.
3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products 
   derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, 
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE 
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, 
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR 
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, 
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE 
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

==========================================

Editorial note: this license is an instance of the BSD license template as provided by the Open Source Initiative: 
http://opensource.org/licenses/BSD-3-Clause

Details on EUROCONTROL: http://www.eurocontrol.int
"""
import threading

from proton import SSLDomain
from proton._handlers import MessagingHandler
from proton._reactor import Container

from swim_pubsub.middleware import SubscriberMiddleware

__author__ = "EUROCONTROL (SWIM)"


class SubscriberHandler(MessagingHandler):

    def __init__(self, subscriber_middleware):
        MessagingHandler.__init__(self)

        self._sm = subscriber_middleware
        self.receivers = {}
        self._started = False

    def _get_ssl_domain(self):
        ssl_domain = SSLDomain(SSLDomain.VERIFY_PEER)
        # Set the CA certificate
        ssl_domain.set_trusted_ca_db("/media/alex/Data/dev/work/eurocontrol/RabbitMQ-docker/certs/ca_certificate.pem")
        # Set the producer certificate and key to authenticate the producer
        ssl_domain.set_credentials("/media/alex/Data/dev/work/eurocontrol/RabbitMQ-docker/certs/client_certificate.pem",
                                   "/media/alex/Data/dev/work/eurocontrol/RabbitMQ-docker/certs/client_key.pem",
                                   'swim-ti')
        return ssl_domain

    def add_subscription(self, queue, data_handler):
        # receiver = self.container.create_receiver(self.conn, queue)
        receiver = self.container.create_receiver(self.conn, queue)
        print(f"created receiver {receiver}")
        print(f'start receiving on: {queue}')

        self.receivers[receiver] = (queue, data_handler)

    def remove_subscription(self, queue):
        for receiver, (receiver_queue, _) in self.receivers.items():
            if queue == receiver_queue:
                receiver.close()
                print(f"closed receiver {receiver} on queue {queue}")
                del self.receivers[receiver]
                break

        # receiver, _ = self.receivers.pop(address)
        # receiver.close()

    def on_start(self, event):
        self.container = event.container
        self.conn = self.container.connect('amqps://localhost:5671', ssl_domain=self._get_ssl_domain())
        self._started = True

    def on_message(self, event):
        _, data_handler = self.receivers[event.receiver]
        data_handler(event.message.body)

        # for _, (receiver, data_handler) in self.receivers.items():
        #     if event.receiver == receiver:
        #         data_handler(event.message.body)

    def subscribe(self, topic, data_handler):
        queue = self._sm.subscribe(topic)
        self.add_subscription(queue, data_handler)

    def unsubscribe(self, topic):
        queue = self._sm.get_queue_from_topic(topic)
        self.remove_subscription(queue)
        self._sm.unsubscribe(topic)

    # def pause(self, topic):
    #     self._sm.pause(topic)
    #
    # def resume(self, topic):
    #     self._sm.resume(topic)

    def has_started(self):
        return self._started


class Subscriber():

    def __init__(self, subscriber_middleware):
        if not isinstance(subscriber_middleware, SubscriberMiddleware):
            raise ValueError('publisher_middleware should be instance of SubscriberMiddleware')

        self._sm = subscriber_middleware
        self._sub_handler = SubscriberHandler(self._sm)

        self._thread = threading.Thread(target=self.run)
        self._thread.daemon = True
        self._thread.start()

    def run(self):
        self._reactor = Container(self._sub_handler).run()

    # def get_topics(self):
    #     return self._sm.get_topics()

    def subscribe(self, topic, data_handler):
        self._sub_handler.subscribe(topic, data_handler)

    def unsubscribe(self, topic):
        self._sub_handler.unsubscribe(topic)

    # def pause(self, topic):
    #     self._sub_handler.pause(topic)
    #
    # def resume(self, topic):
    #     self._sub_handler.resume(topic)

    def is_running(self):
        return self._thread.is_alive() and self._sub_handler.has_started()

class SubscriberError(Exception):
    pass
