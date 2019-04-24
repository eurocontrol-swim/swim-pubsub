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
from functools import wraps

from proton._handlers import MessagingHandler
from proton._reactor import Container, ApplicationEvent

from swim_pubsub.middleware import SubscriberMiddleware

__author__ = "EUROCONTROL (SWIM)"


class SubscriberHandler(MessagingHandler):

    def __init__(self, subscriber_middleware):
        MessagingHandler.__init__(self)

        self._sm = subscriber_middleware
        self.receivers = {}
        self._started = False

    def add_subscription(self, address, data_handler):
        if address not in self.receivers:
            print(f'start receiving on: {address}')
            receiver = self.container.create_receiver(self.conn, address)
            self.receivers[address] = (receiver, data_handler)

    def remove_subscription(self, address):
        receiver, _ = self.receivers.pop(address)
        receiver.close()

    def on_start(self, event):
        self.container = event.container
        self.conn = self.container.connect()
        self._started = True

    def on_message(self, event):
        for _, (receiver, data_handler) in self.receivers.items():
            if event.receiver == receiver:
                data_handler(event.message.body)

    def subscribe(self, topic, data_handler):
        address = self._sm.subscribe(topic)
        self.add_subscription(address, data_handler)

    def unsubscribe(self, topic):
        address = self._sm.unsubscribe(topic)
        self.remove_subscription(address)

    def pause(self, topic):
        self._sm.pause(topic)

    def resume(self, topic):
        self._sm.resume(topic)

    def has_started(self):
        return self._started

class Subscriber():

    def __init__(self, url, subscriber_middleware):
        if not isinstance(subscriber_middleware, SubscriberMiddleware):
            raise ValueError('publisher_middleware should be instance of SubscriberMiddleware')

        self.url = url
        self._sm = subscriber_middleware
        self._sub_handler = SubscriberHandler(self._sm)

        self._thread = threading.Thread(target=self.run)
        self._thread.daemon = True
        self._thread.start()

    def run(self):
        self._reactor = Container(self._sub_handler).run()

    def get_topics(self):
        return self._sm.get_topics()

    def subscribe(self, topic, data_handler):
        self._sub_handler.subscribe(topic, data_handler)

    def unsubscribe(self, topic):
        self._sub_handler.unsubscribe(topic)

    def pause(self, topic):
        self._sub_handler.pause(topic)

    def resume(self, topic):
        self._sub_handler.resume(topic)

    def is_running(self):
        return self._thread.is_alive() and self._sub_handler.has_started()

class SubscriberError(Exception):
    pass
