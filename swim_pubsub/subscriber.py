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

from proton._handlers import MessagingHandler
from proton._reactor import Container

from swim_pubsub.middleware import SubscriberMiddleware

__author__ = "EUROCONTROL (SWIM)"


class SubscriberHandler(MessagingHandler):

    def __init__(self, subscriber_middleware):
        MessagingHandler.__init__(self)

        self.sm = subscriber_middleware
        self.receivers = {}

    def add_subscription(self, address):
        if address not in self.receivers:
            print(f'start receiving on: {address}')
            receiver = self.container.create_receiver(self.conn, address)
            self.receivers[address] = receiver

    def remove_subscription(self, address):
        receiver = self.receivers.pop(address)
        receiver.close()

    def on_start(self, event):
        self.container = event.container
        self.container.selectable(self.sm.injector)
        self.conn = self.container.connect()

    def on_message(self, event):
        pass


class Subscriber:

    def __init__(self, url, subscriber_middleware):
        if not isinstance(subscriber_middleware, SubscriberMiddleware):
            raise ValueError('publisher_middleware should be instance of SubscriberMiddleware')

        self.url = url
        self._sm = subscriber_middleware
        self._sub_handler = SubscriberHandler(self._sm)
        self._reactor = Container(self._sub_handler)

        self._thread = None

    def start(self):
        self._thread = threading.Thread(target=self._reactor.run)
        self._thread.daemon = True
        self._thread.start()

    def _check_thread(self):
        if not self._thread:
            raise SubscriberError('Subscriber app has not started yet.')

    def get_topics(self):
        self._check_thread()
        return self._sm.get_topics()

    def subscribe(self, topic):
        self._check_thread()
        address = self._sm.subscribe(topic)
        self._sub_handler.add_subscription(address)

    def unsubscribe(self, address):
        self._check_thread()
        self._sm.unsubscribe(address)


    def pause(self):
        self._check_thread()
        self._sm.pause()

    def resume(self):
        self._check_thread()
        self._sm.resume()


class SubscriberError(Exception):
    pass