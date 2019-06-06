
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
from proton import Message
from proton._handlers import MessagingHandler

__author__ = "EUROCONTROL (SWIM)"


class Topic(MessagingHandler):

    def __init__(self, name, interval, handler=None):
        MessagingHandler.__init__(self)

        self.name = name
        self.interval = interval
        self.handler = handler
        self.routes = []

        self._sender = None

    @property
    def sender(self):
        return self._sender

    @sender.setter
    def sender(self, value):
        if not self._sender:
            self._sender = value

    @property
    def route_keys(self):
        return [route.key for route in self.routes]

    def add_route(self, key, handler):
        route = Route(key, handler)
        self.routes.append(route)

    def dispatch(self):
        if not self.sender:
            return

        pre_data = self.handler() if self.handler else None

        for route in self.routes:
            data = route.produce_data(pre_data=pre_data)
            if self.sender.credit:
                self.sender.send(data)
                print(data)

    def on_timer_task(self, event):
        self.dispatch()
        event.container.schedule(self.interval, self)


class Route:

    def __init__(self, key, handler):

        self.key = key
        self.handler = handler
        self.counter = 0

    def produce_data(self, pre_data=None):
        data = self.handler(pre_data=pre_data)
        self.counter += 1

        return Message(body={'data': data, 'batch': self.counter}, subject=self.key)


class MessagingHandlerBase(MessagingHandler):

    def __init__(self, host, ssl_domain):
        MessagingHandler.__init__(self)

        self.host = f"amqps://{host}"
        self.ssl_domain = ssl_domain
        self.started = False

    def on_start(self, event):
        self.container = event.container
        self.conn = self.container.connect(self.host, ssl_domain=self.ssl_domain)
        self.started = True
        print(f'Connected to broker @ {self.host}')


class PublisherHandler(MessagingHandlerBase):

    def __init__(self, host, ssl_domain):
        MessagingHandlerBase.__init__(self, host, ssl_domain)

        self.topics = []
        self.endpoint = '/exchange/amq.topic'
        self.container = None

    def on_start(self, event):
        super().on_start(event)

        self.sender = self.container.create_sender(self.conn, self.endpoint)

        for topic in self.topics:
            self._init_topic(topic)

    def add_topic(self, topic):
        self.topics.append(topic)

    def _init_topic(self, topic):
        topic.sender = self.sender
        self.container.schedule(topic.interval, topic)


class SubscriberHandler(MessagingHandlerBase):

    def __init__(self, host, ssl_domain):
        MessagingHandlerBase.__init__(self, host, ssl_domain)

        self.receivers_dict = {}

    def add_receiver(self, queue, data_handler):
        receiver = self.container.create_receiver(self.conn, queue)
        print(f"created receiver {receiver}")
        print(f'start receiving on: {queue}')

        self.receivers_dict[receiver] = (queue, data_handler)

    def remove_receiver(self, queue):
        for receiver, (receiver_queue, _) in self.receivers_dict.items():
            if queue == receiver_queue:
                receiver.close()
                print(f"closed receiver {receiver} on queue {queue}")
                del self.receivers_dict[receiver]
                break

    def on_message(self, event):
        _, data_handler = self.receivers_dict[event.receiver]
        data_handler(event.message.body)
