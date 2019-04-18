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
from proton._reactor import ApplicationEvent, Container

from swim_pubsub.middleware import PublisherMiddleware

__author__ = "EUROCONTROL (SWIM)"


class Subscription:

    def __init__(self, sender):
        self.sender = sender
        self.counter = 0
        self.address = self.sender.target.address

    def dispatch(self, data):
        if self.sender.credit:
            # print(f'sending to {self.sender.target.address} with credit {self.sender.credit}')
            msg = Message(body={'data': f'{data} - {self.counter}'})
            self.sender.send(msg)
            # print(f'sent: -- {self.sender.target.address} - {data} - {self.counter}')
            self.counter += 1


class Topic(MessagingHandler):

    def __init__(self, address, data_handler, interval):
        MessagingHandler.__init__(self)

        self.address = address
        self.data_handler = data_handler
        self.interval = interval
        self.subscriptions = {}

    def add_subscription(self, subscription):
        if subscription.address not in self.subscriptions:
            self.subscriptions[subscription.address] = subscription

    def has_subscription(self, address):
        return address in self.subscriptions

    def dispatch(self):
        data = self.data_handler()
        for _, sub in self.subscriptions.items():
            sub.dispatch(data)

    def on_timer_task(self, event):
        self.dispatch()
        event.container.schedule(self.interval, self)


class PublisherHandler(MessagingHandler):

    def __init__(self, topics, publisher_middleware):
        super(PublisherHandler, self).__init__()

        self.pm = publisher_middleware
        self.topics = topics
        self.topics_dict = {t.address: t for t in self.topics}
        self.subscriptions = []

    def on_start(self, event):
        self.container = event.container
        self.container.selectable(self.pm.injector)
        self.conn = self.container.connect()
        self.pm.sync_topics(self.topics)
        self.container.schedule(1, self)

        for topic in self.topics:
            self._schedule_topic(topic)

    def on_timer_task(self, event):
        self.request_subscriptions()
        self.container.schedule(10, self)

    def _schedule_topic(self, topic):
        self.container.schedule(topic.interval, topic)

    def _new_subscriber(self, address):
        sender = self.container.create_sender(self.conn, address, name=address)
        result = Subscription(sender)

        return result

    def on_subscriptions_loaded(self, event):
        self._update_subscriptions(event.subscriptions)

    def _update_subscriptions(self, subscriptions):
        for topic_address, queues in subscriptions.items():
            topic = self.topics_dict[topic_address]
            for queue in queues:
                address = f'{topic.address}-{queue}'
                if not topic.has_subscription(address):
                    subscription = self._new_subscriber(address)
                    topic.add_subscription(subscription)

    def request_subscriptions(self):
        print('loading subscriptions')
        subscriptions_loaded_event = ApplicationEvent('subscriptions_loaded', connection=self.conn)
        self.pm.load_subscriptions(event=subscriptions_loaded_event)


class Publisher:

    def __init__(self, url, publisher_middleware):
        if not isinstance(publisher_middleware, PublisherMiddleware):
            raise ValueError('publisher_middleware should be instance of PublisherMiddleware')

        self.url = url
        self.publisher_middleware = publisher_middleware
        self.topics = []

    def _topic_addresses(self):
        return [t.address for t in self.topics]

    def _get_address(self, topic_name):
        return f'{self.url}/{topic_name}'

    def register_topic(self, topic_name, data_handler):
        address = self._get_address(topic_name)

        if address in self._topic_addresses():
            raise ValueError(f'{topic_name} is already registered')

        topic = Topic(address, data_handler, 5)

        self.topics.append(topic)

    def run(self):
        if not self.topics:
            raise PublisherError('At least one topic is required to be registered')

        try:
            container = Container(PublisherHandler(self.topics, self.publisher_middleware))
            container.run()
        except KeyboardInterrupt:
            pass


class PublisherError(Exception):
    pass