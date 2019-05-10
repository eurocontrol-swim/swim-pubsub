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

from proton import Message, SSLDomain
from proton._handlers import MessagingHandler
from proton._reactor import Container, EventInjector


__author__ = "EUROCONTROL (SWIM)"


# class Topic(MessagingHandler):
#
#     def __init__(self, name, interval):
#         MessagingHandler.__init__(self)
#
#         self.name = name
#         self.name = name
#         self.routes = []
#         self.endpoint = f'/exchange/{self.name}'
#
#         self.sender = None
#
#     def add_route(self, key, handler, interval):
#         route = Route(key, handler, interval, EventInjector())
#         self.routes.append(route)
#
#     def on_data(self, event):
#         if self.sender and self.sender.credit:
#             self.sender.send(event.data)
#             print(event.data)
#
#
# class Route(MessagingHandler):
#
#     def __init__(self, key, handler, interval, event_injector):
#         MessagingHandler.__init__(self)
#
#         self.key = key
#         self.interval = interval
#         self.handler = handler
#         self.counter = 0
#         self.event_injector = event_injector
#
#     def produce_data(self):
#         data = self.handler()
#         self.counter += 1
#
#         return Message(body={'data': data, 'batch': self.counter}, subject=self.key)
#
#     def on_timer_task(self, event):
#         data = self.produce_data()
#
#         event.container.schedule(self.interval, self)

class Route(MessagingHandler):

    def __init__(self, topic, key, handler, interval):
        MessagingHandler.__init__(self)

        self.topic = topic
        self.key = key
        self.interval = interval
        self.topic_handler = handler
        self.sender = None
        self.counter = 0
        self.endpoint = f'/topic/{self.key}' if topic == 'default' else f'/exchange/{self.topic}/{self.key}'
        # self.endpoint = f'/topic/{self.key}' if topic == 'default' else f'/exchange/{self.topic}'

    def dispatch(self):
        data = self.topic_handler()

        if self.sender and self.sender.credit:
            msg = Message(body={'data': data, 'batch': self.counter})
            # msg = Message(body={'data': data, 'batch': self.counter}, subject=self.key)
            self.sender.send(msg)
            print(data)
            self.counter += 1

    def on_timer_task(self, event):
        self.dispatch()
        event.container.schedule(self.interval, self)


class PublisherHandler(MessagingHandler):

    def __init__(self, routes):
        MessagingHandler.__init__(self)

        # self.pm = publisher_middleware
        self.routes = routes

    def _get_ssl_domain(self):
        ssl_domain = SSLDomain(SSLDomain.VERIFY_PEER)
        # Set the CA certificate
        ssl_domain.set_trusted_ca_db("/media/alex/Data/dev/work/eurocontrol/RabbitMQ-docker/certs/ca_certificate.pem")
        # Set the producer certificate and key to authenticate the producer
        ssl_domain.set_credentials("/media/alex/Data/dev/work/eurocontrol/RabbitMQ-docker/certs/client_certificate.pem",
                                   "/media/alex/Data/dev/work/eurocontrol/RabbitMQ-docker/certs/client_key.pem",
                                   'swim-ti')
        return ssl_domain

    def on_start(self, event):
        self.container = event.container
        self.conn = self.container.connect('amqps://localhost:5671', ssl_domain=self._get_ssl_domain())

        print(f'Connected to broker @ amqps://localhost:5671')
        # self.pm.sync_topics(self.topics)
        # self.container.schedule(1, self)

        for route in self.routes:
            self._init_route(route)

    # def on_timer_task(self, event):
    #     self.request_subscriptions()
    #     self.container.schedule(10, self)

    def _init_route(self, route):
        route.sender = self.container.create_sender(self.conn, route.endpoint)
        self.container.schedule(route.interval, route)
    #
    # def on_subscriptions_loaded(self, event):
    #     self._update_subscriptions(event.subscriptions)
    #
    # def _update_subscriptions(self, subscriptions):
    #     for topic_address, queues in subscriptions.items():
    #         topic = self.topics_dict[topic_address]
    #         for queue in queues:
    #             address = f'{topic.address}-{queue}'
    #             if not topic.has_subscription(address):
    #                 subscription = self._new_subscriber(address)
    #                 topic.add_subscription(subscription)
    #
    # def request_subscriptions(self):
    #     print('loading subscriptions')
    #     subscriptions_loaded_event = ApplicationEvent('subscriptions_loaded', connection=self.conn)
    #     self.pm.load_subscriptions(event=subscriptions_loaded_event)


class Publisher:

    def __init__(self):
        # if not isinstance(publisher_middleware, PublisherMiddleware):
        #     raise ValueError('publisher_middleware should be instance of PublisherMiddleware')
        #
        # self.url = url
        # self.publisher_middleware = publisher_middleware
        self.routes = {}

    def register_route(self, route):
        if route.key in self.routes:
            raise PublisherError('route already exists')

        self.routes[route.key] = route
    # def register_topic(self, topic_name, data_handler):
    #     address = self._get_address(topic_name)
    #
    #     if address in self._topic_addresses():
    #         raise ValueError(f'{topic_name} is already registered')
    #
    #     topic = Topic(address, data_handler, 5)
    #
    #     self.topics.append(topic)

    def run(self):
        if not self.routes:
            raise PublisherError('At least one route is required to be registered')

        try:
            container = Container(PublisherHandler(self.routes.values()))
            container.run()
        except KeyboardInterrupt:
            pass


class PublisherError(Exception):
    pass
