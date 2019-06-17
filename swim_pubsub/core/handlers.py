
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

import logging
from typing import Dict, Callable, Optional, Tuple, List, Any

import proton
from proton._handlers import MessagingHandler

from swim_pubsub.core import ConfigDict
from swim_pubsub.core.errors import BrokerHandlerError
from swim_pubsub.core.utils import get_ssl_domain

__author__ = "EUROCONTROL (SWIM)"

_logger = logging.getLogger(__name__)


class BrokerHandler(MessagingHandler):

    def __init__(self, host: str, ssl_domain: Optional[proton.SSLDomain] = None):
        """
        Base class acting a MessagingHandler to a `proton.Container`. Any custom handler should inherit from this class.

        :param host: host of the broker
        :param ssl_domain: proton SSLDomain for accessing the broker via TSL (SSL)
        """
        MessagingHandler.__init__(self)

        self._host = host
        self.ssl_domain = ssl_domain
        self.started = False

    @property
    def host(self) -> str:
        protocol = "amqps" if self.ssl_domain else "amqp"

        return f"{protocol}://{self._host}"

    def on_start(self, event: proton.Event):
        """
        Is triggered upon running the `proton.Container` that uses this handler. It creates a connection to the broker
        and can be overridden for further startup functionality.

        :param event:
        """
        self.container = event.container
        self.conn = self.container.connect(self.host, ssl_domain=self.ssl_domain)
        self.started = True
        _logger.info(f'Connected to broker @ {self.host}')

    @classmethod
    def create_from_config(cls, config: ConfigDict):
        """
        Factory method for creating an instance from config values

        :param config:
        :return: BrokerHandler
        """
        ssl_domain = get_ssl_domain(
            certificate_db=config['cert_db'],
            cert_file=config['cert_file'],
            cert_key=config['cert_key'],
            password=config['cert_password']
        )
        return cls(host=config['host'], ssl_domain=ssl_domain)


class TopicGroup(MessagingHandler):

    def __init__(self, name: str, interval_in_sec: int, callback: Optional[Callable] = None) -> None:
        """
        A topic group keeps track of similar (in terms of produced data) topics and inherits from
        `proton.MessagingHandler` in order to take advantage of its event scheduling functionality. It coordinates all
        its topics by running their callbacks based on the given interval period.

        :param name: the name of the group
        :param interval_in_sec: how often should the data of its topics produced and dispached.
        :param callback: optional callback that serves as data pre-processor which could be used later by all the topics.
        """
        MessagingHandler.__init__(self)

        self.name: str = name
        self.interval_in_sec: int = interval_in_sec
        self.callback: Callable = callback
        self.topics: List[Topic] = []

        self._sender: Optional[proton.Sender] = None

    @property
    def sender(self):
        return self._sender

    @sender.setter
    def sender(self, value):
        if not self._sender:
            self._sender = value

    @property
    def topic_ids(self) -> List[str]:
        return [topic.id for topic in self.topics]

    def create_topic(self, id: str, callback):
        """
        Creates and appends in the list a new Topic based on the given id and callback
        :param id:
        :param callback:
        """
        if id in self.topic_ids:
            raise BrokerHandlerError(f"There is already topic with id {id}")

        topic = Topic(id, callback)
        self.topics.append(topic)

        return topic

    def dispatch(self) -> None:
        """
        Generate and dispatch messages for each topic in the list
        """
        if not self.sender:
            _logger.info("Not able to dispatch messages because no sender has been assigned yet")
            return

        group_data = self.callback() if self.callback else None

        for topic in self.topics:
            message = topic.generate_message(group_data=group_data)
            if self.sender.credit:
                self.sender.send(message)
                _logger.info(f"Sent message: {message}")

    def on_timer_task(self, event: proton.Event):
        """
        Is triggered upon a scheduled action. In this case the scheduled action is `dispatch`. Dispatching has already
        been scheduled upon initialization of the group and is now being rescheduled.

        :param event:
        """
        self.dispatch()
        event.container.schedule(self.interval_in_sec, self)


class Topic:

    def __init__(self, id: str, callback: Callable):
        """
        Wraps the concept of a broker topic. It generates data through the given callback and routes them in the broker
        based on its id.

        :param id:
        :param callback:
        """
        self.id: str = id
        self.callback: Callable = callback

    def generate_message(self, group_data: Optional[Any] = None):
        """
        Generates the topic data by running the assigned callback. The body of the message will be {'data': data} and
        the type of data depends on the return value of the callback.

        :param group_data: optional data that could be coming from the TopicGroup
        """
        data = self.callback(group_data=group_data)

        result = proton.Message(body={'data': data}, subject=self.id)

        return result


class PublisherBrokerHandler(BrokerHandler):

    def __init__(self, host: str, ssl_domain: Optional[proton.SSLDomain] = None) -> None:
        """
        An implementation of a broker client that is supposed to act as a publisher. It keeps a list of `TopicGroup`
        instances and creates a single `proton.Sender` which assigns to each of them.

        :param host: host of the broker
        :param ssl_domain: proton SSLDomain for accessing the broker via TSL (SSL)
        """
        BrokerHandler.__init__(self, host, ssl_domain)

        self.topic_groups = []
        self.endpoint = '/exchange/amq.topic'

    def on_start(self, event: proton.Event) -> None:
        """
        Is triggered upon running the `proton.Container` that uses this handler.

        :param event:
        """
        # call the parent event handler first to take care of connection
        super().on_start(event)

        self.sender = self.container.create_sender(self.conn, self.endpoint)

        # assigns the sender on all available topic groups and schedules them
        for topic_group in self.topic_groups:
            topic_group.sender = self.sender
            self._schedule_topic_group(topic_group)

    def add_topic_group(self, topic: TopicGroup) -> None:
        """
        :param topic:
        """
        if self.started:
            _logger.info("Cannot add new topic group while running.")

        self.topic_groups.append(topic)

    def _schedule_topic_group(self, topic_group: TopicGroup) -> None:
        """
        :param topic_group:
        """

        self.container.schedule(topic_group.interval_in_sec, topic_group)


class SubscriberBrokerHandler(BrokerHandler):

    def __init__(self, host: str, ssl_domain: Optional[proton.SSLDomain] = None) -> None:
        """
        An implementation of a broker client that is supposed to act as subscriber. It subscribes to queues of the
        broker by creating instances of `proton.Receiver` for each one of them.

        :param host: host of the broker
        :param ssl_domain: proton SSLDomain for accessing the broker via TSL (SSL)
        """
        BrokerHandler.__init__(self, host, ssl_domain)

        # keep track of all the queues by receiver
        self.receivers: Dict[proton.Receiver, Tuple[str, Callable]] = {}

    def _get_receiver_by_queue(self, queue: str) -> proton.Receiver:
        """
        Find the receiver that corresponds to the given queue.
        :param queue:
        :return:
        """
        for receiver, (receiver_queue, _) in self.receivers.items():
            if queue == receiver_queue:
                return receiver

    def create_receiver(self, queue: str, callback: Callable) -> proton.Receiver:
        """
        Create a new `proton.Receiver` and assign the queue and the callback to it

        :param queue: the queue name
        :param callback: a callable that should accept a parameter `data` in order to process the incoming data from the
                         queue.
        """
        receiver = self.container.create_receiver(self.conn, queue)
        self.receivers[receiver] = (queue, callback)

        _logger.debug(f"Created receiver {receiver}")
        _logger.debug(f'Start receiving on: {queue}')

        return receiver

    def remove_receiver(self, queue: str) -> None:
        """
        Remove the receiver that corresponds to the given queue.

        :param queue: the queue name
        """
        receiver = self._get_receiver_by_queue(queue)

        # close the receiver
        receiver.close()
        _logger.debug(f"Closed receiver {receiver} on queue {queue}")

        # remove it from the list
        del self.receivers[receiver]

    def on_message(self, event: proton.Event) -> None:
        """
        Is triggered upon reception of data via the receiver.

        :param event:
        """
        _, callback = self.receivers[event.receiver]

        callback(event.message.body)
