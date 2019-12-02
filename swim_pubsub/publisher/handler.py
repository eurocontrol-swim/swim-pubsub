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
from typing import Union, Any, Optional, List

import proton

from swim_pubsub.core.broker_handlers import BrokerHandler, Connector
from swim_pubsub.core.errors import BrokerHandlerError
from swim_pubsub.core.topics import TopicType
from swim_pubsub.core.topics.topics import ScheduledTopic, TopicDataHandlerError
from swim_pubsub.core.topics.utils import truncate_message

__author__ = "EUROCONTROL (SWIM)"


_logger = logging.getLogger(__name__)


class PublisherBrokerHandler(BrokerHandler):

    def __init__(self, connector: Connector) -> None:
        """
        An implementation of a broker client that is supposed to act as a publisher. It keeps a list of `TopicGroup`
        instances and creates a single `proton.Sender` which assigns to each of them.

        :param connector: takes care of the connection .i.e TSL, SASL etc
        """
        BrokerHandler.__init__(self, connector)

        self.endpoint: str = '/exchange/amq.topic'
        self._sender: Optional[proton.Sender] = None
        self.topics: List[TopicType] = []

    def on_start(self, event: proton.Event) -> None:
        """
        Is triggered upon running the `proton.Container` that uses this handler. If it has ScheduledTopic items in its
        list they will be initialized and scheduled.

        :param event:
        """
        # call the parent event handler first to take care of the connection with the broker
        super().on_start(event)

        try:
            self._sender = self._create_sender(self.endpoint)
            _logger.debug(f"Created sender {self._sender}")
        except BrokerHandlerError as e:
            _logger.error(f'Error while creating sender: {str(e)}')
            return

        for topic in self.topics:
            if isinstance(topic, ScheduledTopic):
                self._init_scheduled_topic(topic)

    def send_message(self, message: Any, subject: str, content_type='application/json') -> None:
        """
        Sends the provided message via the broker. The subject will serve as a routing key in the broker. Typically it
        should be the respective topic_id.

        :param message:
        :param subject:
        :param content_type:
        """
        if not isinstance(message, proton.Message):
            message = proton.Message(body=message)

        message.subject = subject
        message.content_type = content_type

        if self._sender and self._sender.credit:
            self._sender.send(message)
            _logger.info(truncate_message(message=f"Message sent: {message}", max_length=100))
        else:
            _logger.info(truncate_message(message=f"No credit to send message {message}", max_length=100))

    def add_topic(self, topic: TopicType):
        """
        Adds the provided topic in the list. If is is scheduled topic it will be initialized and scheduled
        :param topic:
        """
        if isinstance(topic, ScheduledTopic) and self.started:
            self._init_scheduled_topic(topic)

        self.topics.append(topic)

    def trigger_topic(self, topic: TopicType, context: Optional[Any] = None):
        """
        Generates the topic data via its data handler and sends them via the broker

        :param topic:
        :param context:
        """

        try:
            data = topic.get_data(context=context)
        except TopicDataHandlerError as e:
            _logger.error(f"Error while getting data of topic {topic.name}: {str(e)}")
            return

        _logger.info(f"Sending message for topic {topic.name}")
        self.send_message(message=data, subject=topic.name)

    def _init_scheduled_topic(self, scheduled_topic: ScheduledTopic):
        """
        Sets the send_message method as callback in the topic and schedules it.
        :param scheduled_topic:
        """
        # assign the message_send_callback on the scheduled topic
        scheduled_topic.set_message_send_callback(self.send_message)

        # and schedule it
        self.container.schedule(scheduled_topic.interval_in_sec, scheduled_topic)
