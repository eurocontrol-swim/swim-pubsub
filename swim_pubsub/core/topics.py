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
from typing import Optional, Callable, List

import proton
from proton._handlers import MessagingHandler

from swim_pubsub.core.errors import BrokerHandlerError, AppError

__author__ = "EUROCONTROL (SWIM)"


_logger = logging.getLogger(__name__)


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

    def __repr__(self):
        return f"<TopicGroup: {self.name} / {self.interval_in_sec} sec>"

    @property
    def sender(self):
        return self._sender

    @sender.setter
    def sender(self, value):
        if not self._sender:
            self._sender = value

    @property
    def topic_ids(self) -> List[str]:
        return [topic.topic_id for topic in self.topics]

    def create_topic(self, topic_id: str, callback):
        """
        Creates and appends in the list a new Topic based on the given id and callback
        :param topic_id:
        :param callback:
        """
        if topic_id in self.topic_ids:
            raise BrokerHandlerError(f"There is already topic with id {topic_id}")

        topic = Topic(topic_id, callback)
        self.topics.append(topic)

        return topic

    def dispatch(self) -> None:
        """
        Generate and dispatch messages for each topic in the list
        """
        if not self.sender:
            _logger.warning("Not able to dispatch messages because no sender has been assigned yet")
            return

        topic_group_data = self.callback() if self.callback else None

        for topic in self.topics:
            try:
                message = topic.callback(topic_group_data=topic_group_data)
            except AppError as e:
                _logger.error(f"Error while generating message for topic {topic}: {str(e)}")
                continue

            # add the topic id as subject in order to route the message accordingly
            message.subject = topic.topic_id

            if self.sender.credit:
                self.sender.send(message)
                _logger.info(f"Sent message: {message}")
            else:
                _logger.info(f"No credit to send message: {message}")

    def on_timer_task(self, event: proton.Event):
        """
        Is triggered upon a scheduled action. In this case the scheduled action is `dispatch`. Dispatching has already
        been scheduled upon initialization of the group and is now being rescheduled.

        :param event:
        """
        self.dispatch()
        event.container.schedule(self.interval_in_sec, self)


class Topic:

    def __init__(self, topic_id: str, callback: Callable):
        """
        Wraps the concept of a broker topic. It generates data through the given callback and routes them in the broker
        based on its id.

        :param topic_id:
        :param callback: the callback which return a proton.Message instance
        """
        self.topic_id: str = topic_id
        self.callback: Callable = callback

    def __repr__(self):
        return f"<Topic: {self.topic_id}>"
