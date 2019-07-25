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
from typing import Optional

import proton

from swim_pubsub.core.broker_handlers import BrokerHandler
from swim_pubsub.core.topics import TopicGroup

__author__ = "EUROCONTROL (SWIM)"


_logger = logging.getLogger(__name__)


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
        self.sender = None

    def on_start(self, event: proton.Event) -> None:
        """
        Is triggered upon running the `proton.Container` that uses this handler.

        :param event:
        """
        # call the parent event handler first to take care of connection
        super().on_start(event)

        self.sender = self._create_sender(self.endpoint)

        # assigns the sender on all available topic groups and schedules them
        for topic_group in self.topic_groups:
            topic_group.sender = self.sender
            self._schedule_topic_group(topic_group)

    def add_topic_group(self, topic_group: TopicGroup) -> None:
        """
        # NOTE: This approach of publisher does not allow to modify the set of topic groups while the app is running.
                That would mean to sync the topics in Subscription Manager as well which is not currently 100% supported

        """
        if self.started:
            _logger.warning("Cannot add new topic group while app is running.")
            return

        self.topic_groups.append(topic_group)

    def _schedule_topic_group(self, topic_group: TopicGroup) -> None:
        """
        :param topic_group:
        """

        self.container.schedule(topic_group.interval_in_sec, topic_group)
