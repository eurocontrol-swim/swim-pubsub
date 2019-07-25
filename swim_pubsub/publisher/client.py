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
from typing import Dict

from rest_client.errors import APIError

from swim_pubsub.core.clients import Client
from swim_pubsub.core.errors import ClientError
from swim_pubsub.core.topics import TopicGroup
from swim_pubsub.publisher.handler import PublisherBrokerHandler
from swim_pubsub.core.subscription_manager_service import SubscriptionManagerService

__author__ = "EUROCONTROL (SWIM)"


_logger = logging.getLogger(__name__)


class Publisher(Client):

    def __init__(self, broker_handler: PublisherBrokerHandler, sm_service: SubscriptionManagerService):
        """
        Implementation of an actual SubscriptionManager user who acts as a publisher. The broker_handler should be a
        `PublisherBrokerHandler` or a derivative.

        :param broker_handler:
        :param sm_service:
        """
        self.broker_handler: PublisherBrokerHandler = broker_handler  # for type hint

        Client.__init__(self, broker_handler, sm_service)

        self.topic_groups_dict: Dict[str, TopicGroup] = {}

    @property
    def topic_groups(self):
        return list(self.topic_groups_dict.values())

    def register_topic_group(self, topic_group: TopicGroup):
        """
        Adds the topic group in the local dictionary and in the handler's list
        """
        if topic_group.name in self.topic_groups_dict.keys():
            raise ClientError(f'TopicGroup {topic_group.name} already exists')

        self.topic_groups_dict[topic_group.name] = topic_group

        self.broker_handler.add_topic_group(topic_group)

    def populate_topics(self):
        """
        Registers the topics of each group to the SubscriptionManager. It can be used before starting the corresponding
        app.
        """
        topics_to_populate = [topic_id for topic_group in self.topic_groups for topic_id in topic_group.topic_ids]

        for topic in topics_to_populate:
            try:
                self.sm_service.create_topic(topic)
            except APIError as e:
                _logger.info(f"Error while registering topic: {topic}: {str(e)}")
