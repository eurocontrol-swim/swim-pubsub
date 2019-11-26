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
from typing import Optional, Any, Dict, List, Set

from rest_client.errors import APIError
from subscription_manager_client.models import Topic as SMTopic

from swim_pubsub.core.clients import PubSubClient
from swim_pubsub.core.errors import PubSubClientError
from swim_pubsub.core.topics import TopicType
from swim_pubsub.publisher.handler import PublisherBrokerHandler
from swim_pubsub.core.subscription_manager_service import SubscriptionManagerService

__author__ = "EUROCONTROL (SWIM)"


_logger = logging.getLogger(__name__)


class Publisher(PubSubClient):

    def __init__(self, broker_handler: PublisherBrokerHandler, sm_service: SubscriptionManagerService):
        """
        Implementation of an actual SubscriptionManager user who acts as a publisher. The broker_handler should be a
        `PublisherBrokerHandler` or a derivative.

        :param broker_handler:
        :param sm_service:
        """
        self.broker_handler: PublisherBrokerHandler = broker_handler  # for type hint

        PubSubClient.__init__(self, broker_handler, sm_service)

        self.topics_dict: Dict[str, TopicType] = {}

    def register_topic(self, topic: TopicType):
        """
        - Keeps a reference to the provided topic
        - Creates the topic in SM
        - Passes it to the broker handler
        :param topic:
        """
        if topic.id in self.topics_dict:
            raise PubSubClientError(f"Topic chain with id {topic.id} already exists.")

        try:
            self.sm_service.create_topic(topic_name=topic.id)
        except APIError as e:
            if e.status_code == 409:
                _logger.error(f"Topic {topic.id} already exists in SM")
            else:
                raise PubSubClientError(f"Error while creating topic in SM: {str(e)}")

        self.topics_dict[topic.id] = topic

        self.broker_handler.add_topic(topic)

    def publish_topic(self, topic_id: str, context: Optional[Any] = None):
        """
        On demand data publish of the provided topic_id

        :param topic_id:
        :param context:
        """
        topic = self.topics_dict.get(topic_id)

        if topic is None:
            raise PubSubClientError(f"Invalid topic id: {topic_id}")

        self.broker_handler.trigger_topic(topic=topic, context=context)

    def sync_sm_topics(self):
        """
        Syncs the topics in SM based on the locally registered once:
            - Topics that exist in SM but not locally will be deleted from SM
            - Topics that exist locally but not in SM will be created in SM
        """
        sm_topics: List[SMTopic] = self.sm_service.get_topics()
        sm_topics_str: List[str] = [topic.name for topic in sm_topics]
        local_topics_str: List[str] = [topic.id for topic in self.topics_dict.values()]

        topics_str_to_create: Set[str] = set(local_topics_str) - set(sm_topics_str)
        topics_str_to_delete: Set[str] = set(sm_topics_str) - set(local_topics_str)
        topic_to_delete: List[SMTopic] = [topic for topic in sm_topics if topic.name in topics_str_to_delete]

        for topic_name in topics_str_to_create:
            self.sm_service.create_topic(topic_name=topic_name)

        for topic in topic_to_delete:
            self.sm_service.delete_topic(topic=topic)
