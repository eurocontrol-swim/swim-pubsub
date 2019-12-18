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

__author__ = "EUROCONTROL (SWIM)"

import logging
from typing import Dict, Optional, Any

from swim_pubsub.apps.base import App
from swim_pubsub.broker_handlers.publisher import PublisherBrokerHandler
from swim_pubsub.topics import TopicType

_logger = logging.getLogger(__name__)


class PubApp(App):

    def __init__(self, broker_handler: PublisherBrokerHandler):
        App.__init__(self, broker_handler)

        self.broker_handler: PublisherBrokerHandler = broker_handler  # for type hint

        self.topics_dict: Dict[str, TopicType] = {}

    def register_topic(self, topic: TopicType):
        """
        :param topic:
        """
        if topic.name in self.topics_dict:
            _logger.error(f"Topic with name {topic.name} already exists in broker.")
            return

        self.topics_dict[topic.name] = topic

        self.broker_handler.add_topic(topic)

    def publish_topic(self, topic_name: str, context: Optional[Any] = None):
        """
        On demand data publish of the provided topic_id

        :param topic_name:
        :param context:
        """
        topic = self.topics_dict.get(topic_name)

        if topic is None:
            raise ValueError(f"Invalid topic_name: {topic_name}")

        self.broker_handler.trigger_topic(topic=topic, context=context)

    @classmethod
    def create_from_config(cls, config_file: str):
        return cls._create_from_config(config_file, PublisherBrokerHandler)
