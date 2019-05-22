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
from proton._reactor import Container
from rest_client.errors import APIError

from swim_pubsub.base import PubSubApp
from swim_pubsub.publisher.handler import PublisherHandler

__author__ = "EUROCONTROL (SWIM)"


class PublisherAppError(Exception):
    pass


class PublisherApp(PubSubApp):

    def __init__(self, config_file):
        PubSubApp.__init__(self, config_file)
        self.topics_dict = {}

    def register_topic(self, topic):
        if topic.name in self.topics_dict:
            raise PublisherAppError('topic already exists')

        self.topics_dict[topic.name] = topic

    @property
    def topic_names(self):
        return list(self.topics_dict.keys())

    @property
    def topics(self):
        return list(self.topics_dict.values())

    def _populate_topics(self):
        topics_to_populate = [key for topic in self.topics for key in topic.route_keys]
        for topic in topics_to_populate:
            try:
                self.sm_facade.create_topic(topic)
            except APIError as e:
                print(f"{topic}: {str(e)}")

    def run(self):
        if not self.config:
            raise PublisherAppError("No configuration found")

        if not self.topics_dict:
            raise PublisherAppError('At least one topic is required to be registered')

        print(f"Populating the topics to SubscriptionManager\n")
        self._populate_topics()

        try:
            self._handler = PublisherHandler(
                host=self.config['BROKER']['host'],
                ssl_domain=self.ssl_domain,
                topics=self.topics_dict.values()
            )
            self._container = Container(self._handler)
            self._container.run()
        except KeyboardInterrupt:
            pass
