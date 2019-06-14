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

from rest_client.errors import APIError
from subscription_manager_client.subscription_manager import SubscriptionManagerClient

from swim_pubsub.clients.utils import sms_error_handler
from swim_pubsub.services.subscription_manager import SubscriptionManagerService
from swim_pubsub.clients.errors import ClientError

__author__ = "EUROCONTROL (SWIM)"

_logger = logging.getLogger(__name__)


class Client:

    def __init__(self, msg_handler, sm_service):
        self.msg_handler = msg_handler
        self.sm_service = sm_service

    @classmethod
    def create(cls, msg_handler, sm_config, username, password):
        sm_service = cls._create_sm_service(sm_config, username, password)

        return cls(msg_handler, sm_service)

    @classmethod
    def _create_sm_service(cls, config, username, password):
        sm_client: SubscriptionManagerClient = SubscriptionManagerClient.create(
            host=config['host'],
            https=config['https'],
            timeout=config['timeout'],
            username=username,
            password=password
        )

        if not cls._is_valid_user(sm_client):
            raise ClientError('Invalid user credentials')

        return SubscriptionManagerService(sm_client)

    @staticmethod
    def _is_valid_user(sm_client):
        try:
            sm_client.ping_credentials()
        except APIError as e:
            if e.status_code == 401:
                return False
            else:
                raise

        return True


class Publisher(Client):

    def __init__(self, msg_handler, sm_service):
        Client.__init__(self, msg_handler, sm_service)
        self.topics_dict = {}

    @property
    def topics(self):
        return list(self.topics_dict.values())

    def register_topic(self, topic):
        if topic.name in self.topics_dict:
            raise Exception('topic already exists')

        self.topics_dict[topic.name] = topic
        self.msg_handler.add_topic(topic)

    def populate_topics(self):
        topics_to_populate = [key for topic in self.topics for key in topic.topic_ids]

        for topic in topics_to_populate:
            try:
                self.sm_service.create_topic(topic, )
            except APIError as e:
                _logger.info(f"{topic}: {str(e)}")


class Subscriber(Client):

    def __init__(self, msg_handler, sm_service):
        Client.__init__(self, msg_handler, sm_service)
        self.subscriptions = {}

    @sms_error_handler
    def get_topics(self):
        return self.sm_service.get_topics()

    @sms_error_handler
    def subscribe(self, topic_name, data_handler):
        queue = self.sm_service.subscribe(topic_name)
        _logger.info("Subscribed in SM")

        self.msg_handler.create_receiver(queue, data_handler)
        _logger.info('Created receiver')

        self.subscriptions[topic_name] = queue

    @sms_error_handler
    def unsubscribe(self, topic_name):
        queue = self.subscriptions[topic_name]

        self.msg_handler.remove_receiver(queue)
        _logger.info('Removed receiver')

        self.sm_service.unsubscribe(queue)
        _logger.info("Deleted subscription from SM")

    @sms_error_handler
    def pause(self, topic_name):
        queue = self.subscriptions[topic_name]

        self.sm_service.pause(queue)
        _logger.info("Paused subscription in SM")

    @sms_error_handler
    def resume(self, topic_name):
        queue = self.subscriptions[topic_name]

        self.sm_service.resume(queue)
        _logger.info("Resumed subscription in SM")
