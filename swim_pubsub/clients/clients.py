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
from typing import Dict, List, Callable

from rest_client.errors import APIError
from subscription_manager_client.subscription_manager import SubscriptionManagerClient

from swim_pubsub.clients.utils import sms_error_handler
from swim_pubsub.core import ConfigDict
from swim_pubsub.core.handlers import BrokerHandler, PublisherBrokerHandler, TopicGroup, SubscriberBrokerHandler
from swim_pubsub.services.subscription_manager import SubscriptionManagerService
from swim_pubsub.clients.errors import ClientError

__author__ = "EUROCONTROL (SWIM)"

_logger = logging.getLogger(__name__)


class Client:

    def __init__(self, broker_handler: BrokerHandler, sm_service: SubscriptionManagerService) -> None:
        """
        Represents an actual SubscriptionManager user (publisher or subscriber)

        :param broker_handler: the BrokerHandler used by the app which is passed to every client using it.
        :param sm_service: the service proxy which will be used to access the SubscriptionManager
        """
        self.broker_handler: BrokerHandler = broker_handler
        self.sm_service: SubscriptionManagerService = sm_service

    @classmethod
    def create(cls,
               broker_handler: BrokerHandler,
               sm_config: ConfigDict,
               username: str,
               password: str):
        """
        Helper factory constructor
        :param broker_handler:
        :param sm_config:
        :param username: the actual SubscriptionManager username of the client
        :param password: the actual SubscriptionManager password of the client
        :return: Client
        """
        sm_client = cls._create_sm_client(sm_config, username, password)

        sm_service =  SubscriptionManagerService(sm_client)

        return cls(broker_handler, sm_service)

    @staticmethod
    def _create_sm_client(config: ConfigDict, username: str, password: str) -> SubscriptionManagerClient:
        """
        Create a SubscriptionManagerClient but first make a check whether the given credentials a valid.
        :param config:
        :param username:
        :param password:
        :return:
        """
        sm_client: SubscriptionManagerClient = SubscriptionManagerClient.create(
            host=config['host'],
            https=config['https'],
            timeout=config['timeout'],
            username=username,
            password=password
        )

        try:
            sm_client.ping_credentials()
        except APIError as e:
            if e.status_code == 401:
                raise ClientError('Invalid user credentials')
            raise

        return sm_client


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
        if topic_group.name in self.topic_groups:
            raise ClientError(f'TopicGroup {topic_group.name} already exists')

        self.topic_groups_dict[topic_group.name] = topic_group

        self.broker_handler.add_topic_group(topic_group)

    def populate_topics(self):
        """
        Registers the topics of each group to the SubscriptionManager. It can be used before starting the corresponding
        app.
        """
        topics_to_populate = [id for topic_group in self.topic_groups for id in topic_group.topic_ids]

        for topic in topics_to_populate:
            try:
                self.sm_service.create_topic(topic)
            except APIError as e:
                _logger.info(f"Error while registering {topic}: {str(e)}")


class Subscriber(Client):

    def __init__(self, broker_handler: SubscriberBrokerHandler, sm_service: SubscriptionManagerService):
        """
        Implementation of an actual SubscriptionManager user who acts as a publisher. The broker_handler should be a
        `SubscriberBrokerHandler` or a derivative.

        :param broker_handler:
        :param sm_service:
        """
        self.broker_handler: SubscriberBrokerHandler = broker_handler  # for type hint

        Client.__init__(self, broker_handler, sm_service)

        self.subscriptions: Dict[str, str] = {}

    @sms_error_handler
    def get_topics(self) -> List[str]:
        """
        Retrieves all the topic names from the SubscriptionManager
        """
        return self.sm_service.get_topics()

    @sms_error_handler
    def subscribe(self, topic_name: str, callback: Callable):
        """
        Subscribes the subscriber to the given topic name in the SubscriptionManager. The SubscriptionManager will
        crate a new unique queue which will be used to reveive data from the given topic. The callback will be called
        upon receiving any data from the queue.

        :param topic_name:
        :param callback:
        """
        queue = self.sm_service.subscribe(topic_name)
        _logger.info(f"Subscribed in SM and got unique queue: {queue}")

        self.broker_handler.create_receiver(queue, callback)

        self.subscriptions[topic_name] = queue

    @sms_error_handler
    def unsubscribe(self, topic_name: str):
        """
        Unsubscribes the subscriber from the given topic by removing the corresponding receiver from the handler and by
        deleting the corresponding subscription from the SubscriptionManager
        :param topic_name:
        """
        queue = self.subscriptions[topic_name]

        self.broker_handler.remove_receiver(queue)

        self.sm_service.unsubscribe(queue)
        _logger.info("Deleted subscription from SM")

    @sms_error_handler
    def pause(self, topic_name: str):
        """
        Pauses the subscription of the given topic by pausing the corresponding subscription in the SubscriptionManager
        :param topic_name:
        """
        queue = self.subscriptions[topic_name]

        self.sm_service.pause(queue)
        _logger.info("Paused subscription in SM")

    @sms_error_handler
    def resume(self, topic_name: str):
        """
        Resumes the subscription of the given topic by resuming the corresponding subscription in the SubscriptionManager
        :param topic_name:
        """
        queue = self.subscriptions[topic_name]

        self.sm_service.resume(queue)
        _logger.info("Resumed subscription in SM")
