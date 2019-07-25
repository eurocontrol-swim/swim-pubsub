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

from swim_pubsub.core.clients import Client
from swim_pubsub.core.utils import handle_sms_error, handle_broker_handler_error
from swim_pubsub.core.subscription_manager_service import SubscriptionManagerService
from swim_pubsub.subscriber.handler import SubscriberBrokerHandler

__author__ = "EUROCONTROL (SWIM)"


_logger = logging.getLogger(__name__)


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

    @handle_sms_error
    def get_topics(self) -> List[str]:
        """
        Retrieves all the topic names from the SubscriptionManager
        """
        return self.sm_service.get_topics()

    @handle_sms_error
    @handle_broker_handler_error
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

    @handle_sms_error
    @handle_broker_handler_error
    def unsubscribe(self, topic_name: str):
        """
        Unsubscribes the subscriber from the given topic by removing the corresponding receiver from the handler and by
        deleting the corresponding subscription from the SubscriptionManager
        :param topic_name:
        """
        queue = self.subscriptions[topic_name]

        self.broker_handler.remove_receiver(queue)

        self.sm_service.unsubscribe(queue)
        _logger.info("Deleted subscription from Subscription Manager")

    @handle_sms_error
    def pause(self, topic_name: str):
        """
        Pauses the subscription of the given topic by pausing the corresponding subscription in the SubscriptionManager
        :param topic_name:
        """
        queue = self.subscriptions[topic_name]

        self.sm_service.pause(queue)
        _logger.info("Paused subscription in Subscription Manager")

    @handle_sms_error
    def resume(self, topic_name: str):
        """
        Resumes the subscription of the given topic by resuming the corresponding subscription in the SubscriptionManager
        :param topic_name:
        """
        queue = self.subscriptions[topic_name]

        self.sm_service.resume(queue)
        _logger.info("Resumed subscription in Subscription Manager")
