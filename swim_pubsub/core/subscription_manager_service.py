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
from typing import List

from rest_client.errors import APIError
from subscription_manager_client.models import Topic, Subscription
from subscription_manager_client.subscription_manager import SubscriptionManagerClient

from swim_pubsub.core.errors import SubscriptionManagerServiceError

__author__ = "EUROCONTROL (SWIM)"


_logger = logging.getLogger(__name__)


class SubscriptionManagerService:

    def __init__(self, client: SubscriptionManagerClient) -> None:
        """
        Wraps the basic functionalities of the SubscriptionManager
        """
        self.client: SubscriptionManagerClient = client

    def get_topics(self) -> List[str]:
        """
        Retrieves all the available topic names
        :return:
        """
        db_topics = self.client.get_topics()

        result = [topic.name for topic in db_topics]

        return result

    def create_topics(self, topic_names: List[str]):
        """
        Creates new records for each one of the given topics in the Subscription Manager
        """
        for topic_name in topic_names:
            self.create_topic(topic_name)

    def create_topic(self, topic_name: str):
        """
        Creates a new record for the given topic in the Subscription Manager
        """
        topic = Topic(name=topic_name)

        self.client.post_topic(topic)

    def subscribe(self, topic_name: str) -> str:
        """
        Subscribes the client to the given topic.

        :return: A unique queue corresponding to this subscription
        """
        db_topics = self.client.get_topics()

        try:
            topic = [topic for topic in db_topics if topic.name == topic_name][0]
        except IndexError:
            raise SubscriptionManagerServiceError(f"{topic_name} is not registered in Subscription Manager")

        subscription = Subscription(
            topic_id=topic.id
        )

        try:
            db_subscription = self.client.post_subscription(subscription)
        except APIError as e:
            raise SubscriptionManagerServiceError(f"Error while subscribing to {topic_name}: {str(e)}")

        return db_subscription.queue

    def unsubscribe(self, queue: str):
        """
        Unsubscribes the client from the topic that corresponds to the given queue
        """
        subscription = self._get_subscription_by_queue(queue)

        try:
            self.client.delete_subscription_by_id(subscription.id)
        except APIError as e:
            raise SubscriptionManagerServiceError(f"Error while deleting subscription '{subscription.id}': {str(e)}")

    def pause(self, queue: str):
        """
        Deactivates the subscription that corresponds to the given queue
        :param queue:
        """
        subscription = self._get_subscription_by_queue(queue)

        subscription.active = False

        try:
            self.client.put_subscription(subscription.id, subscription)
        except APIError as e:
            raise SubscriptionManagerServiceError(f"Error while updating subscription '{subscription.id}': {str(e)}")

    def resume(self, queue: str):
        """
        Reactivates the subscription that corresponds to the given queue
        """
        subscription = self._get_subscription_by_queue(queue)

        subscription.active = True

        try:
            self.client.put_subscription(subscription.id, subscription)
        except APIError as e:
            raise SubscriptionManagerServiceError(f"Error while updating subscription '{subscription.id}': {str(e)}")

    def _get_subscription_by_queue(self, queue: str) -> Subscription:
        """
        Retrieves a `subscription_manager_client.models.Subscription` by its queue
        """
        subscriptions = self.client.get_subscriptions(queue=queue)

        if not subscriptions:
            raise SubscriptionManagerServiceError(f"No subscription found for queue '{queue}'")

        return subscriptions[0]
