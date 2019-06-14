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

from rest_client.errors import APIError
from subscription_manager_client.models import Topic, Subscription
from subscription_manager_client.subscription_manager import SubscriptionManagerClient

__author__ = "EUROCONTROL (SWIM)"


class SubscriptionManagerServiceError(Exception):
    pass


class SubscriptionManagerService:

    def __init__(self, client: SubscriptionManagerClient):
        self.client: SubscriptionManagerClient = client

    def get_topics(self):
        db_topics = self.client.get_topics()

        result = [topic.name for topic in db_topics]

        return result

    def create_topics(self, topic_names):
        for topic_name in topic_names:
            self.create_topic(topic_name)

    def create_topic(self, topic_name):
        topic = Topic(name=topic_name)

        self.client.post_topic(topic)

    def sync_topics(self, topic_names):
        db_topics = self.client.get_topics()

        db_topic_names = [topic.name for topic in db_topics]

        topic_names_to_create = set(topic_names) - set(db_topic_names)
        topic_names_to_delete = set(db_topic_names) - set(topic_names)
        db_topics_to_delete = [topic for topic in db_topics if topic.name in topic_names_to_delete]

        for topic_name in topic_names_to_create:
            self.create_topic(topic_name)

        for db_topic in db_topics_to_delete:
            self.client.delete_topic_by_id(db_topic.id)

    def _get_subscription_by_queue(self, queue):
        subscriptions = self.client.get_subscriptions(queue=queue)

        if not subscriptions:
            raise SubscriptionManagerServiceError(f"No subscription found for queue '{queue}'")

        return subscriptions[0]

    def subscribe(self, topic_name):
        db_topics = self.client.get_topics()

        try:
            topic = [topic for topic in db_topics if topic.name == topic_name][0]
        except IndexError:
            raise SubscriptionManagerServiceError(f"{topic_name} is not registered")

        subscription = Subscription(
            topic_id=topic.id
        )

        try:
            db_subscription = self.client.post_subscription(subscription)
        except APIError as e:
            raise SubscriptionManagerServiceError(f"Error while subscribing to {topic_name}: {str(e)}")

        return db_subscription.queue

    def unsubscribe(self, queue):
        subscription = self._get_subscription_by_queue(queue)

        try:
            self.client.delete_subscription_by_id(subscription.id)
        except APIError as e:
            raise SubscriptionManagerServiceError(f"Error while deleting subscription '{subscription.id}': {str(e)}")

    def pause(self, queue):
        subscription = self._get_subscription_by_queue(queue)

        subscription.active = False

        try:
            self.client.put_subscription(subscription.id, subscription)
        except APIError as e:
            raise SubscriptionManagerServiceError(f"Error while updating subscription '{subscription.id}': {str(e)}")

    def resume(self, queue):
        subscription = self._get_subscription_by_queue(queue)

        subscription.active = True

        try:
            self.client.put_subscription(subscription.id, subscription)
        except APIError as e:
            raise SubscriptionManagerServiceError(f"Error while updating subscription '{subscription.id}': {str(e)}")

