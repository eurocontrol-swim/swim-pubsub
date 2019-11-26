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
import uuid
from unittest import mock

import pytest
from rest_client.errors import APIError
from subscription_manager_client.models import Topic, Subscription
from subscription_manager_client.subscription_manager import SubscriptionManagerClient

from swim_pubsub.core.errors import SubscriptionManagerServiceError
from swim_pubsub.core.subscription_manager_service import SubscriptionManagerService

__author__ = "EUROCONTROL (SWIM)"


@pytest.mark.parametrize('sm_topics', [
    [Topic('topic1'), Topic('topic2'), Topic('topic3')]
])
def test_get_topics(sm_topics):
    sm_client = SubscriptionManagerClient(mock.Mock())

    sm_client.get_topics = mock.Mock(return_value=sm_topics)

    sm_service = SubscriptionManagerService(sm_client)

    assert sm_topics == sm_service.get_topics()


def test_create_topic():
    sm_client = SubscriptionManagerClient(mock.Mock())

    sm_client.post_topic = mock.Mock()

    sm_service = SubscriptionManagerService(sm_client)

    sm_service.create_topic('topic_name')

    called_topic = sm_client.post_topic.call_args[0][0]

    assert called_topic.name == 'topic_name'


def test_delete_topic():
    sm_client = SubscriptionManagerClient(mock.Mock())

    mock_delete_topic_by_id = mock.Mock()
    sm_client.delete_topic_by_id = mock_delete_topic_by_id

    sm_service = SubscriptionManagerService(sm_client)

    topic = Topic(name='topic', id=1)

    sm_service.delete_topic(topic)

    mock_delete_topic_by_id.assert_called_once_with(topic_id=topic.id)


def test_subscribe__topic_name_is_not_registered_in_sm__raises_SubscriptionManagerServiceError():
    sm_client = SubscriptionManagerClient(mock.Mock())

    sm_client.get_topics = mock.Mock(return_value=[Topic('topic1')])

    sm_service = SubscriptionManagerService(sm_client)

    with pytest.raises(SubscriptionManagerServiceError) as e:
        sm_service.subscribe('topic2')
    assert 'topic2 is not registered in Subscription Manager' == str(e.value)


def test_subscribe__sm_api_error__raises_SubscriptionManagerServiceError():
    sm_client = SubscriptionManagerClient(mock.Mock())

    sm_client.get_topics = mock.Mock(return_value=[Topic('topic1')])

    sm_service = SubscriptionManagerService(sm_client)

    sm_client.post_subscription = mock.Mock(side_effect=APIError('server error', status_code=500))

    with pytest.raises(SubscriptionManagerServiceError) as e:
        sm_service.subscribe('topic1')


def test_subscribe__no_errors__returns_subscription_queue():
    topic = Topic(name='topics', id=1)

    sm_client = SubscriptionManagerClient(mock.Mock())

    sm_client.get_topics = mock.Mock(return_value=[topic])

    sm_service = SubscriptionManagerService(sm_client)

    subscription = Subscription(queue=uuid.uuid4().hex, topic_id=topic.id)

    sm_client.post_subscription = mock.Mock(return_value=subscription)

    queue = sm_service.subscribe('topics')

    assert subscription.queue == queue

    called_subscription = sm_client.post_subscription.call_args[0][0]

    assert topic.id == called_subscription.topic_id


def test_unsubscribe__sm_api_error__raises_SubscriptionManagerServiceError():
    subscription = Subscription(id=1, queue=uuid.uuid4().hex)

    sm_client = SubscriptionManagerClient(mock.Mock())

    sm_service = SubscriptionManagerService(sm_client)

    sm_service._get_subscription_by_queue = mock.Mock(return_value=subscription)

    sm_client.delete_subscription_by_id = mock.Mock(side_effect=APIError('server error', status_code=500))

    with pytest.raises(SubscriptionManagerServiceError) as e:
        sm_service.unsubscribe(subscription.queue)
    assert f"Error while deleting subscription '{subscription.id}': [500] - server error" == str(e.value)


def test_unsubscribe__no_errors():
    subscription = Subscription(id=1, queue=uuid.uuid4().hex)

    sm_client = SubscriptionManagerClient(mock.Mock())

    sm_service = SubscriptionManagerService(sm_client)

    sm_service._get_subscription_by_queue = mock.Mock(return_value=subscription)

    sm_client.delete_subscription_by_id = mock.Mock()

    sm_service.unsubscribe(subscription.queue)

    called_subscription_id = sm_client.delete_subscription_by_id.call_args[0][0]

    assert subscription.id == called_subscription_id


def test_pause__sm_api_error__raises_SubscriptionManagerServiceError():
    subscription = Subscription(id=1, queue=uuid.uuid4().hex, active=True)

    sm_client = SubscriptionManagerClient(mock.Mock())

    sm_service = SubscriptionManagerService(sm_client)

    sm_service._get_subscription_by_queue = mock.Mock(return_value=subscription)

    sm_client.put_subscription = mock.Mock(side_effect=APIError('server error', status_code=500))

    with pytest.raises(SubscriptionManagerServiceError) as e:
        sm_service.pause(subscription.queue)
    called_subscription_id, called_subscription = sm_client.put_subscription.call_args[0]
    assert called_subscription_id == subscription.id
    assert called_subscription.active is False
    assert f"Error while updating subscription '{subscription.id}': [500] - server error" == str(e.value)


def test_pause__no_errors():
    subscription = Subscription(id=1, queue=uuid.uuid4().hex, active=True)

    sm_client = SubscriptionManagerClient(mock.Mock())

    sm_service = SubscriptionManagerService(sm_client)

    sm_service._get_subscription_by_queue = mock.Mock(return_value=subscription)

    sm_client.put_subscription = mock.Mock()

    sm_service.pause(subscription.queue)

    called_subscription_id, called_subscription = sm_client.put_subscription.call_args[0]
    assert called_subscription_id == subscription.id
    assert called_subscription.active is False


def test_resume__sm_api_error__raises_SubscriptionManagerServiceError():
    subscription = Subscription(id=1, queue=uuid.uuid4().hex, active=False)

    sm_client = SubscriptionManagerClient(mock.Mock())

    sm_service = SubscriptionManagerService(sm_client)

    sm_service._get_subscription_by_queue = mock.Mock(return_value=subscription)

    sm_client.put_subscription = mock.Mock(side_effect=APIError('server error', status_code=500))

    with pytest.raises(SubscriptionManagerServiceError) as e:
        sm_service.resume(subscription.queue)
    assert f"Error while updating subscription '{subscription.id}': [500] - server error" == str(e.value)
    called_subscription_id, called_subscription = sm_client.put_subscription.call_args[0]
    assert called_subscription_id == subscription.id
    assert called_subscription.active is True


def test_resume__no_errors():
    subscription = Subscription(id=1, queue=uuid.uuid4().hex, active=False)

    sm_client = SubscriptionManagerClient(mock.Mock())

    sm_service = SubscriptionManagerService(sm_client)

    sm_service._get_subscription_by_queue = mock.Mock(return_value=subscription)

    sm_client.put_subscription = mock.Mock()

    sm_service.resume(subscription.queue)

    called_subscription_id, called_subscription = sm_client.put_subscription.call_args[0]
    assert called_subscription_id == subscription.id
    assert called_subscription.active is True


def test__get_subscription_by_queue__no_subscriptions_found__raises_SubscriptionManagerServiceError():

    sm_client = SubscriptionManagerClient(mock.Mock())

    sm_service = SubscriptionManagerService(sm_client)

    sm_client.get_subscriptions = mock.Mock(return_value=[])

    with pytest.raises(SubscriptionManagerServiceError) as e:
        sm_service._get_subscription_by_queue('queue')
    assert "No subscription found for queue 'queue'" == str(e.value)


def test__get_subscription_by_name__subscription_is_found_and_returned():
    subscription = Subscription(queue='queue')

    sm_client = SubscriptionManagerClient(mock.Mock())

    sm_service = SubscriptionManagerService(sm_client)

    sm_client.get_subscriptions = mock.Mock(return_value=[subscription])

    db_subscription = sm_service._get_subscription_by_queue(subscription.queue)

    assert db_subscription == subscription
