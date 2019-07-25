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
import uuid
from unittest import mock

import pytest
from rest_client.errors import APIError
from subscription_manager_client.models import Topic

from swim_pubsub.core.errors import SubscriptionManagerServiceError, BrokerHandlerError
from swim_pubsub.subscriber import Subscriber

__author__ = "EUROCONTROL (SWIM)"


def test_subscriber__get_topics__sm_api_error__logs_error_and_raises_SubscriptionManagerServiceError(caplog):
    caplog.set_level(logging.DEBUG)

    broker_handler = mock.Mock()
    sm_service = mock.Mock()
    sm_service.get_topics = mock.Mock(side_effect=SubscriptionManagerServiceError('server error'))

    subscriber = Subscriber(broker_handler, sm_service)

    expected_message = f"Error while accessing Subscription Manager: server error"
    with pytest.raises(SubscriptionManagerServiceError) as e:
        subscriber.get_topics()

        log_message = caplog.records[0]
        assert expected_message == str(e)
        assert expected_message == log_message.message


def test_subscriber__get_topics__no_errors():
    topics = [Topic(name='topic1'), Topic(name='topic2')]

    broker_handler = mock.Mock()
    sm_service = mock.Mock()
    sm_service.get_topics = mock.Mock(return_value=topics)

    subscriber = Subscriber(broker_handler, sm_service)

    assert topics == subscriber.get_topics()


def test_subscriber__subscribe__sm_api_error__logs_error_and_raises_SubscriptionManagerServiceError(caplog):
    caplog.set_level(logging.DEBUG)

    broker_handler = mock.Mock()
    sm_service = mock.Mock()
    sm_service.subscribe = mock.Mock(side_effect=SubscriptionManagerServiceError('server error'))

    subscriber = Subscriber(broker_handler, sm_service)

    expected_message = f"Error while accessing Subscription Manager: server error"
    with pytest.raises(SubscriptionManagerServiceError) as e:
        subscriber.subscribe('topic', callback=mock.Mock())

        log_message = caplog.records[0]
        assert expected_message == str(e)
        assert expected_message == log_message.message


def test_subscriber__broker_handler_error__logs_and_raises_BrokerHandlerError(caplog):
    caplog.set_level(logging.DEBUG)

    queue = uuid.uuid4().hex

    broker_handler = mock.Mock()
    broker_handler.create_receiver = mock.Mock(side_effect=BrokerHandlerError('could not create receiver'))
    sm_service = mock.Mock()
    sm_service.subscribe = mock.Mock(return_value=queue)

    subscriber = Subscriber(broker_handler, sm_service)

    with pytest.raises(BrokerHandlerError) as e:
        subscriber.subscribe('topic', callback=mock.Mock())

        log_messages = caplog.records
        assert 'could not create receiver' == str(e)
        assert 'could not create receiver' == log_messages[0].message
        assert f"Subscribed in SM and got unique queue: {queue}" == log_messages[1].message


def test_subscriber__subscribe__no_errors(caplog):
    caplog.set_level(logging.DEBUG)

    queue = uuid.uuid4().hex
    topic = 'topic'
    callback = mock.Mock()

    broker_handler = mock.Mock()
    broker_handler.create_receiver = mock.Mock()
    sm_service = mock.Mock()
    sm_service.subscribe = mock.Mock(return_value=queue)

    subscriber = Subscriber(broker_handler, sm_service)

    subscriber.subscribe(topic, callback)

    log_message = caplog.records[0]
    assert f"Subscribed in SM and got unique queue: {queue}" == log_message.message

    broker_handler.create_receiver.assert_called_once_with(queue, callback)

    assert subscriber.subscriptions[topic] == queue


def test_subscriber__unsubscribe__sm_api_error__logs_error_and_raises_SubscriptionManagerServiceError(caplog):
    caplog.set_level(logging.DEBUG)

    queue = uuid.uuid4().hex
    topic = 'topic'

    broker_handler = mock.Mock()
    sm_service = mock.Mock()
    sm_service.unsubscribe = mock.Mock(side_effect=SubscriptionManagerServiceError('server error'))

    subscriber = Subscriber(broker_handler, sm_service)
    subscriber.subscriptions[topic] = queue

    expected_message = f"Error while accessing Subscription Manager: server error"
    with pytest.raises(SubscriptionManagerServiceError) as e:
        subscriber.unsubscribe(topic)

        log_message = caplog.records[0]
        assert expected_message == str(e)
        assert expected_message == log_message.message


def test_subscriber__unsubscribe__broker_handler_error__logs_and_raises_BrokerHandlerError(caplog):
    caplog.set_level(logging.DEBUG)

    queue = uuid.uuid4().hex
    topic = 'topic'

    broker_handler = mock.Mock()
    broker_handler.remove_receiver = mock.Mock(side_effect=BrokerHandlerError('could not remove receiver'))
    sm_service = mock.Mock()
    sm_service.unsubscribe = mock.Mock()

    subscriber = Subscriber(broker_handler, sm_service)
    subscriber.subscriptions[topic] = queue

    with pytest.raises(BrokerHandlerError) as e:
        subscriber.unsubscribe(topic)

        log_message = caplog.records[0]
        assert 'could not remove receiver' == str(e)
        assert 'could not remove receiver' == log_message.message
        sm_service.unsubscribe.assert_not_called()


def test_subscriber__unsubscribe__no_errors(caplog):
    caplog.set_level(logging.DEBUG)

    queue = uuid.uuid4().hex
    topic = 'topic'

    broker_handler = mock.Mock()
    broker_handler.remove_receiver = mock.Mock()
    sm_service = mock.Mock()
    sm_service.unsubscribe = mock.Mock()

    subscriber = Subscriber(broker_handler, sm_service)
    subscriber.subscriptions[topic] = queue

    subscriber.unsubscribe(topic)

    log_message = caplog.records[0]
    assert 'Deleted subscription from Subscription Manager' == log_message.message
    sm_service.unsubscribe.assert_called_once_with(queue)
    broker_handler.remove_receiver.assert_called_once_with(queue)


def test_subscriber__pause__sm_api_error__logs_message_and_raises_SubscriptionManagerServiceError(caplog):
    caplog.set_level(logging.DEBUG)

    caplog.set_level(logging.DEBUG)

    queue = uuid.uuid4().hex
    topic = 'topic'

    broker_handler = mock.Mock()
    sm_service = mock.Mock()
    sm_service.pause = mock.Mock(side_effect=SubscriptionManagerServiceError('server error'))

    subscriber = Subscriber(broker_handler, sm_service)
    subscriber.subscriptions[topic] = queue

    expected_message = f"Error while accessing Subscription Manager: server error"
    with pytest.raises(SubscriptionManagerServiceError) as e:
        subscriber.pause(topic)

        log_message = caplog.records[0]
        assert expected_message == str(e)
        assert expected_message == log_message.message


def test_subscriber__pause__no_errors(caplog):
    caplog.set_level(logging.DEBUG)

    caplog.set_level(logging.DEBUG)

    queue = uuid.uuid4().hex
    topic = 'topic'

    broker_handler = mock.Mock()
    sm_service = mock.Mock()
    sm_service.pause = mock.Mock()

    subscriber = Subscriber(broker_handler, sm_service)
    subscriber.subscriptions[topic] = queue

    subscriber.pause(topic)

    log_message = caplog.records[0]
    assert 'Paused subscription in Subscription Manager' == log_message.message
    sm_service.pause.assert_called_once_with(queue)


def test_subscriber__resume__sm_api_error__logs_message_and_raises_SubscriptionManagerServiceError(caplog):
    caplog.set_level(logging.DEBUG)

    caplog.set_level(logging.DEBUG)

    queue = uuid.uuid4().hex
    topic = 'topic'

    broker_handler = mock.Mock()
    sm_service = mock.Mock()
    sm_service.resume = mock.Mock(side_effect=SubscriptionManagerServiceError('server error'))

    subscriber = Subscriber(broker_handler, sm_service)
    subscriber.subscriptions[topic] = queue

    expected_message = f"Error while accessing Subscription Manager: server error"
    with pytest.raises(SubscriptionManagerServiceError) as e:
        subscriber.resume(topic)

        log_message = caplog.records[0]
        assert expected_message == str(e)
        assert expected_message == log_message.message


def test_subscriber__resume__no_errors(caplog):
    caplog.set_level(logging.DEBUG)

    caplog.set_level(logging.DEBUG)

    queue = uuid.uuid4().hex
    topic = 'topic'

    broker_handler = mock.Mock()
    sm_service = mock.Mock()
    sm_service.resume = mock.Mock()

    subscriber = Subscriber(broker_handler, sm_service)
    subscriber.subscriptions[topic] = queue

    subscriber.resume(topic)

    log_message = caplog.records[0]
    assert 'Resumed subscription in Subscription Manager' == log_message.message
    sm_service.resume.assert_called_once_with(queue)