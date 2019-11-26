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
from unittest import mock
from unittest.mock import Mock, call

import pytest
from rest_client.errors import APIError
from subscription_manager_client.models import Topic as SMTopic

from swim_pubsub.core.errors import PubSubClientError
from swim_pubsub.core.topics.topics import Topic, Pipeline
from swim_pubsub.publisher import Publisher

__author__ = "EUROCONTROL (SWIM)"


def test_publisher__register_topic__topic_already_exists__raises_ClientError():
    broker_handler = mock.Mock()
    sm_service = mock.Mock()

    def handler1(context): return "handler1"
    def handler2(context): return context + "handler2"
    def handler3(context): return context + "handler3"

    pipeline = Pipeline([handler1, handler2, handler3])

    topic = Topic(topic_id='topic', pipeline=pipeline)

    publisher = Publisher(broker_handler, sm_service)

    publisher.topics_dict[topic.id] = topic

    with pytest.raises(PubSubClientError) as e:
        publisher.register_topic(topic)
    assert f"Topic chain with id {topic.id} already exists." == str(e.value)


def test_publisher__register_topic__sm_error_409__logs_message(caplog):
    caplog.set_level(logging.DEBUG)

    broker_handler = mock.Mock()
    sm_service = mock.Mock()
    sm_service.create_topic = Mock(side_effect=APIError(status_code=409, detail="error"))

    def handler1(context): return "handler1"
    def handler2(context): return context + "handler2"
    def handler3(context): return context + "handler3"

    pipeline = Pipeline([handler1, handler2, handler3])

    topic = Topic(topic_id='topic', pipeline=pipeline)

    publisher = Publisher(broker_handler, sm_service)

    publisher.register_topic(topic)

    log_message = caplog.records[0]
    assert f"Topic {topic.id} already exists in SM" == log_message.message

    assert topic in publisher.topics_dict.values()


def test_publisher__register_topic__sm_error__raises_ClientError():
    broker_handler = mock.Mock()
    sm_service = mock.Mock()
    sm_service.create_topic = Mock(side_effect=APIError(status_code=500, detail="error"))

    def handler1(context): return "handler1"
    def handler2(context): return context + "handler2"
    def handler3(context): return context + "handler3"

    pipeline = Pipeline([handler1, handler2, handler3])

    topic = Topic(topic_id='topic', pipeline=pipeline)

    publisher = Publisher(broker_handler, sm_service)

    with pytest.raises(PubSubClientError) as e:
        publisher.register_topic(topic)
    assert f"Error while creating topic in SM: [500] - error" == str(e.value)

    assert topic not in publisher.topics_dict.values()


def test_publisher__register_topic__topic_does_not_exist_and_is_registered_in_broker_handler_and_sm():
    broker_handler = mock.Mock()
    sm_service = mock.Mock()

    mock_sm_create_topic = Mock()
    sm_service.create_topic = mock_sm_create_topic

    def handler1(context): return "handler1"
    def handler2(context): return context + "handler2"
    def handler3(context): return context + "handler3"

    pipeline = Pipeline([handler1, handler2, handler3])

    topic = Topic(topic_id='topic', pipeline=pipeline)

    publisher = Publisher(broker_handler, sm_service)

    publisher.register_topic(topic)

    assert topic in publisher.topics_dict.values()
    mock_sm_create_topic.assert_called_once_with(topic_name=topic.id)


def test_publish_topic__topic_id_does_not_exist__raises_clienterror():

    broker_handler = mock.Mock()
    sm_service = mock.Mock()

    publisher = Publisher(broker_handler, sm_service)

    with pytest.raises(PubSubClientError) as e:
        publisher.publish_topic('invalid_topic_id')
    assert f"Invalid topic id: invalid_topic_id" == str(e.value)


def test_publish_topic__topic_exists_and_broker_handler_is_called():
    broker_handler = mock.Mock()
    sm_service = mock.Mock()

    def handler1(context): return "handler1"
    def handler2(context): return context + "handler2"
    def handler3(context): return context + "handler3"

    pipeline = Pipeline([handler1, handler2, handler3])

    topic = Topic(topic_id='topic', pipeline=pipeline)

    publisher = Publisher(broker_handler, sm_service)

    publisher.register_topic(topic)

    mock_trigger_topic = Mock()

    publisher.broker_handler.trigger_topic = mock_trigger_topic

    context = {}
    publisher.publish_topic(topic.id, context=context)

    mock_trigger_topic.assert_called_once_with(topic=topic, context=context)


def test_publisher__sync_topics():

    broker_handler = mock.Mock()
    sm_service = mock.Mock()

    mock_sm_create_topic = Mock()
    sm_service.create_topic = mock_sm_create_topic

    mock_sm_delete_topic = Mock()
    sm_service.delete_topic = mock_sm_delete_topic

    def handler1(context): return "handler1"
    def handler2(context): return context + "handler2"
    def handler3(context): return context + "handler3"

    topic1 = Topic(topic_id='topic1', pipeline=Pipeline([handler1]))
    topic2 = Topic(topic_id='topic2', pipeline=Pipeline([handler2]))
    topic3 = Topic(topic_id='topic3', pipeline=Pipeline([handler3]))

    publisher = Publisher(broker_handler, sm_service)

    # register topics but not all of them are saved in SM
    publisher.register_topic(topic=topic1)
    publisher.topics_dict[topic2.id] = topic2
    publisher.topics_dict[topic3.id] = topic3

    mock_sm_create_topic.assert_called_once_with(topic_name=topic1.id)

    sm_topic1 = SMTopic(id=1, name="topic1")
    sm_topic4 = SMTopic(id=4, name="topic4")
    mock_sm_get_topics = Mock(return_value=[sm_topic1, sm_topic4])
    sm_service.get_topics = mock_sm_get_topics

    publisher.sync_sm_topics()

    # sm_topic4 was deleted because it is not in the current local list of topics
    mock_sm_delete_topic.assert_called_once_with(topic=sm_topic4)

    # topic1 was created during topic registration and topic2, topic3 were created during sync
    assert 3 == mock_sm_create_topic.call_count
    for c in [call(topic_name='topic1'), call(topic_name='topic2'), call(topic_name='topic3')]:
        assert c in mock_sm_create_topic.mock_calls
