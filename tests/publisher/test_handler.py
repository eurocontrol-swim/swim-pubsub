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
from unittest.mock import Mock

import pytest
from proton import Message

from swim_pubsub.core.broker_handlers import BrokerHandler
from swim_pubsub.core.errors import BrokerHandlerError
from swim_pubsub.core.topics.topics import ScheduledTopic, Topic, PipelineError, Pipeline
from swim_pubsub.publisher import PublisherBrokerHandler

__author__ = "EUROCONTROL (SWIM)"


def test_send_message__no_credit__message_is_not_sent_and_logs_message(caplog):
    caplog.set_level(logging.DEBUG)

    handler = PublisherBrokerHandler(mock.Mock())
    mock_sender = Mock()
    mock_sender.credit = 0
    handler._sender = mock_sender

    message = Message(body="topic data")
    subject = "subject"

    handler.send_message(message=message, subject=subject)

    log_message = caplog.records[0]
    assert f"No credit to send message {message}..." == log_message.message


def test_send_message__enough_credit__message_is_sent_and_logs_message(caplog):
    caplog.set_level(logging.DEBUG)

    handler = PublisherBrokerHandler(mock.Mock())
    mock_sender = Mock()
    mock_sender.send = Mock()
    mock_sender.credit = 1
    handler._sender = mock_sender

    message = Message(body="topic data")
    subject = "subject"

    handler.send_message(message=message, subject=subject)

    assert subject == message.subject
    mock_sender.send.assert_called_once_with(message)

    log_message = caplog.records[0]
    assert f"Message sent: {message}..." == log_message.message


def test_send_message__data_not_message_instance__message_is_converted_and_sent_and_logs_message(caplog):
    caplog.set_level(logging.DEBUG)

    handler = PublisherBrokerHandler(mock.Mock())
    mock_sender = Mock()
    mock_sender.send = Mock()
    mock_sender.credit = 1
    handler._sender = mock_sender

    data = "topic data"
    subject = "subject"

    handler.send_message(message=data, subject=subject)

    message = mock_sender.send.call_args[0][0]

    assert subject == message.subject
    mock_sender.send.assert_called_once_with(message)

    log_message = caplog.records[0]
    assert f"Message sent: {message}..." == log_message.message


def test_add_topic__topic_is_added():
    handler = PublisherBrokerHandler(mock.Mock())

    def handler1(context): return "handler1"
    def handler2(context): return context + "handler2"
    def handler3(context): return context + "handler3"

    pipeline = Pipeline([handler1, handler2, handler3])

    topic = Topic(topic_name='topic', pipeline=pipeline)

    handler.add_topic(topic)

    assert 1 == len(handler.topics)
    assert topic in handler.topics


@pytest.mark.parametrize('handler_started, scheduled_topic_initialized', [(True, True), (False, False)])
def test_add_topic__scheduled_topic_is_added_and_initialized(handler_started,
                                                                         scheduled_topic_initialized):
    handler = PublisherBrokerHandler(mock.Mock())
    handler.started = handler_started
    mock_init_scheduled_topic = Mock()
    handler._init_scheduled_topic = mock_init_scheduled_topic

    def handler1(context): return "handler1"
    def handler2(context): return context + "handler2"
    def handler3(context): return context + "handler3"

    pipeline = Pipeline([handler1, handler2, handler3])

    scheduled_topic = ScheduledTopic(topic_name='topic', pipeline=pipeline, interval_in_sec=5)

    handler.add_topic(scheduled_topic)

    assert 1 == len(handler.topics)
    assert scheduled_topic in handler.topics
    if scheduled_topic_initialized:
        mock_init_scheduled_topic.assert_called_once_with(scheduled_topic)
    else:
        mock_init_scheduled_topic.assert_not_called()


def test_init_scheduled_topic():
    handler = PublisherBrokerHandler(mock.Mock())
    handler.container = mock.Mock()
    handler.container.schedule = mock.Mock()

    def handler1(context): return "handler1"
    def handler2(context): return context + "handler2"
    def handler3(context): return context + "handler3"

    pipeline = Pipeline([handler1, handler2, handler3])

    scheduled_topic = ScheduledTopic(topic_name='topic', pipeline=pipeline, interval_in_sec=5)

    handler._init_scheduled_topic(scheduled_topic)

    assert scheduled_topic._message_send_callback == handler.send_message
    handler.container.schedule.assert_called_once_with(scheduled_topic.interval_in_sec, scheduled_topic)


def test_trigger_topic__pipelineerror_occurs__message_is_not_sent_and_logs_message(caplog):
    caplog.set_level(logging.DEBUG)

    handler = PublisherBrokerHandler(mock.Mock())
    mock_send_message = Mock()
    handler.send_message = mock_send_message
    topic = Mock()
    topic.run_pipeline = Mock(side_effect=PipelineError('pipeline error'))
    topic.name = "1"
    context = {}

    handler.trigger_topic(topic, context)

    log_message = caplog.records[0]
    assert f"Error while getting data of topic {topic.name}: pipeline error" == log_message.message

    mock_send_message.assert_not_called()


def test_trigger_topic__message_is_sent_and_logs_message(caplog):
    caplog.set_level(logging.DEBUG)

    handler = PublisherBrokerHandler(mock.Mock())
    mock_send_message = Mock()
    handler.send_message = mock_send_message
    data = Message(body="data")
    topic = Mock()
    topic.run_pipeline = Mock(return_value=data)
    topic.name = "1"
    context = {}

    handler.trigger_topic(topic, context)

    log_message = caplog.records[0]
    assert f"Sending message for topic {topic.name}" == log_message.message

    mock_send_message.assert_called_once_with(message=data, subject=topic.name)


def test_on_start__create_sender_error__logs_error_and_returns(caplog):
    caplog.set_level(logging.DEBUG)

    handler = PublisherBrokerHandler(mock.Mock())
    handler._create_sender = mock.Mock(side_effect=BrokerHandlerError('proton error'))
    handler._schedule_topic_group = mock.Mock()

    event = mock.Mock()
    with mock.patch.object(BrokerHandler, 'on_start'):
        handler.on_start(event)

        log_message = caplog.records[0]
        assert 'Error while creating sender: proton error' == log_message.message
        handler._schedule_topic_group.assert_not_called()


def test_on_start__no_errors():
    def handler1(context): return "handler1"
    def handler2(context): return context + "handler2"
    def handler3(context): return context + "handler3"
    def handler4(context): return context + "handler4"

    scheduled_topic = ScheduledTopic(topic_name='s_topic', pipeline=Pipeline([handler1, handler2]), interval_in_sec=5)
    topic = Topic(topic_name='topic', pipeline=Pipeline([handler3, handler4]))

    handler = PublisherBrokerHandler(mock.Mock())

    sender = mock.Mock()
    handler._create_sender = mock.Mock(return_value=sender)
    mock_init_scheduled_topic = Mock()
    handler._init_scheduled_topic = mock_init_scheduled_topic

    handler.add_topic(topic)
    handler.add_topic(scheduled_topic)

    event = mock.Mock()
    with mock.patch.object(BrokerHandler, 'on_start'):
        handler.on_start(event)

        assert sender == handler._sender
        mock_init_scheduled_topic.assert_called_once_with(scheduled_topic)
