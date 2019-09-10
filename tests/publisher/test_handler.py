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

from swim_pubsub.core.broker_handlers import BrokerHandler
from swim_pubsub.core.errors import BrokerHandlerError
from swim_pubsub.core.topics import TopicGroup
from swim_pubsub.publisher import PublisherBrokerHandler

__author__ = "EUROCONTROL (SWIM)"


def test_add_topic_group__handler_is_started__logs_message_and_returns(caplog):
    caplog.set_level(logging.DEBUG)

    handler = PublisherBrokerHandler(mock.Mock())
    handler.started = mock.Mock(return_value=True)

    topic_group = TopicGroup(name='group', interval_in_sec=5)

    handler.add_topic_group(topic_group)

    log_message = caplog.records[0]
    assert "Cannot add new topic group while app is running." == log_message.message

    assert 0 == len(handler.topic_groups)


def test_add_topic_group__handler_is_not_started__topic_group_is_added(caplog):
    handler = PublisherBrokerHandler(mock.Mock())

    topic_group = TopicGroup(name='group', interval_in_sec=5)

    handler.add_topic_group(topic_group)

    assert 1 == len(handler.topic_groups)
    assert topic_group in handler.topic_groups


def test_schedule_topic_group():
    handler = PublisherBrokerHandler(mock.Mock())
    handler.container = mock.Mock()
    handler.container.schedule = mock.Mock()

    topic_group = TopicGroup(name='group', interval_in_sec=5)

    handler.add_topic_group(topic_group)

    handler._schedule_topic_group(topic_group)

    handler.container.schedule.assert_called_once_with(topic_group.interval_in_sec, topic_group)


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
    sender = mock.Mock()
    topic_group = TopicGroup(name='group', interval_in_sec=5)

    handler = PublisherBrokerHandler(mock.Mock())
    handler._create_sender = mock.Mock(return_value=sender)
    handler._schedule_topic_group = mock.Mock()
    handler.add_topic_group(topic_group)

    event = mock.Mock()
    with mock.patch.object(BrokerHandler, 'on_start'):
        handler.on_start(event)

        assert sender == topic_group.sender
        handler._schedule_topic_group.assert_called_once_with(topic_group)
