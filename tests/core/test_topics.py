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

import pytest
from proton import Message

from swim_pubsub.core.errors import BrokerHandlerError, AppError
from swim_pubsub.core.topics import TopicGroup, Topic

__author__ = "EUROCONTROL (SWIM)"


def test_topic_group__create_topic__topic_id_already_exists__raises_BrokerHandlerError():
    topic_group = TopicGroup(name='group', interval_in_sec=5)
    topic_group.topics.append(Topic(topic_id='topic', callback=lambda a: a))

    with pytest.raises(BrokerHandlerError) as e:
        topic_group.create_topic('topic', callback=lambda a: a)
        assert "There is already topic with id topic" == str(e)


def test_topic_group__create_topic__topic_does_not_exist_and_is_created():
    topic_group = TopicGroup(name='group', interval_in_sec=5)
    topic_group.topics.append(Topic(topic_id='topic', callback=lambda a: a))

    assert 1 == len(topic_group.topics)
    assert topic_group.topics[0].topic_id == 'topic'


def test_topic_group__dispatch__sender_is_not_set__logs_message_and_returns(caplog):
    caplog.set_level(logging.DEBUG)

    topic_group = TopicGroup(name='group', interval_in_sec=5, callback=mock.Mock())
    topic_group.topics.append(Topic(topic_id='topic1', callback=mock.Mock()))
    topic_group.topics.append(Topic(topic_id='topic2', callback=mock.Mock()))

    topic_group.dispatch()

    log_message = caplog.records[0]
    assert "Not able to dispatch messages because no sender has been assigned yet" == log_message.message
    assert logging.WARNING == log_message.levelno

    topic_group.callback.assert_not_called()
    for topic in topic_group.topics:
        topic.callback.assert_not_called()


def test_topic_group__dispatch__one_topic_raises_AppError__sending_is_skipped(caplog):
    caplog.set_level(logging.DEBUG)

    topic_group = TopicGroup(name='group', interval_in_sec=5, callback=mock.Mock(return_value='data'))
    topic_group.topics.append(Topic(topic_id='topic1', callback=mock.Mock(side_effect=AppError('topic1 error'))))
    topic_group.sender = mock.Mock()
    topic_group.sender.credit = 1000
    topic_group.sender.send = mock.Mock()

    topic_group.dispatch()

    log_message = caplog.records[0]
    assert f"Error while generating message for topic {topic_group.topics[0]}: topic1 error" == log_message.message
    assert logging.ERROR == log_message.levelno

    topic_group.callback.assert_called_once()

    topic_group.topics[0].callback.assert_called_once_with(topic_group_data='data')
    topic_group.sender.send.assert_not_called()


def test_topic_group__dispatch__sender_has_no_credit__sending_is_skipped(caplog):
    caplog.set_level(logging.DEBUG)
    message = Message(subject='topic1')

    topic_group = TopicGroup(name='group', interval_in_sec=5, callback=mock.Mock(return_value='data'))
    topic_group.topics.append(Topic(topic_id='topic1', callback=mock.Mock(return_value=message)))
    topic_group.sender = mock.Mock()
    topic_group.sender.credit = 0
    topic_group.sender.send = mock.Mock()

    topic_group.dispatch()

    topic_group.callback.assert_called_once()

    topic_group.topics[0].callback.assert_called_once_with(topic_group_data='data')
    topic_group.sender.send.assert_not_called()

    log_message = caplog.records[0]
    assert f"No credit to send message: {message}" == log_message.message
    assert logging.INFO == log_message.levelno


def test_topic_group__dispatch__no_obstacles__message_is_sent(caplog):
    caplog.set_level(logging.DEBUG)
    message = Message(subject='topic1')

    topic_group = TopicGroup(name='group', interval_in_sec=5, callback=mock.Mock(return_value='data'))
    topic_group.topics.append(Topic(topic_id='topic1', callback=mock.Mock(return_value=message)))
    topic_group.sender = mock.Mock()
    topic_group.sender.credit = 1000
    topic_group.sender.send = mock.Mock()

    topic_group.dispatch()

    topic_group.callback.assert_called_once()
    topic_group.topics[0].callback.assert_called_once_with(topic_group_data='data')
    topic_group.sender.send.assert_called_once_with(message)

    log_message = caplog.records[0]
    assert f"Sent message: {message}" == log_message.message
    assert logging.INFO == log_message.levelno
