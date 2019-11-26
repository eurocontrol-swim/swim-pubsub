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
from functools import partial
from unittest.mock import Mock

import pytest

from swim_pubsub.core.topics.topics import Topic, ScheduledTopic, PipelineError, Pipeline

__author__ = "EUROCONTROL (SWIM)"


@pytest.mark.parametrize('handler', [
    1, 1.0, "str", (1,), [], {}
])
def test_pipeline__handler_is_not_callable__raise_valueerror(handler):
    with pytest.raises(ValueError) as e:
        Pipeline([handler])
    assert f"{handler} is not callable" == str(e.value)

    with pytest.raises(ValueError) as e:
        Pipeline().append(handler)
    assert f"{handler} is not callable" == str(e.value)


@pytest.mark.parametrize('handler', [
    lambda context: context,
    partial(lambda context: context)
])
def test_pipeline__handler_is_valid_and_added_in_list(handler):
    p = Pipeline([handler])
    assert handler in p

    p = Pipeline()
    p.append(handler)
    assert handler in p


def test_topic_run_pipeline__all_topic_handlers_are_called_in_chain_mode():
    def handler1(context): return "handler1"
    def handler2(context): return context + "handler2"
    def handler3(context): return context + "handler3"

    pipeline = Pipeline([handler1, handler2, handler3])

    topic = Topic(topic_id='topic', pipeline=pipeline)
    context = {}

    result = topic.run_pipeline(context=context)

    assert "handler1handler2handler3" == result


def test_scheduled_topic__trigger_message_send__no_message_send_handler__returns_and_logs_message(caplog):
    caplog.set_level(logging.DEBUG)

    pipeline = Mock()
    pipeline.run = Mock()

    scheduled_topic = ScheduledTopic(topic_id='topic', pipeline=pipeline, interval_in_sec=5)

    scheduled_topic._trigger_message_send()

    log_message = caplog.records[0]
    expected_message = "Not able to send messages because no sender has been assigned yet for topic 'topic'"
    assert expected_message == log_message.message
    pipeline.run.assert_not_called()


def test_scheduled_topic__trigger_message_send__pipelineerror_returns_and_logs_message(caplog):
    caplog.set_level(logging.DEBUG)

    pipeline = Mock()
    pipeline.run = Mock(side_effect=PipelineError('pipeline error'))

    scheduled_topic = ScheduledTopic(topic_id='topic', pipeline=pipeline, interval_in_sec=5)
    scheduled_topic.set_message_send_callback(Mock())

    scheduled_topic._trigger_message_send()

    log_message = caplog.records[0]
    expected_message = f"Error while getting data of scheduled topic {scheduled_topic.id}: pipeline error"
    assert expected_message == log_message.message


def test_scheduled_topic__trigger_message_send_is_called_normally_and_logs_message(caplog):
    caplog.set_level(logging.DEBUG)

    def handler1(context): return "handler1"
    def handler2(context): return context + "handler2"
    def handler3(context): return context + "handler3"

    pipeline = Pipeline([handler1, handler2, handler3])

    scheduled_topic = ScheduledTopic(topic_id='topic', pipeline=pipeline, interval_in_sec=5)
    mock_message_send_handler = Mock()
    scheduled_topic.set_message_send_callback(mock_message_send_handler)

    scheduled_topic._trigger_message_send()

    log_message = caplog.records[0]
    expected_message = f"Sending message for scheduled topic {scheduled_topic.id}"
    assert expected_message == log_message.message
    mock_message_send_handler.assert_called_once_with(message="handler1handler2handler3", subject=scheduled_topic.id)


def test_scheduled_topic__on_timer_task__message_send_is_triggered_and_task_is_rescheduled(caplog):
    caplog.set_level(logging.DEBUG)

    def handler1(context): return "handler1"
    def handler2(context): return context + "handler2"
    def handler3(context): return context + "handler3"

    pipeline = Pipeline([handler1, handler2, handler3])

    scheduled_topic = ScheduledTopic(topic_id='topic', pipeline=pipeline, interval_in_sec=5)

    scheduled_topic._trigger_message_send = Mock()

    event = Mock()
    event.container = Mock()
    event.container.schedule = Mock()

    scheduled_topic.on_timer_task(event)

    scheduled_topic._trigger_message_send.assert_called_once()
    event.container.schedule.assert_called_once_with(5, scheduled_topic)
