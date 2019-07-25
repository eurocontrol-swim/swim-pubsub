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
from rest_client.errors import APIError

from swim_pubsub.core.errors import ClientError
from swim_pubsub.core.topics import TopicGroup
from swim_pubsub.publisher import Publisher

__author__ = "EUROCONTROL (SWIM)"


def test_publisher__register_topic_group__topic_group_already_exists__raises_ClientError():
    broker_handler = mock.Mock()
    sm_service = mock.Mock()
    topic_group = TopicGroup(name='group', interval_in_sec=5)

    publisher = Publisher(broker_handler, sm_service)

    publisher.topic_groups_dict[topic_group.name] = topic_group

    with pytest.raises(ClientError) as e:
        publisher.register_topic_group(topic_group)
        assert f'TopicGroup {topic_group.name} already exists' == str(e)


def test_publisher__register_topic_group__topic_group_does_not_exist_and_is_registered():
    broker_handler = mock.Mock()
    sm_service = mock.Mock()
    topic_group = TopicGroup(name='group', interval_in_sec=5)

    publisher = Publisher(broker_handler, sm_service)

    publisher.register_topic_group(topic_group)

    assert topic_group in publisher.topic_groups


def test_publisher__populate_topics__sm_api_error__logs_error(caplog):
    caplog.set_level(logging.INFO)

    broker_handler = mock.Mock()
    sm_service = mock.Mock()
    topic_group = TopicGroup(name='group', interval_in_sec=5)
    topic_group.create_topic('topic', callback=mock.Mock())

    publisher = Publisher(broker_handler, sm_service)
    publisher.register_topic_group(topic_group)

    publisher.sm_service.create_topic = mock.Mock(side_effect=APIError('server error', status_code=500))

    publisher.populate_topics()

    log_message = caplog.records[0]
    assert f"Error while registering topic: topic: server error"
