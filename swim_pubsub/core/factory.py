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
from typing import Type

from swim_pubsub.core.base import App
from swim_pubsub.core.handlers import PublisherBrokerHandler, SubscriberBrokerHandler, BrokerHandler
from swim_pubsub.core.utils import yaml_file_to_dict

__author__ = "EUROCONTROL (SWIM)"


def create_app_from_config(config_file: str, broker_handler_class: Type[BrokerHandler]) -> App:
    """
    Entry point for creating an App()
    First it parses the config file and then initializes accordingly the BrokerHandler.
    :param config_file: the path of the config file
    :param broker_handler_class:
    """
    config = yaml_file_to_dict(config_file)
    handler = broker_handler_class.create_from_config(config['BROKER'])
    app = App(handler)
    app.config = config

    # configure logging
    logging.config.dictConfig(app.config['LOGGING'])

    return app


def create_publisher_app_from_config(config_file: str):
    """
    Creates a publisher App by passing the PublisherBrokerHandler specifically.

    :param config_file: the path of the config file
    """
    return create_app_from_config(config_file, broker_handler_class=PublisherBrokerHandler)


def create_subscriber_app_from_config(config_file: str):
    """
    Creates a subscriber App by passing the SubscriberBrokerHandler specifically.

    :param config_file: the path of the config file
    """
    return create_app_from_config(config_file, broker_handler_class=SubscriberBrokerHandler)
