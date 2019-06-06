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

from swim_pubsub.core.utils import yaml_file_to_dict, get_ssl_domain
from swim_pubsub.core.handlers import PublisherHandler, SubscriberHandler
from swim_pubsub.core.base import PublisherApp, SubscriberApp
from swim_pubsub.services.subscription_manager import SubscriptionManagerConfig

__author__ = "EUROCONTROL (SWIM)"


class AppFactory:

    @classmethod
    def create_publisher_app_from_config(cls, config_file):
        return cls._create_app_from_config(config_file, PublisherHandler, PublisherApp)

    @classmethod
    def create_subscriber_app_from_config(cls, config_file):
        return cls._create_app_from_config(config_file, SubscriberHandler, SubscriberApp)

    @classmethod
    def _create_app_from_config(cls, config_file, msg_handler_class, app_class):
        config = yaml_file_to_dict(config_file)

        msg_handler = cls._create_msg_handler_from_config(config['BROKER'], msg_handler_class)

        sm_config = cls._create_sm_config_from_config(config['SUBSCRIPTION-MANAGER'])

        return app_class(msg_handler, sm_config)

    @classmethod
    def _create_sm_config_from_config(cls, config):
        sm_config = SubscriptionManagerConfig(
            host=config['host'],
            https=config['https'],
            timeout=config['timeout'],
        )

        return sm_config

    @classmethod
    def _create_msg_handler_from_config(cls, config, msg_handler_class):
        ssl_domain = get_ssl_domain(
            certificate_db=config['cert_db'],
            cert_file=config['cert_file'],
            cert_key=config['cert_key'],
            password=config['cert_password']
        )
        msg_handler = msg_handler_class(host=config['host'], ssl_domain=ssl_domain)

        return msg_handler

