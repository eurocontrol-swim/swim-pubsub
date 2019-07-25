
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
from typing import Optional

import proton
from proton._handlers import MessagingHandler

from swim_pubsub.core import ConfigDict
from swim_pubsub.core import utils
from swim_pubsub.core.errors import BrokerHandlerError

__author__ = "EUROCONTROL (SWIM)"

_logger = logging.getLogger(__name__)


class BrokerHandler(MessagingHandler):

    def __init__(self, host: str, ssl_domain: Optional[proton.SSLDomain] = None):
        """
        Base class acting a MessagingHandler to a `proton.Container`. Any custom handler should inherit from this class.

        :param host: host of the broker
        :param ssl_domain: proton SSLDomain for accessing the broker via TSL (SSL)
        """
        MessagingHandler.__init__(self)

        self._host = host
        self.ssl_domain = ssl_domain
        self.started = False
        self.container = None
        self.conn = None

    @property
    def host(self) -> str:
        protocol = "amqps" if self.ssl_domain else "amqp"

        return f"{protocol}://{self._host}"

    def  on_start(self, event: proton.Event):
        """
        Is triggered upon running the `proton.Container` that uses this handler. It creates a connection to the broker
        and can be overridden for further startup functionality.

        :param event:
        """
        self.container = event.container
        self.conn = self.container.connect(self.host, ssl_domain=self.ssl_domain)
        self.started = True
        _logger.info(f'Connected to broker @ {self.host}')

    def _create_sender(self, endpoint: str) -> proton.Sender:
        try:
            return self.container.create_sender(self.conn, endpoint)
        except Exception as e:
            raise BrokerHandlerError(f'{str(e)}')

    def _create_receiver(self, endpoint: str) -> proton.Receiver:
        try:
            return self.container.create_receiver(self.conn, endpoint)
        except Exception as e:
            raise BrokerHandlerError(f'{str(e)}')

    @classmethod
    def create_from_config(cls, config: ConfigDict):
        """
        Factory method for creating an instance from config values

        :param config:
        :return: BrokerHandler
        """

        ssl_domain = utils.get_ssl_domain(
            certificate_db=config['cert_db'],
            cert_file=config['cert_file'],
            cert_key=config['cert_key'],
            password=config['cert_password']
        ) if config['tls_enabled'] else None

        return cls(host=config['host'], ssl_domain=ssl_domain)
