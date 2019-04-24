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
from datetime import datetime, timedelta
from functools import partial

from opensky_network_client.opensky_network import OpenskyNetworkClient
from rest_client.errors import APIError

from swim_pubsub.publisher import Publisher
from swim_pubsub.middleware import PublisherMiddleware

__author__ = "EUROCONTROL (SWIM)"


class MyMiddleware(PublisherMiddleware):

    def sync_topics(self, topics, event=None):
        # do something
        pass

    def load_subscriptions(self, event=None):
        event.subscriptions = {
            'localhost:5672/opensky/brussels_airport_arrivals_today': ['queue1'],
            'localhost:5672/opensky/brussels_airport_departures_today': ['queue1']
        }
        self.injector.trigger(event)


def _today():
    today = datetime.today()
    begin = datetime(today.year, today.month, today.day, 0, 0) - timedelta(days=1)
    end = datetime(today.year, today.month, today.day, 23, 59, 59)

    return int(begin.timestamp()), int(end.timestamp())


class OpenSkyNetworkDataHandler:
    def __init__(self):
        self.client = OpenskyNetworkClient.create('opensky-network.org')

    def arrivals_today_handler(self, airport):
        begin, end = _today()

        try:
            result = self.client.get_flight_arrivals(airport, begin, end, json=True)
        except APIError:
            result = []

        return result

    def departures_today_handler(self, airport):
        begin, end = _today()

        try:
            result = self.client.get_flight_departures(airport, begin, end, json=True)
        except APIError:
            result = []

        return result


if __name__ == '__main__':
    app = Publisher('localhost:5672/opensky', MyMiddleware())

    data_handler = OpenSkyNetworkDataHandler()
    brussels_airport = 'EBBR'
    brussels_airport_arrivals_today = partial(data_handler.arrivals_today_handler, brussels_airport)
    brussels_airport_departures_today = partial(data_handler.departures_today_handler, brussels_airport)

    app.register_topic('brussels_airport_arrivals_today', brussels_airport_arrivals_today)
    app.register_topic('brussels_airport_departures_today', brussels_airport_departures_today)

    app.run()
