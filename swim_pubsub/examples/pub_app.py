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

from swim_pubsub.core.factory import AppFactory
from swim_pubsub.core.handlers import Topic

__author__ = "EUROCONTROL (SWIM)"


def _today():
    today = datetime.today()
    begin = datetime(today.year, today.month, today.day, 0, 0) - timedelta(days=1)
    end = datetime(today.year, today.month, today.day, 23, 59, 59)

    return int(begin.timestamp()), int(end.timestamp())


class OpenSkyNetworkDataHandler:
    def __init__(self):
        self.client = OpenskyNetworkClient.create('opensky-network.org')

    def arrivals_today_handler(self, icao24, pre_data=None):
        begin, end = _today()

        try:
            result = self.client.get_flight_arrivals(icao24, begin, end, json=True)
        except APIError:
            result = []

        return "\n".join(f'{arr["icao24"]} arrived from {arr["estDepartureAirport"]} to {arr["estArrivalAirport"]}'
                         for arr in result)

    def departures_today_handler(self, icao24, pre_data=None):
        begin, end = _today()

        try:
            result = self.client.get_flight_departures(icao24, begin, end, json=True)
        except APIError:
            result = []

        return "\n".join(f'{arr["icao24"]} departed from {arr["estDepartureAirport"]} to {arr["estArrivalAirport"]}'
                         for arr in result)


if __name__ == '__main__':

    pub_app = AppFactory.create_publisher_app_from_config('config.yml')

    data_handler = OpenSkyNetworkDataHandler()

    airports = {
        'Brussels': 'EBBR',
        'Amsterdam': 'EHAM',
        'Paris': 'LFPG',
        'Berlin': 'EDDB',
        'Athens': 'LGAV',
        'Madrid': 'LECU'
    }

    arrivals_topic = Topic('arrivals', 5)
    departures_topic = Topic('departures', 5)

    for airport, icao24 in airports.items():
        arrivals_handler = partial(data_handler.arrivals_today_handler, icao24)
        departures_handler = partial(data_handler.departures_today_handler, icao24)

        arrivals_topic.add_route(key=f"arrivals.{airport.lower()}", handler=arrivals_handler)
        departures_topic.add_route(key=f"departures.{airport.lower()}", handler=departures_handler)

    publisher = pub_app.create_publisher('test', 'test')
    publisher.register_topic(arrivals_topic)
    publisher.register_topic(departures_topic)

    pub_app.run()
