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
import json
import uuid

from swim_pubsub.subscriber import Subscriber
from swim_pubsub.middleware import SubscriberMiddleware
from swim_pubsub.rabbitqm_client import RabbitmqManagementClient

__author__ = "EUROCONTROL (SWIM)"


class MyMiddleware(SubscriberMiddleware):
    def __init__(self):
        host = 'https://localhost:15671'
        verify = '/media/alex/Data/dev/work/eurocontrol/RabbitMQ-docker/certs/ca_certificate.pem'
        self.rabbit_client = RabbitmqManagementClient(host, verify=verify)

    def get_queue_from_topic(self, topic):
        bindings = self.rabbit_client.get_bindings()
        queues = [
            b['destination']
            for b in bindings
            if b['destination_type'] == 'queue'
            and b['routing_key'] == topic
        ]

        if not queues:
            return

        return queues[0]

    def subscribe(self, topic):
        key = topic
        topic, *_ = topic.split(".")

        queue = uuid.uuid4().hex

        self.rabbit_client.create_queue(queue)
        self.rabbit_client.bind_queue(queue, topic, key)

        #TODO: add new subscription in SM

        return queue

    def unsubscribe(self, topic):
        queue = self.get_queue_from_topic(topic)
        if not queue:
            print(f'No queue found for {topic}')
            return

        print(f"deleting {queue}")
        r = self.rabbit_client.delete_queue(queue)
        print(r)
        # TODO: remove subscription in SM

    def pause(self, topic):

        # pause subscription in SM

        return f'localhost:5672/opensky/{topic}-queue1'

    def resume(self, topic):

        # resume subscription in SM

        return f'localhost:5672/opensky/{topic}-queue1'


def handle_brussels_arrivals(body, queue):
    arrivals = "\n".join(f'{arr["icao24"]} arrived from {arr["estDepartureAirport"]}' for arr in body['data'])

    with open(f'/home/alex/brussels_arrivals-{queue}', 'a') as f:
        f.write(f'Arrivals: {arrivals}\n')
        f.write(f'Received batch #{body["batch"]}\n\n')


def handle_brussels_departures(body, queue):
    departures = "\n".join(f'{arr["icao24"]} departed to {arr["estArrivalAirport"]}' for arr in body['data'])

    with open(f'/home/alex/brussels_departures-{queue}', 'a') as f:
        f.write(f'Departures: {departures}\n')
        f.write(f'Received batch #{body["batch"]}\n\n')


def handler(body, topic):

    with open(f'/home/alex/data/{topic}', 'a') as f:
        f.write(f'{topic}: {body["data"]}\n')
        f.write(f'Received batch #{body["batch"]}\n\n')


def create_app():
    app = Subscriber(MyMiddleware())

    while not app.is_running():
        pass

    return app

from functools import partial
app = create_app()
# dha = partial(handle_brussels_arrivals, queue='queue1')
# dhd = partial(handle_brussels_departures, queue='queue1')
# app.subscribe('brussels_airport_arrivals_today', dha)
# app.subscribe('brussels_airport_departures_today', dhd)
