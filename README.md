# SWIM PubSub: Publish/Subscribe mini framework 1.6.2

SWIM PubSub implements the publish-subscribe messaging pattern. It is based on the python library 
[qpid-proton](https://github.com/apache/qpid-proton/tree/master/python) which extends and can be used to create
standalone applications behaving as publishers, subscribers or both.


## Prerequisites

### Subscription Manager
A Subscription Manager keeps track (in DB) of the available topics from publishers and manages the subscriptions of the
subscribers. Moreover, it works closely with a broker (currently tested with [RabbitMQ](https://www.rabbitmq.com/))
where it coordinates the flow of the data from the topics to dedicated queues per subscription based on its DB data. The
functionalities of the Subscription Manager are abstracted away under the 
[SubscriptionManagerService](#subscription-manager-service) while the currently used implementation is
[https://bitbucket.org/antavelos-eurocontrol/subscription-manager/src](https://bitbucket.org/antavelos-eurocontrol/subscription-manager/src)


## Configuration
The configuration of your application should live in a `yml` file in the root directory of your project. There are three
mandatory configuration settings you need to provide:

### Broker
This type of settings involve the host of the broker as well as the TSL configuration needed from a client to connect to
it. More specifically:

  - `host`: the host of the broker, e.g. localhost:5671
  - `cert_db`: the path to the certificate db
  - `cert_file`: the path to the client's certificate
  - `cert_key`: the path to the client's key
  - `cert_password`: the client's password
  - `sasl_user`: the broker user to be used for SASL authentication
  - `sasl_password`: the broker user's password to be used for SASL authentication
  
Example:
```yml
BROKER:
  host: 'localhost:5671'
  cert_db: '/path/to/ca_certificate.pem'
  cert_file: '/path/to/client_certificate.pem'
  cert_key: '/path/to/client_key.pem'
  cert_password: 'mysecurepassword'
  sasl_user: 'mysasluser'
  sasl_password: 'mysaslpassword'
```

Below are the available connection policies which take effect according to specific combinations of the above configuration 
settings with the following order:

  - **TLS**: it takes place when `host`, `cert_db`, `cert_file` and `cert_key` are present. 
  - **SASL**: it takes place when `host`, `cert_db`, `sasl_user` and `sasl_password` are present. 
  - **Unauthenticated**: if only the `host` is present the client will try to connect without authentication. Of course 
  the success of it will also depend on whether the given port is open by the broker.
 
### Subscription Manager
This type of settings involve parameters needed to connect to the Subscription Manager server. More specifically:

  - `host`: the host of the server
  - `https`: indicates whether secure communication will be used or not
  - `timeout`: max time in seconds for a request to the server

Example:
```yml
SUBSCRIPTION-MANAGER:
  host: 'localhost:8080'
  https: false
  timeout: 30
```

### Logging
This type of settings involve general configuration of the logging of the application. Example:

```yml
LOGGING:
  version: 1

  handlers:
    console:
      class: logging.StreamHandler
      formatter: default
      level: DEBUG

  formatters:
    default:
      format: '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
      class: logging.Formatter

  disable_existing_loggers: false

  root:
    level: DEBUG
    handlers: [console]

  loggers:
    swim-pubsub:
      level: DEBUG

    proton:
      level: INFO
```

More settings can be added and can be eventually be used by the config parameter of the applications instance e.g.
`app.config` which is a dictionary.

## Basic Concepts

### Application (App)
`swim_pubsub.core.base.App` extends the `proton.ractor.Container` of `qpid-proton` library by adding extra functionality:

  - it allows the application to keep track of its clients (publishers, subscribers, etc)
  - it allows an application to run in threaded mode. It can be useful in case you need to exploit the app instance 
    further after its initialization, e.g. add or remove clients.

Finally, an app should be better created via the factory method `App.create_from_config` which will parse the 
configuration file and take care of the rest of the initialization. This method, besides the path of the config file it
also accepts a `broker_handler_class` parameter. This can be used by the user in order to pass a custom handler which
has to inherit from `BrokerHandler` (see below). Following you will find relevant examples for the `PubApp` and `SubApp`
case.

### BrokerHandler
It extends the `proton.reactor.MessagingHandler` which will allow it to interact with the broker via the `qpid-proton`
library using the [AMQP Version 1.0](https://www.amqp.org/resources/specifications) protocol.

### Client
It represents a user of an App (publisher, subscriber or both). Upon instantiation it is injected with the broker 
handler of its app (for sending/receiving data to/from the broker) as well as a `SubscriptionManagerService` object (for
managing their topics and subscriptions in the Subscription Manager).

### Topic
It represents the topic to be routed in the broker and it handles its data generation via its data_handler.
Its data generation can be triggered on demand.

### ScheduledTopic
It is a topic but it can also schedule its data generation and publishing in interval periods.

### Subscription Manager Service
It wraps up the functionality of the Subscription Manager and is used by a `Client` in order to access it for topic
and/or subscription management.


## Usage

### Generic App


### PubApp & SubApp
`PubApp` and `SubApp` are extentions of `App` tailor made for the cases of a publisher and subscriber. They provide the 
necessary functionality that will allow them to take full advantage and interact with the Subscription Manager.

#### PubApp
This type of App makes use of two custom objects:

  - `PublisherBrokerHandler`: which extends the `BrokerHandler` by keeping track of the topics groups and their topics 
     as well as by managing their data publication via broker.
  - `Publisher`: which extends the `Client` by allowing to populate its topics in the Subscription Manager.

Similarly to the App, a PubApp instance can be created by using the factory method `create_from_config`. The below 
example demonstrates a simple example of a publisher. It also assumes that a proper config file is in place with 
contents in accordance to the above rules:

```python
import os
import random
import json

from proton import Message

from swim_pubsub.core.topics.topics import Topic, ScheduledTopic
from swim_pubsub.publisher import PubApp


# data handlers
def even_integers(context=None):
    random_integers = [random.randrange(100) for _ in range(100)]

    data = [num for num in random_integers if num % 2 == 0]

    message = Message()
    message.content_type = 'application/json'
    message.body = json.dumps(data)

    return message


def odd_integers(context=None):
    random_integers = [random.randrange(100) for _ in range(100)]

    data = [num for num in random_integers if num % 2 == 1]

    message = Message()
    message.content_type = 'application/json'
    message.body = json.dumps(data)

    return message


# create topics
even_topic = Topic('integers.even', data_handler=even_integers)
odd_topic = ScheduledTopic('integers.odd', data_handler=odd_integers, interval_in_sec=5)

# create app
current_dir = os.path.dirname(os.path.realpath(__file__))
config_file = os.path.join(current_dir, 'config.yml')
app = PubApp.create_from_config(config_file)

# create publisher, the below credentials should belong to a valid Subscription Manager user
publisher = app.register_publisher(username='test', password='test')

# register topics

# the 'integers.even' topic can be triggered lated on demand
publisher.register_topic(even_topic)

# the 'integers.odd' topic will be triggered every 5 seconds
publisher.register_topic(odd_topic)
```

##### Run in foreground
```python

try:
    app.run()
except KeyboardInterrupt:
    pass
```
    
##### Run in the background
If the app runs in the background the publisher the publisher can publish topics on demand
```python
app.run(threaded=True)

publisher.publish_topic('integers.odd')
publisher.publish_topic('integers.even')
```
A typical log output would be similar to the following:
```shell script
2019-11-22 12:11:20,120 - swim_pubsub.core.broker_handlers - INFO - Connected to broker @ amqps://0.0.0.0:5671
2019-11-22 12:11:20,120 - swim_pubsub.publisher.handler - DEBUG - Created sender <proton._endpoints.Sender 0x7f0d32b90490 ~ 0x7f0d1c18bf10>
2019-11-22 12:11:25,122 - swim_pubsub.core.topics.topics - INFO - Sending message for scheduled topic integers.odd
2019-11-22 12:11:25,122 - swim_pubsub.publisher.handler - INFO - Sent message Message(priority=4, body='[51, 85, 29, 21, 37, 3, 53, 87, 71, 41, 23, 1, 7...
2019-11-22 12:11:30,124 - swim_pubsub.core.topics.topics - INFO - Sending message for scheduled topic integers.odd
2019-11-22 12:11:30,124 - swim_pubsub.publisher.handler - INFO - Sent message Message(priority=4, body='[37, 13, 79, 45, 47, 25, 99, 29, 9, 81, 3, 13, 6...
2019-11-22 12:11:35,126 - swim_pubsub.core.topics.topics - INFO - Sending message for scheduled topic integers.odd
2019-11-22 12:11:35,126 - swim_pubsub.publisher.handler - INFO - Sent message Message(priority=4, body='[49, 79, 35, 27, 73, 5, 9, 97, 63, 17, 25, 79, 4...
2019-11-22 12:11:40,127 - swim_pubsub.core.topics.topics - INFO - Sending message for scheduled topic integers.odd
2019-11-22 12:11:40,127 - swim_pubsub.publisher.handler - INFO - Sent message Message(priority=4, body='[91, 85, 5, 3, 81, 23, 93, 7, 89, 3, 63, 31, 9, ...
2019-11-22 12:11:45,130 - swim_pubsub.core.topics.topics - INFO - Sending message for scheduled topic integers.odd
2019-11-22 12:11:45,131 - swim_pubsub.publisher.handler - INFO - Sent message Message(priority=4, body='[93, 67, 19, 45, 73, 41, 19, 65, 59, 53, 11, 55,...
2019-11-22 12:11:50,132 - swim_pubsub.core.topics.topics - INFO - Sending message for scheduled topic integers.odd
2019-11-22 12:11:50,133 - swim_pubsub.publisher.handler - INFO - Sent message Message(priority=4, body='[67, 31, 47, 19, 37, 63, 39, 73, 39, 13, 11, 9, ...
2019-11-22 12:11:55,133 - swim_pubsub.core.topics.topics - INFO - Sending message for scheduled topic integers.odd
2019-11-22 12:11:55,133 - swim_pubsub.publisher.handler - INFO - Sent message Message(priority=4, body='[91, 9, 61, 67, 83, 45, 57, 67, 59, 99, 31, 79, ...
>>> publisher.publish_topic('integers.even')
2019-11-22 12:11:57,474 - swim_pubsub.publisher.handler - INFO - Sending message for topic integers.even
2019-11-22 12:11:57,474 - swim_pubsub.publisher.handler - INFO - Sent message Message(priority=4, body='[38, 60, 12, 52, 84, 80, 26, 2, 58, 52, 24, 92, ...
2019-11-22 12:12:00,136 - swim_pubsub.core.topics.topics - INFO - Sending message for scheduled topic integers.odd
2019-11-22 12:12:00,136 - swim_pubsub.publisher.handler - INFO - Sent message Message(priority=4, body='[99, 31, 91, 91, 87, 17, 5, 61, 51, 79, 47, 97, ...
```

The above code will create two different flows of data (topics) in the broker identified by their id. Every 5 seconds,
100 numbers will be generated, they will be split into odds and evens and they will be routed in the broker. The next 
section describes the concept of a `SubApp` and how it can be used in order to consume such kind of data from a broker.

#### SubApp
This type of App makes use of two custom objects:

  - `SubscriberBrokerHandler`: which extends the `BrokerHandler` by keeping track of the queues and the subscriptions 
     as well as by managing their data consumption via broker.
  - `Subscriber`: which extends the `Client` by allowing to manage its subscriptions in the Subscription Manager.

Similarly to the App, a SubApp instance can be created by using the factory method `create_from_config`. The below 
example demonstrates a simple example of a subscriber. It also assumes that a proper config file is in place with 
contents in accordance to the above rules:

```python
from functools import partial

from swim_pubsub.subscriber import SubApp

# callbacks

# the first parameter of the callback will be used to pass the data from the broker
# it will be a dictionary such as {'data': publisher_data} where publisher_data is the actual data coming from the 
# publisher
def save_numbers(message, type):
    
    with open(f'{type}_numbers.txt', 'a') as f:
        f.write('New message:\n')
        f.write(f'Content Type: {message.content_type}')
        f.write(f"Data: {message.body}\n")
    
save_even_numbers = partial(save_numbers, type='even')
save_odd_numbers = partial(save_numbers, type='odd')

# create and run the app
app = SubApp.create_from_config('config.yml')
app.run(threaded=True)

# create subscribers, the below credentials should belong to a valid Subscription Manager user 
subscriber1 = app.register_publisher(username='test1', password='test')
subscriber2 = app.register_publisher(username='test2', password='test')

# manage their subscriptions
subscriber1.subscribe(topic_name='integers.even', callback=save_even_numbers)
subscriber1.subscribe(topic_name='integers.odd', callback=save_odd_numbers)

subscriber2.subscribe(topic_name='integers.odd', callback=save_odd_numbers)

subscriber1.pause(topic_name='integers.odd')
subscriber1.resume(topic_name='integers.odd')
subscriber1.unsubscribe(topic_name='integers.odd')

subscriber2.unsubscribe(topic_name='integers.odd')
```
> Note that the app is created in threaded mode and the subscribers are created after that. Since the number of 
subscribers as well as their subscriptions usually vary, this method allows us to keep the app running in the 
background and handle the subscribers and their subscriptions on the fly.
