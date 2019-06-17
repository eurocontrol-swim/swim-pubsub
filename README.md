# SWIM PubSub: Publish/Subscribe mini framework 1.2.0

SWIM PubSub implements the publish-subscribe messaging pattern. It is based on the python library [qpid-proton](https://github.com/apache/qpid-proton/tree/master/python)
which extends and can be used to create standalone applications behaving as publishers, subscribers or both.


## [Prerequisites](#prerequisites)

### [Subscription Manager](#subscription-manager)
A Subscription Manager keeps track (in DB) of the available topics from publishers and manages the subscriptions of the
subscribers. Moreover, it works closely with a broker (currently tested with [RabbitMQ](https://www.rabbitmq.com/))
where it coordinates the flow of the data from the topics to dedicated queues per subscription based on its DB data. The
functionalities of the Subscription Manager are abstracted away under the [SubscriptionManagerService]() while the
currently used implementation is
[https://bitbucket.org/antavelos-eurocontrol/subscription-manager/src](https://bitbucket.org/antavelos-eurocontrol/subscription-manager/src)


## [Base classes and concepts](#base-classes-and-concepts)

### [App](#app)
An `swim_pubsub.core.base.App` instance wraps up the functionality of a `proton.Container`. It keeps track of `Client`
instances which can behave as producers, subscribers or both (depending on the their implementation) by interacting
with the Subscription Manager.

### [BrokerHandler](#broker-handler)
A `swim_pubsub.core.handlers.BrokerHandler` wraps up the functionality of `proton._handlers.MessagingHandler` which is
basically the event handler mechanism behind [qpid-proton](https://github.com/apache/qpid-proton/tree/master/python) and
can be extended with more specific publisher or subscriber actions.

### [Client](#client)
A `swim_pubsum.clients.clients.Client` instance represents a user of an <a href="#app">App</a> (publisher, subscriber or both) which
interacts with the broker by sending and receiving data from it as well as the [Subscription Manager]() for managing
their topics ans subscriptions.

### [Publisher](#publisher)
A `swim_pubsum.clients.clients.Publisher` inherits from <a href="#client">Client</a> by adding extra functionality e.g. for keeping track
of its topics

### [Subscriber](#subscriber)
A `swim_pubsum.clients.clients.Subscriber` inherits from <a href="#client">Client</a> by adding extra functionality e.g. for keeping track
of its subscriptions

### [TopicGroup](#topic-group)
A `swim_pubsub.handlers.TopicGroup` groups instances of <a href="#topic">Topic</a>. It represents a generic type of messaging data that
can be split into sub types (subtopics) and routed in the broker.

### [Topic](#topioc)
A `swim_pubsub.handlers.Topic` represents a sub type of a <a href="#topic-group">TopicGroup</a> that takes care of routing its data in the
broker which eventually will be consumed from a dedicated subscriber via a dedicated queue.

### [SubscriptionManagerService](#subscription-manager-service)
A `swim_pubsub.services.SubscriptionManagerService` wraps up the functionality of a <a href="#subscription-manager">Subscription</a> and is used
by a <a href="#client">Client</a> in order to access it.
