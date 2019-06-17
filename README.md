# SWIM PubSub: Publish/Subscribe mini framework 1.2.0

SWIM PubSub implements the publish-subscribe messaging pattern. It is based on the python library [qpid-proton](https://github.com/apache/qpid-proton/tree/master/python)
which extends and can be used to create standalone applications behaving as publishers, subscribers or both.


## [Prerequisites](#prerequisites)

### <a href="#subscription-manager">Subscription Manager</a>
A Subscription Manager keeps track (in DB) of the available topics from publishers and manages the subscriptions of the
subscribers. Moreover, it works closely with a broker (currently tested with [RabbitMQ](https://www.rabbitmq.com/))
where it coordinates the flow of the data from the topics to dedicated queues per subscription based on its DB data. The
functionalities of the Subscription Manager are abstracted away under the [SubscriptionManagerService]() while the
currently used implementation is
[https://bitbucket.org/antavelos-eurocontrol/subscription-manager/src](https://bitbucket.org/antavelos-eurocontrol/subscription-manager/src)


## <a href="#base-classes-and-concepts">Base classes and concepts</a>

### <a href="#app">App</a>
An `swim_pubsub.core.base.App` instance wraps up the functionality of a `proton.Container`. It keeps track of `Client`
instances which can behave as producers, subscribers or both (depending on the their implementation) by interacting
with the Subscription Manager.

### <a href="#broker-handler">BrokerHandler</a>
A `swim_pubsub.core.handlers.BrokerHandler` wraps up the functionality of `proton._handlers.MessagingHandler` which is
basically the event handler mechanism behind [qpid-proton](https://github.com/apache/qpid-proton/tree/master/python) and
can be extended with more specific publisher or subscriber actions.

### <a href="#client">Client</a>
A `swim_pubsum.clients.clients.Client` instance represents a user of an [App](#app) (publisher, subscriber or both) 
which interacts with the broker by sending and receiving data from it as well as the
[Subscription Manager](#subscription-manager) for managing their topics ans subscriptions.

### <a href="#publisher">Publisher</a>
A `swim_pubsum.clients.clients.Publisher` inherits from [Client](#client) by adding extra functionality e.g. for keeping
track of its topics.

### <a href="#subscriber">Subscriber</a>
A `swim_pubsum.clients.clients.Subscriber` inherits from [Client](#client) by adding extra functionality e.g. for 
keeping track of its subscriptions.

###  <a href="#topic-group">TopicGroup</a>
A `swim_pubsub.handlers.TopicGroup` groups instances of [Topic](#topic). It represents a generic type of messaging data
that can be split into sub types (subtopics) and routed in the broker.

### <a href="#topic">Topic</a>
A `swim_pubsub.handlers.Topic` represents a sub type of a [TopicGroup](#topic-group) that takes care of routing its
data in the broker which eventually will be consumed from a dedicated subscriber via a dedicated queue.

### <a href="#subscription-manager-service">SubscriptionManagerService</a>
A `swim_pubsub.services.SubscriptionManagerService` wraps up the functionality of a
[Subscription Manager](#subscription-manager) and is used by a [Client](#client) in order to access it.
