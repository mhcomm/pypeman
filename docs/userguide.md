# Use cases


# Core concepts

In one image better than in 100 words:

![General Architecture](./images/general_view.png)


## Channels

Channels are chains of processing, the nodes, which use or
transform message content. A channel is specialised for a type
of input and can be linked to an endpoint for incoming message.

A channel receive a message, process it and return the response.

Channels are main components of pypeman.
When you want to process a message,
you first create a channel then add nodes to process the message.

Channel ``name`` argument is mandatory and must be unique through the whole project.

You can specify a message store (see below) at channel initialisation
if you want to save all processed message. Use `message_store_factory` argument with
an instance of wanted message store factory.

* Add node with `.add(*nodes)`
* Duplicate path with `.fork()`
* Make conditionnal alternative path with `.when(condition)`
* Make conditionnal case paths with `.case(condition1, condition2, ...)` returns one channel for each
  arg condition.

If you want to create new channel, inherit from the base class and call ``self.handle(msg)`` method
with generated message.

### HttpChannel

Channel that handles Http connection. The Http message is the message payload and some headers become
 metadata of message.

### FilewatcherChannel

Watch for file change or creation. File content becomes message payload. Filename is in message meta.

### TimeChannel

Periodic execution of tasks.

## Nodes

A node is a processing unit in a chain.
To create a node, inherit form `pypeman.nodes.Node` and override `process(msg)` method.

### ThreadNode

Base node that should be used for all long processing nodes.

### LogNode

Debug node log some information with `logging` library.


## Messages

Message contains the core information processed by nodes and carried by channel.
The message payload may be: Json, Xml, Soap, Hl7, text, Python object...

Useful attributes:

* payload: the message content.
* meta: message metadata, should be used to add extra information about the payload.
* context: previous message can be saved in context dict for further access.

## Endpoints

Endpoints are server instances used by channel to get message from net protocols like HTTP, Soap or HL7, ....
They listen to a specific port for a specific protocol.

## Message Stores

A Message store is really useful to keep a copy of all messages sent to a channel.
It's like a log but with complete message data and metadata. This way you can trace all
processing or replay a specific message (Not implemented yet). Each channel can have his message store.

You don't use message stores directly but MessageStoreFactory instance to allow reusing of a configuration.

### FileMessageStoreFactory

Give a `FileMessageStore` instanc that save all messages in file in a directory hierachy.

### MemoryMessageStoreFactory

Give a `MemoryMessageStore` instance that save messages in memory. Lost each time you stop pypeman.
