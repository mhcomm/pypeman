
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

You can specify a message store (see below) at channel initialisation
if you want to save all processed message. Use `message_store` argument with
an instance of wanted message store.

* Add node with `.add(*nodes)`
* Duplicate path with `.fork()`
* Make alternative path with `.when(condition)`

### HttpChannel

TBD

### FilewatcherChannel

TBD

### TimeChannel

TBD
 

## Nodes

A node is a processing unit in a chain.
To create a node, inherit form `pypeman.nodes.Node` and overide `process(msg)` method.

### ThreadNode

TBD

### LogNode

TBD


## Messages

Message contains the core information processed by nodes and carried by channel.
The message payload can be of any type: Json, Xml, Soap, Hl7, text, ...

Useful attributes:

* payload: The message content
* meta: Message metadata, should be used to add extra information about the payload
* context: Previous message can be saved in context dict for further access.


## Endpoints

Endpoints are server instance used by channel to get message from net protocols like HTTP, Soap or HL7, ....
They listen to a specific port for a specific protocol.

## Message Stores

A Message store is really useful to keep a copy of all message sent to a channel.
It's like a log but with complete message data and metadata. This way you can trace all
Processing or replay a specific message (Not implemented yet).

### FileMessageStore

A `FileMessageStore` save all message in file in a directory hierachy.