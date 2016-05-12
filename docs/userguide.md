# Core concepts

## Messages

Message contains the core information processed by channels.

Useful attributes:

* payload: Message content
* meta: Message metadata
* context: Previous message can be saved in context dict.


## Channels

Channels is a chain of processing (the nodes) wich use or
transform message content.

* Add node with `add()`
* Duplicate path with `fork()`.
* Make alternative path with `when(condition)`.
 

## Nodes

A node is a processing unit in a chain.
To create a node, overide `process(msg)` method.

## Endpoints

Endpoints are server instance used by channel
which listen to a specific port for a specific protocol.

