Core concepts
=============

In one image better than in 100 words:

.. image:: ./images/general_view.png


Channels
--------

Channels are chains of processing, the nodes, which use or
transform message content. A channel is specialised for a type
of input and can be linked to an endpoint for incoming message.

A channel receive a message, process it and return the response.

Channels are main components of pypeman.
When you want to process a message, you first create a channel then add nodes to
process the message.

.. autoclass:: pypeman.channels.BaseChannel
    :members:


.. ttt
    * Duplicate path with `.fork()`
    * Make conditionnal alternative path with `.when(condition)`
    * Make conditionnal case paths with `.case(condition1, condition2, ...)` returns one channel for each
      arg condition.

..  autoclass:: pypeman.contrib.time.CronChannel
    :members:

..  autoclass:: pypeman.contrib.http.HttpChannel
    :members:

..  autoclass:: pypeman.channels.FileWatcherChannel
    :members:

..  autoclass:: pypeman.contrib.ftp.FTPWatcherChannel
    :members:

..  autoclass:: pypeman.contrib.hl7.MLLPChannel
    :members:


Nodes
-----

A node is a processing unit in a channel.
To create a node, inherit form `pypeman.nodes.BaseNode` and
override `process(msg)` method.

Specific nodes
``````````````
..  autoclass:: pypeman.nodes.BaseNode
    :members:

..  autoclass:: pypeman.nodes.ThreadNode
    :members:

Other nodes
```````````

..  autoclass:: pypeman.nodes.B64Decode
    :members:

..  autoclass:: pypeman.nodes.B64Encode
    :members:

..  autoclass:: pypeman.nodes.Decode
    :members:

..  autoclass:: pypeman.nodes.Encode
    :members:

..  autoclass:: pypeman.nodes.Email
    :members:

..  autoclass:: pypeman.nodes.FileReader
    :members:

..  autoclass:: pypeman.nodes.FileWriter
    :members:

..  autoclass:: pypeman.contrib.ftp.FTPFileReader
    :members:

..  autoclass:: pypeman.contrib.ftp.FTPFileWriter
    :members:

..  autoclass:: pypeman.contrib.hl7.HL7ToPython
    :members:

..  autoclass:: pypeman.contrib.http.HttpRequest
    :members:

..  autoclass:: pypeman.nodes.JsonToPython
    :members:

..  autoclass:: pypeman.nodes.Log
    :members:

..  autoclass:: pypeman.nodes.Map
    :members:

..  autoclass:: pypeman.contrib.hl7.PythonToHL7
    :members:

..  autoclass:: pypeman.nodes.PythonToJson
    :members:

..  autoclass:: pypeman.contrib.xml.PythonToXML
    :members:

..  autoclass:: pypeman.nodes.Sleep
    :members:

..  autoclass:: pypeman.nodes.Save
    :members:

..  autoclass:: pypeman.nodes.ToOrderedDict
    :members:

..  autoclass:: pypeman.contrib.xml.XMLToPython
    :members:

Messages
--------

Message contains the core information processed by nodes and carried by channel.
The message payload may be: Json, Xml, Soap, Hl7, text, Python object...

Useful attributes:

* payload: the message content.
* meta: message metadata, should be used to add extra information about the payload.
* context: previous message can be saved in context dict for further access.

..  autoclass:: pypeman.message.Message
    :members:

Endpoints
---------

Endpoints are server instances used by channel to get message from net protocols like HTTP, Soap or HL7, ....
They listen to a specific port for a specific protocol.

..  autoclass:: pypeman.contrib.http.HTTPEndpoint
    :members:

..  autoclass:: pypeman.contrib.hl7.MLLPEndpoint
    :members:

Message Stores
--------------

A Message store is really useful to keep a copy of all messages sent to a channel.
It's like a log but with complete message data and metadata. This way you can trace all
processing or replay a specific message (Not implemented yet). Each channel can have his message store.

You don't use message stores directly but MessageStoreFactory instance to allow reusing of a configuration.

Generic classes
```````````````

..  autoclass:: pypeman.msgstore.MessageStoreFactory
    :members:

..  autoclass:: pypeman.msgstore.MessageStore
    :members:

Message store factories
```````````````````````

..  autoclass:: pypeman.msgstore.NullMessageStoreFactory
    :members:

..  autoclass:: pypeman.msgstore.FakeMessageStoreFactory
    :members:

..  autoclass:: pypeman.msgstore.MemoryMessageStoreFactory
    :members:

..  autoclass:: pypeman.msgstore.FileMessageStoreFactory
    :members:
