Channel testing
===============

Getting started
---------------

Testing framework uses the standard unittest python package. So you can write your tests as always in a `tests` package or
a module visible from your project. To use pypeman specifics helpers, your test case classes must inherit from
`test.PypeTestCase`.

To launch tests, just execute : ::

    pypeman test

Specific helpers
----------------

**PypeTestCase.get_channel(channel_name)**
    To get a channel in test mode from your project file by name.

**Channel_instance.get_node(node_name)**
    To get a specific node by name.

**Node_instance.mock(input=None, output=None)**

    Allows to mock node inputs or the output msg. If you mock output, the original process() is completely bypassed. You can also use
    a function that takes a `msg` argument as input or output mock to use or modify a message.

**Node_instance.last_input()**
    Returns last input message of a node.

**Node_instance.processed**
    Keeps a processed message count. Reset between each test.

