Channel testing
===============

Getting started
---------------

Testing framework use standard unittest python package. So you can write your tests as always in a `tests` package or
module visible from your project. To use pypeman specifics helpers, your test case classes must inherit from
`test.PypeTestCase`.

To launch test, just execute : ::

    pypeman test

Specific helpers
----------------

**PypeTestCase.get_channel(channel_name)**
    To get a channel in test mode from your project file by name.

**Channel_instance.get_node(node_name)**
    To get a specific node by name.

**Node_instance.mock(input=None, output=None)**

    Allow to mock node input or output msg. If you mock output, original process is completely bypassed. You can also use
    function that takes a `msg` argument as input or output mock to use or modify message.

**Node_instance.last_input()**
    Return last input message of a node.

**Node_instance.processed**
    Keep a processed message count. Reseted between each test.

