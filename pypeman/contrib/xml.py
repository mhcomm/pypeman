
from pypeman import endpoints, channels, nodes, message

import xmltodict


class XMLToPython(nodes.BaseNode):
    """ Convert XML message payload to python dict."""

    def __init__(self, *args, **kwargs):
        self.process_namespaces = kwargs.pop('process_namespaces', False)
        super().__init__(*args, **kwargs)


    def process(self, msg):
        msg.payload = xmltodict.parse(msg.payload, process_namespaces=self.process_namespaces)
        msg.content_type = 'application/python'
        return msg


class PythonToXML(nodes.BaseNode):
    """ Convert python payload to XML."""

    def __init__(self, *args, **kwargs):
        self.pretty = kwargs.pop('pretty', False)
        super().__init__(*args, **kwargs)

    def process(self, msg):
        msg.payload = xmltodict.unparse(msg.payload, pretty=self.pretty)
        msg.content_type = 'application/xml'
        return msg