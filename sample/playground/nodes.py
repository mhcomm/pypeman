__author__ = 'jeremie'


class Sequence(object):
    def __init__(*args, **kwargs):
        pass

    def add_node(self, node_type, config=None):
        pass


class Node(object):
    def __init__(*args, **kwargs):
        pass




class Filter(Node):
    def __init__(*args, **kwargs):
        pass

class Endpoint(Node):
    def __init__(*args, **kwargs):
        pass

class Connector(Node):
    def __init__(*args, **kwargs):
        pass

class Producer(Node):
    def __init__(*args, **kwargs):
        pass

class Consumer(Node):
    def __init__(*args, **kwargs):
        pass

class Transformer(Node):
    def __init__(*args, **kwargs):
        pass



class Timer(Producer):
    def __init__(*args, **kwargs):
        pass

class Store(Consumer):
    def __init__(*args, **kwargs):
        pass

class Mapper(Transformer):
    def __init__(*args, **kwargs):
        pass

class Log(Consumer):
    def __init__(*args, **kwargs):
        pass

    def process(self, message):
        print message
        return message


def Execute(Transformer):
    def __init__(*args, **kwargs):
        pass