__author__ = 'jeremie'


from pyesb import endpoints, nodes, Sequence, receiver, chain, Channel


""" -------------- """

class Destock(Sequence):
    inpoint = nodes.MLLP(port=32222, auto_ack=False).source('ack')

    outpoint = nodes.FileWriter(path="A/B/C").source(inpoint.output)
    ack = "sample"

class Destock2(Sequence):
    def __init__(self):
        nodes.MLLP(to="input", port=32222, auto_ack=False).source('ack')

        nodes.FileWriter(path="A/B/C").source('input')

        nodes.HL7AckGenerator('ack', version='2.5').source("input")


# Routing to "view" to process messages
# something like

# endpoints.MLLP(views.Destock3, port=23412)
# endpoints.HTTP(views.Destock3, url="/sample/toto")
# endpoints.Timer(views.Destock3, period="30m")


class Destock3(Sequence):


    def process(self, payload):
        # Param context
        # or
        payload = self.source.get()

        payload.toFile(path="A/B/C")
        # or
        nodes.FileWriter.write(payload, path="A/B/C")

        self.destination.send(nodes.HL7AckGenerator(version='2.5').generate())
        # or
        return nodes.HL7AckGenerator(version='2.5')

    def __init__(self):
        self.receiver = nodes.MLLP(to="input", port=32222, auto_ack=False)




@receiver(protocol='MLLP', port='3322')
def process(payload):
    nodes.FileWriter(payload, path="a/b/c")
    return nodes.HL7AckGenerator(version='2.5')


@chain(type='socket', proto='MLLP', port='8973')
def hl7Chain(channel):
    channel

# chain, jobs, receiver, cage, node, endpoint, payload,
# message, data, channel, pipeline, line

c = Channel()

c.add(nodes.FileWriter(path="A/B/C"))

c.add()

