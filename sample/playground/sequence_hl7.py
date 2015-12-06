__author__ = 'jeremie'


from pyesb import endpoints, nodes


""" -------------- """
# Tokens are exchanged between two nodes to know who can work on the message
# payload

# IN exist for all sequences
endpoints.MLLP("IN", port=32222, auto_ack=False).source('ACK')

nodes.HL7ToMessage("A", version="2.5").source('IN')

nodes.GenHL7Ack("ACK", version="2.5").source('A')

# Fail exist for all sequences and called when there is error raised
# PB, what if fails after ACK ?
nodes.GenHL7NAck("FAIL", version="2.5").to("Fail")

nodes.Mapper("B", map={"MSH.1":"lastname"}).source('A')

@nodes.transformer("C", to="D")
def convert(message):
    if message.stay_state == 'A':
        message.state = "ACTIVE"

    return message


# DROP example
@nodes.filter("C", to="D")
def filter_invalid(message):
    if message.stay_state == 'U':
        raise nodes.DropMessage("Unused state.")

    return message


nodes.MessageToJson("D").to("E")

endpoints.HTTP("E", url="https://portal.lan:30455/patients").to("F")


nodes.Log("LOG", target="sequence1.hl7", message="data = {message}, attr = {message.lastname}")