__author__ = 'jeremie'


from pyesb import endpoints, sources


""" -------------- """
# Tokens are exchanged between two nodes to know who can work on the message
# payload
sources.Timer(period="30s").to("IN")

query = """SELECT * FROM table"""

sources.HTTPQuery("IN", url="https://localhost:8282", method="POST", data={"query":query}).to("B")




