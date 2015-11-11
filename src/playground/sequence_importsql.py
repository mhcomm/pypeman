__author__ = 'jeremie'


from pyesb import endpoints, nodes


""" -------------- """
# Tokens are exchanged between two nodes to know who can work on the message
# payload


query = """SELECT * FROM table"""



nodes.SQLQuery("IN", host="localhost", port="9999", backend="msql", query=query)


