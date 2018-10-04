from pypeman import nodes, channels

print("Project loaded")

chan = channels.BaseChannel(name='base')
chan.append(nodes.Log(name="log"))
