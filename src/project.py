import channels
import nodes

c = channels.HttpChannel(method='*', url='/{name}')

c.add(nodes.Log(), nodes.JsonToPython(), nodes.Log(), nodes.Add1(), nodes.Add1(), nodes.Log(), nodes.PythonToJson())

c2 = channels.TimeChannel(cron='*/10 * * * *')

c2.add(nodes.Log(), nodes.Log())