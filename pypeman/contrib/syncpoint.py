#############################################################################
# RFC Syncpoint / Inter channel communication.
""" 

For our current project we have an issue, that could be solved with inter channel 
communication.

I'd like to propose two pypeman standard nodes, that could solve this problem.
Before implementing, I'd like to have feedback whether the suggested implementation 
makes sense.

pypeman may often be connecting old style synchronous web frame works like
a simple http_getter with django and celery tasks.


Scenario: an external application wants Pypeman wants perform a lasting operation
          on a synchronous web server and retrieve the result.

As we know, long lasting requests are a huge problem for synchronous web servers, 
that have a limited amount of workers / processes. 

Ext SW     Pypeman    syncronous
                      web framework
                      (e.g. Django)
  |  request  |             |
  |---------->|             |
  |           |------------>X
  |           |             X 
  |           |             X long lasting request 
  |           |             X
  |           |             X
  |           |             X
  | Response  |<------------X
  |<----------|             |


In order to deblock the web framework and allow it to handle other requests
these frameworks delegate often work intensive tasks to dedicated workers. 
An example is Django/celery. 
This allows the web server to acknowledge the request quickly and handly other 
HTTP requests.


#############################################################################
Ext SW     Pypeman    syncronous         worker trigger
  |           |       web framework      by webfw
  |           |       (e.g. Django)      (e.g. celery)
  |           |             |                 |
  |--req----->|             |                 |
  |           |             |                 |
  |           |---req------>X                 |
  |           |             |----trigger----->X
  |           |<--resp------|                 X
  |           |  early acknowledge            X
  |           |             |                 X
  |           |             |                 X
  |           |             |                 X long running task finished.
  |           |             |                 |  But how to notify pypeman???



The solution could be, that pypeman is holding back the response to the client 
until the worker (celery) task will notify pypeman

#############################################################################
Ext SW     Pypeman    syncronous         worker trigger
  |           |       web framework      by webfw
  |           |       (e.g. Django)      (e.g. celery)
  |           |             |                 |
  |--req----->|             |                 |
  |           |             |                 |
  |           |----req----->X                 |
  |           |             |----trigger----->X
  |           |<---resp-----|                 X
  |           |  early acknowledge            X
  |           |             |                 X
  |           |             |                 X
  |           |<---------notify---------------X long running task finished.
  |           |             |                 |
  |           |--req ------>|                 |  potential request to fetch
  |           |<--resp------|                 |  generated data
  |<----------|             |                 |


Suggested implementation:
Pypeman implements a pair of nodes.
One WaitNode, that will wait in a channel until a Notification Node from 
another channel unblocks it by sending it the id it's waiting for.


The solution could be, that pypeman is holding back the response to the client 
until the worker (celery) task will notify pypeman

#############################################################################
Ext SW     Pypeman    syncronous         worker trigger
  |           |       web framework      by webfw
  |           |       (e.g. Django)      (e.g. celery)
  |           |             |                 |
  |--req1---->|             |                 |
  |           |             |                 |
  |           |----req----->X                 |
  |           |             |---trigger(ID=1)>X
  |           |<-resp(ID=1)-|                 X
  |           w  early acknowledge            X Here the channel that handled
  |           w             |                 X req1 will wait to be notified by ID=1
  |           w             |                 X
  |           |<---------notify(ID=1)---------X long running task finished.
  |           |             |                 | the channel handling this request
  |           |             |                 | will Notify 
  |           |             |                 | 
  |           |--req ------>|                 |  potential request to fetch
  |           |<--resp------|                 |  generated data
  |<-resp1----|             |                 |



Potential Issues for implementation:
req1 and notify have to be handled in parallel, so perhaps we need to separate
socket end points?


Potential Issues:
-  pypeman channels might lock if a notification never arrives, especially channels, that do
    handle requests only in order.


Benefits:
- inter channel synchronisation via notify/wait allows pypeman to 
    - implement long polling features
    - to implement channels, that depend on asynchronous events.


Alternatives:
- there's push modules for nginx, but they need recompilation of nginx.
    ( e.g. https://nchan.io/ or https://github.com/wandenberg/nginx-push-stream-module)
   If using long polling mode, then no changes are required to pypeman.
    ( I can make a diagram if it is considered useful for that case)

"""


class WaitNode:
    def __init__(self, timeout=60):
        pass

    def wait_for_event(self, event_id):
        """ waits for an event, could be a uuid returned by the web server
        """


class NotifyNode:
    def __init__(self):
        pass

    def notify(self, event_id):
        """ sends notification for event_id, which will
            unblock all WaitNodes waiting for that event
        """
        
