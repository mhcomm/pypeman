#!/usr/bin/env python

import asyncio
import sys
import os

import datetime

from autobahn.asyncio.wamp import ApplicationSession
from autobahn.asyncio.websocket import WampWebSocketClientFactory
from autobahn.wamp.types import ComponentConfig

from asyncio import coroutine

from pypeman.events import channel_change_state
from pypeman.channels import all as all_channels
from pypeman.message import Message

import logging

logger = logging.getLogger("pypeman.store")

# TODO : Find a way to import this from pypeman directly (channels or whatever)
state_mapper = ['STARTING', 'WAITING', 'PROCESSING', 'STOPPING', 'STOPPED']

WAMP_SRV_URL, WAMP_REALM = u"ws://localhost:8080/ws", "realm1"
host, port = 'localhost', 8080

class PypemanSession(ApplicationSession):
    @asyncio.coroutine
    def onJoin(self, details):
        def list_channels():
            channels = [dict(name=c.name, state=state_mapper[c._status]) for c in all_channels]
            logger.debug("List channels requested")
            return channels
        
        def start_channel(channel_name):
            try:
                logger.debug("Start channel %s requested", channel_name)
                chan = [chan for chan in all_channels if channel_name == chan.name][0]
                chan_prev_status = chan.status
                yield from chan.start()
                resp = "%s successfully started (status from %r to %r)" % (chan.name, chan_prev_status, chan.status)
            except Exception as exc:
                resp = str(exc)
            return resp
        
        def message_store(channel_name):
            try:
                logger.debug("Message store requested for channel %s", channel_name)
                chan = [chan for chan in all_channels if channel_name == chan.name][0]
                msg_store = chan.message_store
                
                msgs = [msg["message"].payload for msg in msg_store.search()] 
                print(msg_store)
                print(msg_store.search())
                print(msgs)
                
                logger.info("message store channel requested: returning %r", msgs)
                return msgs
            except Exception as exc:
                return str(exc)
                
        def stop_channel(channel_name):
            try:
                logger.debug("Stop channel %s requested", channel_name)
                chan = [chan for chan in all_channels if channel_name == chan.name][0]
                chan_prev_status = chan.status
                yield from chan.stop()
                resp = "%s successfully stopped (status from %r to %r)" % (chan.name, chan_prev_status, chan.status)
            except Exception as exc:
                resp = str(exc)
            return resp
            
        def change_channel_state(name, action):
            chan = [chan for chan in all_channels if name == chan.name][0]
            if action.lower() == "stop":
                yield from chan.stop()
            elif action.lower() == "start":
                yield from chan.start()
             
            resp = "Received %r on channel %r." % (action, name)
            return resp

        @channel_change_state.receiver
        def publish_state_change(channel, old_state, new_state):
            msg = "channel %s has changed state from %r to %r" % (channel, old_state, new_state) 
            # info = dict(channel=channel, state=new_state)
            data = dict(channel=channel.name, state=state_mapper[new_state], old_state=state_mapper[old_state])
            self.publish(u"pypeman.state_change", data)
          
        yield from self.register(stop_channel, u'pypeman.stop_channel')
        yield from self.register(change_channel_state, u'pypeman.change_channel_state')
        yield from self.register(list_channels, u'pypeman.list_channels')
        yield from self.register(message_store, u'pypeman.message_store')

def start_client(loop=None, wamp_srv_url=WAMP_SRV_URL, realm=WAMP_REALM):
    if loop is None:
        loop = asyncio.get_event_loop()

    def create(): 
        try:
            cfg = ComponentConfig(realm) 
            session = PypemanSession(cfg)
        except Exception as exc:
            logger.exception("WAMP session could not be created", str(exc))
            loop.stop()
        else:
            print("WAMP session successfully created on %s:%d" % (host, port))
            return session
    
    transport_factory = WampWebSocketClientFactory(create, url=wamp_srv_url)
    coro = loop.create_connection(transport_factory, host, port, ssl=None)
    loop.run_until_complete(coro)
