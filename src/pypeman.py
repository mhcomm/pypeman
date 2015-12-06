#!/usr/bin/env python

import asyncio
import channels
import endpoints

def main():
    print('start')
    # Reference the event loop.
    import project

    loop = asyncio.get_event_loop()

    for chan in channels.all:
        loop.run_until_complete(chan.start())

    for end in endpoints.all:
        loop.run_until_complete(end.start())


    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

if __name__ == '__main__':
    main()