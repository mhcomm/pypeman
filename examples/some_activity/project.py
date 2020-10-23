# Here you can add any special system path for your project
# import sys
# sys.path.insert(your_path)
import logging
import os

from pypeman import channels
from pypeman import nodes
# from pypeman import endpoints
from pypeman.tests.common import StoreNode

# from pypeman.conf import settings


pchan = channels.CronChannel(
        name="periodic",
        cron="* * * * * */10",
        # cron="* * * * * 0",
        )

pchan.add(
    nodes.Log(
        name="log1",
        level=logging.DEBUG,
        ),
    StoreNode(
        name="store1",
        ),
    )


watch_chan = channels.FileWatcherChannel(
    path=os.path.realpath("."),
    regex=r'.*\.txt$',
    name="watch_txt",
    )

watch_chan.add(
    nodes.Log(
        name="log2",
        level=logging.DEBUG,
        ),
    StoreNode(
        name="store2",
        ),
    )
