# Here you can add any special system path for your project
# import sys
# sys.path.insert(your_path)
import logging

from pypeman import channels
from pypeman import nodes
# from pypeman import endpoints
from pypeman.tests.common import StoreNode

# from pypeman.conf import settings


pchan = channels.CronChannel(
        name="periodic",
        cron="* * * * * */2",
        )

pchan.add(
    nodes.Log(
        name="log1",
        level=logging.DEBUG,
        ),
    StoreNode(
        name="store",
        ),
    )
