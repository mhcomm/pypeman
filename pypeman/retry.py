import asyncio
import logging
import os

from pathlib import Path

from pypeman import exceptions
from pypeman.msgstore import FileMessageStore

logger = logging.getLogger(__name__)


class RetryFileMsgStore(FileMessageStore):
    """
    A class that is attached to a channel and automatically replays messages that
    are stored in it after a given time.
    Nodes that catch specific exceptions add the message in this filestore, the
    retryfilestore will pause the attached channel and will retry
    to inject the message after a given time. If the message continue to fail, it will
    re-wait and re-try.
    If the message is successfully injected, it will be remove from the message store.
    When the RetryFileMsgStore is empty, it will un-pause the channel and stop himself
    """
    STOPPED = "STOPPED"
    RETRY_MODE = "RETRY_MODE"
    state = None

    def __init__(self, *args, path, channel, retry_delay=60, **kwargs):
        path = os.path.join(path, "retry_store")
        self.retry_delay = retry_delay  # delay in seconds between retries
        self.channel = channel
        self.state = self.STOPPED
        self.stop_flag = False
        self.retry_task = None
        self.exit_event = asyncio.Event()
        self.test_mode = False
        super().__init__(*args, path=path, **kwargs)

    def _reset_test(self):
        """
            If test mode is set to True, don't start the retry task,
            you must call retry method manually
        """
        self.test_mode = True

    async def start(self):
        await super().start()
        cnt_msgs = await self.count_msgs()
        if cnt_msgs > 0:
            await self.start_retry_mode()

    async def stop(self):
        if self.state != self.STOPPED:
            self.exit_event.set()
            if self.retry_task:
                await asyncio.gather(self.retry_task)

    async def store_until_retry(self, msg, nodename):
        """
        Store a message, and start the retryStore if it's not already started
        The message will be inject in the "nodename" node

        Args:
            msg (message.Message): Message to Retry later
            nodename (str|None): The nodename where to inject
        """
        logger.debug(f"Retrystore of {self.channel.short_name} Store msg {msg}")
        store_id = msg.store_id
        store_chan_name = msg.store_chan_name
        id = await self.store(msg=msg)
        await self.add_message_meta_infos(id=id, meta_info_name="nodename", info=nodename)
        await self.add_message_meta_infos(id=id, meta_info_name="store_id", info=store_id)
        await self.add_message_meta_infos(id=id, meta_info_name="store_chan_name", info=store_chan_name)
        if self.state != self.RETRY_MODE:
            await self.start_retry_mode()

    async def search_by_store_id(self, store_id, count=0):
        """Returns a list of <count> messages ids for a given store_id
        if count is 0, returns all messages for this id

        Args:
            store_id (str|None): The store_id to search in meta
            count (int): The number of maximum wanted results (if set to 0, unlimited)

        Returns:
            list of str: list of message ids
        """
        msg_ids_to_return = []
        for year in await self.sorted_list_directories(os.path.join(self.base_path)):
            for month in await self.sorted_list_directories(
                    os.path.join(self.base_path, year)):
                for day in await self.sorted_list_directories(
                        os.path.join(self.base_path, year, month)):
                    folder_path = Path(self.base_path) / year / month / day
                    for fpath in folder_path.glob("*.meta"):
                        msg_id = fpath.stem
                        msg_store_id = await self.get_message_meta_infos(
                            id=msg_id, meta_info_name="store_id")
                        if msg_store_id == store_id:
                            msg_ids_to_return.append(msg_id)
                        if count:
                            if len(msg_ids_to_return) == count:
                                return msg_ids_to_return
        return msg_ids_to_return

    async def retry_one_store_id(self, msg_store_id):
        """
        Launch retry of 1 Base message (as a base message could have been
        yielded into multiple sub messages, this function could run the retry of
        multiples sub-messages)

        Args:
            msg_store_id (str): The base message id to search in meta.store_id
        """
        if msg_store_id is not None:
            msg_ids = await self.search_by_store_id(store_id=msg_store_id)
        else:
            msg_ids = await self.search_by_store_id(store_id=msg_store_id, count=1)

        logger.debug(
            f"Retrystore of {self.channel.short_name} try to retry "
            f"store_id={msg_store_id} ({len(msg_ids)} messages)"
        )
        # TODO: at moment, the retry store doesn't handle order of yielded sub messages
        # as they don't provide their order
        catched_exc = None
        for idx, msg_id in enumerate(msg_ids):
            msg_data = await self.get(id=msg_id)
            nodename_where_inject = msg_data["meta"]["nodename"]
            msg_store_id = msg_data["meta"]["store_id"]
            msg_store_chan_name = msg_data["meta"]["store_chan_name"]
            chan = self.channel
            msg = msg_data["message"]
            msg.store_id = msg_store_id
            msg.store_chan_name = msg_store_chan_name
            await self.delete(id=msg_id)
            is_last_msg = idx == len(msg_ids) - 1
            # TODO: currently, inject doesn't work with endnodes as it don't get the worst
            # state to call correct nodes: I'll implement it in future weeks
            # It needs a little refactorisation of how "special" nodes are called
            call_endnodes = nodename_where_inject is None
            try:
                await chan.inject(
                    msg=msg,
                    start_nodename=nodename_where_inject,
                    call_endnodes=call_endnodes,
                    set_state=is_last_msg,
                )
            except exceptions.RetryException:
                raise
            except Exception as exc:
                catched_exc = exc
        if catched_exc is not None:
            raise catched_exc

    async def _set_base_msg_state(self, msg_data):
        """
        Change the state of the base message of a sub message dict

        Args:
            msg_data (dict): _description_
        """
        from pypeman.channels import get_channel
        msg_store_chan_name = msg_data["meta"]["store_chan_name"]
        msg_store_id = msg_data["meta"]["store_id"]
        if not (msg_store_chan_name or msg_store_chan_name):
            return
        if msg_store_chan_name != self.channel.short_name:
            msg_store = get_channel(msg_store_chan_name).message_store
        else:
            msg_store = self.channel.message_store

        if msg_store and msg_store_id:
            await msg_store.set_state_to_worst_sub_state(msg_store_id)

    async def retry(self):
        """
        Launch retry for all messages until all messages are processed or until the
        first RetryException catch
        """
        from pypeman.channels import BaseChannel
        logger.debug(f"Retrystore of {self.channel.short_name} try to retry")
        while self.state == self.RETRY_MODE and not self.exit_event.is_set():
            async with self.channel.lock:
                msgs_data = await self.search(count=1, order_by="timestamp")
                if len(msgs_data) == 0:
                    logger.debug("No more messages to reply, chan will be un-paused")
                    self.channel.status = BaseChannel.WAITING
                    self.state = self.STOPPED
                    break
                else:
                    msg_data = msgs_data[0]
                    message_store_id = msg_data["meta"]["store_id"]
            retry_exc_catched = False
            try:
                await self.retry_one_store_id(msg_store_id=message_store_id)
                logger.debug(
                    f"Retrystore Retry {self.channel.short_name}: Retry of "
                    f"store_id={ message_store_id } Done"
                )
                continue
            except exceptions.RetryException:
                logger.debug(
                    f"Retrystore Retry {self.channel.short_name}: Retry of "
                    f"store_id={ message_store_id } not good: RetryExc catched,"
                    " will retry later"
                )
                retry_exc_catched = True
                return
            except Exception:
                logger.debug(
                    f"Retrystore Retry {self.channel.short_name}: Retry of "
                    f"store_id={ message_store_id } Done (with err)"
                )
                continue
            finally:
                if not retry_exc_catched:
                    await self._set_base_msg_state(msg_data)

    async def start_retry_mode(self):
        """
        Start the Retr loop and Pause the channel
        """
        from pypeman.channels import BaseChannel
        logger.debug(f"Retrystore of {self.channel.short_name} start retry mode")
        self.state = self.RETRY_MODE
        if self.channel.status != BaseChannel.PAUSED:
            self.channel.status = BaseChannel.PAUSED
        if not self.test_mode:
            self.retry_task = asyncio.create_task(self.wait_retries())

    async def wait_retries(self):
        """The infinite retry loop
        """
        while self.state == self.RETRY_MODE and not self.exit_event.is_set():
            await self.retry()
            try:
                await asyncio.wait_for(self.exit_event.wait(), timeout=self.retry_delay)
            except asyncio.TimeoutError:
                pass
