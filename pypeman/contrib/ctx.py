"""
Additional Nodes for working with the context
"""

import pypeman.nodes as nodes

from pypeman.nodes import NodeException


class CombineCtx(nodes.BaseNode):
    """
    creates payload dict with combination of different contexts
    """

    def __init__(self, ctx_names, meta_from=None, flatten=False,  *args, **kwargs):
        """
            param ctx_names: list of context names to save or dict with mapping
                   of context name to payload keys
            param: flatten: if true all ctx payloads (must be dicts) will be combined
                    in one dict
            param: meta_from: specifies which meta the resulting message should have
        """
        super().__init__(*args, **kwargs)
        if not isinstance(ctx_names, dict):
            if flatten:
                ctx_dst = len(ctx_names) * [None]
            else:
                ctx_dst = ctx_names
            ctx_names = dict(zip(ctx_names, ctx_dst))
        self.ctx_names = ctx_names
        if len(ctx_names) < 2:
            raise NodeException("must have at least two contexts for combining")
        if meta_from is None:
            meta_from = next(iter(ctx_names.keys()))
        self.meta_from = meta_from

    def process(self, msg):
        payload = {}
        ch_logger = self.channel.logger
        ch_logger.debug(
            "combine ctx_names = %s / meta from %r",
            repr(self.ctx_names),
            self.meta_from,
            )
        # TODO: can we just copy the meta if the payload changed?
        # TODO: If not, then rmv meta_from param and create new meta
        msg.meta = msg.ctx[self.meta_from]['meta']

        for ctx_name, dst in self.ctx_names.items():
            ch_logger.debug("combine ctx = %s -> %s", ctx_name, dst)
            if dst is None:
                ctx_payload = msg.ctx[ctx_name]['payload']
                ch_logger.debug(
                    "upd payload with ctx payload of %s ( %s )",
                    ctx_name, repr(ctx_payload))
                payload.update(ctx_payload)
            else:
                payload[dst] = msg.ctx[ctx_name]['payload']

        msg.payload = payload

        return msg
