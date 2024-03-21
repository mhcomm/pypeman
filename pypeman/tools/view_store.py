import argparse
import ast
import asyncio
import json

from pathlib import Path

from pypeman.msgstore import FileMessageStore


class Filter:
    """
    a simple filter to filter messages fetched from a storage
    """
    def __init__(self, filter_str):
        self.filter_str = filter_str
        name, val = filter_str.split("=", 1)
        self.name = name
        self.val = ast.literal_eval(val)

    def __str__(self):
        return self.filter_str

    def match(self, msg):
        return msg.payload.get(self.name) == self.val

    def __repr__(self):
        return str(self)


def split_path_to_store_n_id(path):
    """
    helper to split an absolute msg_store path to a store and its id-path
    """
    path_wo_date = path.parents[3]
    store_name = path_wo_date.name
    store_path = path_wo_date.parent
    # TODO: most refactor the stores to simplify access by uid or file name or bu
    # TODO: absolute file name
    store = FileMessageStore(str(store_path), store_name)
    rel_path_w_date = path.relative_to(path_wo_date)
    return store, rel_path_w_date


def print_msg(store_id, msg):
    """
    just print store id and msg payload
    """
    print(store_id, msg.payload)


def handle_store_entry(path, pl_type=None, filters=None, action=print):
    """
    handles a store entry and print if not rejected by filters
    """
    store, rel_path_w_date = split_path_to_store_n_id(path)
    entry = asyncio.run(store.get(str(rel_path_w_date)))
    msg = entry["message"]
    msg_id = entry["id"]
    payload = msg.payload
    if pl_type == "json":
        msg.payload = payload = json.loads(payload)

    match = True
    for filter in filters:
        if not filter.match(msg):
            match = False
            break
    if not match:
        return
    action(msg_id, msg)


def process_file_store(
        path, pl_type=None, filters=None, action=print_msg):
    """
    handles one or multiple file store messages
    must later see how to make code handle also other stores

    param: path: the store 'id' / path of the store entry
    param: filters: list of filters to filter message objects
    param: action: action to perform with entry
    """

    if path.is_file():
        handle_store_entry(
            path, pl_type=pl_type, filters=filters, action=action)

    all_entries = path.glob("**/*")
    f_paths = (
        path for path in all_entries
        if path.is_file() and not path.suffix
    )
    for entry in f_paths:
        handle_store_entry(
            entry, pl_type=pl_type, filters=filters, action=action)


def mk_parser():
    """
    argument parser for view_store command
    """
    descr = "view store contents or search"
    parser = argparse.ArgumentParser(descr)
    parser.add_argument("path", help="path to search messages")
    parser.add_argument(
        "--type", "-t",
        default="json",
        help="payload type (dflt = %(default)s)",
    )
    parser.add_argument(
        "--filter", "-f",
        help="filter (names=value) at the moment only for payload fields",
        action="append",
    )
    return parser


def main():
    options = mk_parser().parse_args()
    path = Path(options.path)
    filters = options.filter or []
    filters = [Filter(filter_str) for filter_str in filters]
    process_file_store(path, pl_type=options.type, filters=filters)


if __name__ == "__main__":
    main()
