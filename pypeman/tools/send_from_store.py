import argparse

from functools import partial
from pathlib import Path

import requests

from pypeman.tools.view_store import Filter
from pypeman.tools.view_store import process_file_store


def http_send(url, store_id, msg):
    """
    sends message to a given url
    """
    print(f"send {store_id} to {url}")
    print(f"msg = {msg}")
    payload = msg.payload
    ses = requests.session()
    ses.post(url, json=payload)


def get_send_function(dst_str):
    """
    determines function, that will send a message to a target
    """
    if dst_str.startswith("http"):
        return partial(http_send, dst_str)

    raise Exception(f"can't determine send function for {dst_str}")


def mk_parser():
    """
    create argument parser for send_from_store command
    """
    descr = "send store contents to an endpoint"
    parser = argparse.ArgumentParser(descr)
    parser.add_argument("path", help="path to search messages")
    parser.add_argument(
        "--type", "-t",
        default="json",
        help="payload type (dflt = %(default)s)",
    )
    parser.add_argument(
        "--filter", "-f",
        help="filter (dot_separated_names=value)",
        action="append",
    )
    parser.add_argument(
        "--destination", "-d",
        help="destination 'url' to send messages to",
        default="http://localhost:8000",
    )
    return parser


def main():
    options = mk_parser().parse_args()
    print(f"options {options}")
    path = Path(options.path)
    filters = options.filter or []
    filters = [Filter(filter_str) for filter_str in filters]
    send_func = get_send_function(options.destination)
    process_file_store(path, pl_type=options.type, filters=filters, action=send_func)


if __name__ == "__main__":
    main()
