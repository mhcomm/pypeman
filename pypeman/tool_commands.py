"""
dispatcher command for pypeman tools
"""
import argparse
import importlib
import sys

from functools import partial


commands = [
    ("view_store", "read and search in file a pypeman store"),
    ("send_from_store", "send entry from store"),
]


def list_commands():
    """
    list command and it's short description
    """
    print("List of commands:")
    for cmd, descr in commands:
        print(f"{cmd}: {descr}")


def imp_mod_and_run(cmd, args):
    """
    import module and run its main func
    """
    mod = importlib.import_module(f"pypeman.tools.{cmd}")
    sys.argv[1:] = args
    mod.main()


def mk_parser():
    """
    create argument parser
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-l", "--list-commands", help="list commands with small description", action="store_true")
    subparsers = parser.add_subparsers(dest="command", help="sub command to call")
    for cmd, descr in commands:
        sub_prs = subparsers.add_parser(cmd, description=descr, add_help=False)
        sub_prs.set_defaults(func=partial(imp_mod_and_run, cmd))
    return parser


def main():
    parser = mk_parser()
    options, args = parser.parse_known_args()
    if options.list_commands:
        list_commands()
        return
    if not options.command:
        parser.print_help()
        return
    options.func(args)


if __name__ == "__main__":
    main()
