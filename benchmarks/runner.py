#!/usr/bin/env python
"""'Benchmark' a thing.

This script basically performs the following steps:
    * clone the project to a work dir
    * checkout a commit/version
    * execute the script
    * call the script's `run` function

There is a special case when the commit is HEAD: it will use the
working tree files while still running the script from the given
working directory.

A banchmark script may be any python script:
    * it is loaded, executing any top-level statements;
    * if it has a `setup` member, it is called like a function;
    * then it's `run` member is called like and profiled/timed.

`run` is called with what `setup` returned, as if `run(*setup())`.
Both functions may be async.
"""

import argparse
import asyncio
import cProfile as profile
import io
import os
import subprocess as subp
import sys
import tempfile
import timeit


class Options:
    pypeman: str  # repo directory
    commit: str
    dir: str
    script: io.TextIOBase
    prout: str | None
    timeit: bool


def setup_and_run(opts: Options):
    # restore after this function: it should be transparent to caller
    oldpwd = os.getcwd()

    # ensure absolute before chdir
    if opts.prout:
        opts.prout = os.path.abspath(opts.prout)

    # will chdir into the cloned repo, so this ensure user script imports still behave as expected
    spath = getattr(opts.script, "name")
    sys.path.append(os.getcwd() if spath is None else os.path.dirname(os.path.abspath(spath)))

    if "HEAD" == opts.commit:
        # when using HEAD there is no cloning; this instead gives direct import access
        sys.path.append(opts.pypeman)
    else:
        # otherwise clone and checkout...
        subp.run(["git", "clone", "--shared", "--", opts.pypeman, opts.dir]).check_returncode()
        subp.run(["git", "-C", opts.dir, "checkout", "--detach", opts.commit]).check_returncode()
    os.chdir(opts.dir)

    # read/exec the script
    script = opts.script.read()
    opts.script.close()
    mod: ... = {
        "__name__": "<benchmark>" if spath is None else os.path.basename(spath).removesuffix(".py"),
        "__file__": "<benchmark>" if spath is None else spath,
        "__doc__": None,
    }
    exec(script, mod)

    # setup and run
    setup = mod.get("setup")
    mod["_args"] = ()
    if setup:
        mod["_args"] = asyncio.run(setup()) if asyncio.iscoroutinefunction(setup) else setup()
    if asyncio.iscoroutinefunction(mod["run"]):
        arun, mod["run"] = mod["run"], lambda *a: asyncio.run(arun(*a))
    if opts.timeit:
        print(f"run() took {timeit.timeit('run(*_args)', number=1, globals=mod)}s")
    else:
        profile.runctx("run(*_args)", globals=mod, locals=mod, filename=opts.prout)

    os.chdir(oldpwd)


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--pypeman", nargs="?", help="Path to a local clone of the repository.")
    parser.add_argument("commit", metavar="commit-ish", help="Use this git hash/tag/...")
    parser.add_argument(
        "dir",
        metavar="work-dir",
        nargs="?",
        help="""Use a named dir rather than a temp dir; some benchmarks may have a heavy setup
                and it may be preferable to keep and reuse between runs. If this is not provided,
                a temp throwaway dir is created for the duration of the benchmark.""",
    )
    parser.add_argument("script", type=argparse.FileType("r"), help="Benchmark script to use.")
    parser.add_argument("--prof-out", "-o", dest="prout", help="Profile output.")
    parser.add_argument(
        "--timeit",
        action="store_true",
        help="""Run through timeit instead. The '-o' argument is unused. Because benchmark scripts
                are expected to be potentially costly and already containing repetition, a single
                measurement is done.""",
    )
    opts = parser.parse_args(namespace=Options())

    if not opts.pypeman:
        # expects to be within the git repo; find the root path
        root = subp.run(["git", "rev-parse", "--show-toplevel"], stdout=subp.PIPE, text=True)
        assert root.stdout, (
            f"`git rev-parse` failed: {root.stderr!r}, called from {os.getcwd()!r}; "
            + "it is also possible to use --pypeman <repo-dir>"
        )
        opts.pypeman = root.stdout.strip()

    # use provided location
    if opts.dir:
        return setup_and_run(opts)

    # use temp location
    with tempfile.TemporaryDirectory() as dir:
        opts.dir = dir
        return setup_and_run(opts)


if "__main__" == __name__:
    main()
