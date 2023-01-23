import os
import sys
import time
import tempfile
import traceback

import asyncio


async def check_for_newerfile(future, lockfile, interval):

    def mtime(p):
        return os.stat(p).st_mtime

    exists = os.path.exists
    files = dict()

    for module in list(sys.modules.values()):
        path = getattr(module, '__file__', '')
        if path[-4:] in ('.pyo', '.pyc'):
            path = path[:-1]
        if path and exists(path):
            files[path] = mtime(path)

    async def reccur():
        status = None
        await asyncio.sleep(interval)

        if not exists(lockfile) or mtime(lockfile) < time.time() - interval - 5:
            status = 'error'

        for path, lmtime in list(files.items()):
            if not exists(path) or mtime(path) > lmtime:
                status = 'reload'
                print('Pending reload...')
                break

        if status:
            future.set_result(status)
        else:
            asyncio.create_task(reccur())

    asyncio.create_task(reccur())


def reloader_opt(to_call, reloader, interval):
    if reloader and not os.environ.get('PROCESS_CHILD'):
        import subprocess
        lockfile = None
        try:
            fd, lockfile = tempfile.mkstemp(prefix='process.', suffix='.lock')
            os.close(fd)  # We only need this file to exist. We never write to it.
            while os.path.exists(lockfile):
                args = [sys.executable] + sys.argv
                environ = os.environ.copy()
                environ['PROCESS_CHILD'] = 'true'
                environ['PROCESS_LOCKFILE'] = lockfile
                p = subprocess.Popen(args, env=environ)
                while p.poll() is None:  # Busy wait...
                    os.utime(lockfile, None)  # I am alive!
                    time.sleep(interval)
                if p.poll() != 3:
                    if os.path.exists(lockfile):
                        os.unlink(lockfile)
                    sys.exit(p.poll())
        except KeyboardInterrupt:
            pass
        finally:
            if os.path.exists(lockfile):
                os.unlink(lockfile)
        return

    try:
        if reloader:
            lockfile = os.environ.get('PROCESS_LOCKFILE')

            future = asyncio.Future()
            asyncio.create_task(check_for_newerfile(future, lockfile, interval))

            def done(future):
                # Stop event loop
                loop = asyncio.get_event_loop()
                if loop.is_running() and future.result() != 'error':
                    loop.stop()

            future.add_done_callback(done)

            to_call()

            if future.done() and future.result() == 'reload':
                sys.exit(3)

        else:
            to_call()

    except KeyboardInterrupt:
        pass
    except (SystemExit, MemoryError):
        raise
    except Exception:
        if not reloader:
            raise
        traceback.print_exc()
        time.sleep(interval)
        sys.exit(3)
