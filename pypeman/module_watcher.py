#!/usr/bin/env python
# Author: Chris Eberle <eberle1080@gmail.com>
# Watch for any changes in a module or package, and reload it automatically

import pyinotify
import importlib
import os

class ModuleWatcher(pyinotify.ProcessEvent):
    """
    Automatically reload any modules or packages as they change
    """

    def __init__(self):
        "El constructor"

        self.wm = pyinotify.WatchManager()
        self.notifier = None
        self.mod_map = {}

    def _watch_file(self, file_name, module):
        "Add a watch for a specific file, and map said file to a module name"

        file_name = os.path.realpath(file_name)
        self.mod_map[file_name] = module
        self.wm.add_watch(file_name, pyinotify.EventsCodes.IN_MODIFY)
        #print 'Watching', file_name

    def watch_module(self, name):
        "Load a module, determine which files it uses, and watch them"

        if importlib.is_builtin(name) != 0:
            # Pretty pointless to watch built-in modules
            return

        (fd, pathname, description) = importlib.find_module(name)

        try:
            mod = importlib.load_module(name, fd, pathname, description)
            if fd:
                self._watch_file(fd.name, name)
            else:
                for root, dirs, files in os.walk(pathname):
                    for filename in files:
                        fpath = os.path.join(root, filename)
                        if fpath.endswith('.py'):
                            self._watch_file(fpath, name)
        finally:
            if fd:
                fd.close()

    def start_watching(self):
        "Start the pyinotify watch thread"

        if self.notifier is None:
            self.notifier = pyinotify.ThreadedNotifier(self.wm, self)
        self.notifier.start()

    def stop_watching(self):
        "Stop the pyinotify watch thread"

        if self.notifier is not None:
            self.notifier.stop()

    def process_IN_MODIFY(self, event):
        "A file of interest has changed"

        # Is it a file I know about?
        if event.path not in self.mod_map:
            return

        # Find out which module is using that file
        modname = self.mod_map[event.path]

        # Reload the module
        (fd, pathname, description) = importlib.find_module(modname)
        try:
            importlib.load_module(modname, fd, pathname, description)
        finally:
            if fd:
                fd.close()

        #print 'Reload', modname

if __name__ == '__main__':
    # Test everything

    import sys

    mw = ModuleWatcher()
    mw.watch_module('module1')
    mw.watch_module('module2')
    mw.start_watching()

    try:
        input('Press ENTER to exit')
    finally:
        mw.stop_watching()
        sys.exit(0)





'''class FileCheckerThread(threading.Thread):
    """ Interrupt main-thread as soon as a changed module file is detected,
        the lockfile gets deleted or gets to old. """

    def __init__(self, lockfile, interval, mainloop):
        threading.Thread.__init__(self)
        self.daemon = True
        self.lockfile, self.interval = lockfile, interval
        #: Is one of 'reload', 'error' or 'exit'
        self.status = None
        self.mainloop = mainloop

    def run(self):
        print(ident(), 'file watch thread start')
        exists = os.path.exists
        mtime = lambda p: os.stat(p).st_mtime
        files = dict()

        for module in list(sys.modules.values()):
            path = getattr(module, '__file__', '')
            if path[-4:] in ('.pyo', '.pyc'): path = path[:-1]
            if path and exists(path): files[path] = mtime(path)

        while not self.status:
            #print(ident(), 'file watch')
            #print(self.mainloop.is_running())
            if not exists(self.lockfile) or mtime(self.lockfile) < time.time() - self.interval - 5:
                self.status = 'error'
                thread.interrupt_main()
            for path, lmtime in list(files.items()):
                if 'pypema' in path:
                    pass #print(path)
                if not exists(path) or mtime(path) > lmtime:
                    self.status = 'reload'
                    print(ident(), 'need reload')
                    thread.interrupt_main()
                    break
            time.sleep(self.interval)
        print(ident(), 'end thread')

    def __enter__(self):
        self.start()

    def __exit__(self, exc_type, *_):
        if not self.status:
            self.status = 'exit'  # silent exit
        self.join()
        return exc_type is not None and issubclass(exc_type, KeyboardInterrupt)'''


'''print(ident(), "child")
        if reloader and False:
            lockfile = os.environ.get('PROCESS_LOCKFILE')
            bgcheck = FileCheckerThread(lockfile, interval, asyncio.get_event_loop())
            with bgcheck:
                print(ident(), "start call")
                to_call()
                print("end call")
            print (ident(), 'reload two')

            if bgcheck.status == 'reload':
                sys.exit(3)
        else:
            print('normal')
            to_call()'''