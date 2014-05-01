#!/usr/bin/python
import argparse
import json
import cmd
import shlex
import inspect

from server import Server


def get_level(level, default='one'):
    if not level:
        return default
    if level == '1':
        return 'one'
    elif level == '9':
        return 'all'
    elif level in ('one', 'all'):
        return level
    else:
        print "*** expecting level: got {}".format(level)
        return None


def command(f):
    spec = inspect.getargspec(f)
    argc = len(spec.args) if spec.args else 0
    defaults = len(spec.defaults) if spec.defaults else 0
    min_args = argc - defaults - 1
    max_args = argc - 1

    def modified(self, line):
        args = shlex.split(line)
        if not (min_args <= len(args) <= max_args):
            if min_args == max_args:
                print "*** expecting {} arguments: got {}".format(min_args, len(args))
            else:
                print "*** epxecting between {} and {} arguments: got {}".format(min_args, max_args, len(args))
            return
        return f(self, *args)

    modified.__doc__ = f.__doc__
    return modified


class Cmd(cmd.Cmd):

    prompt = '>> '

    def __init__(self, server):
        cmd.Cmd.__init__(self)
        self.server = server

    def precmd(self, line):
        if line == 'EOF':
            return 'exit'
        elif line == 'show-all':
            return 'showall'
        else:
            return line

    def default(self, line):
        print "*** unrecognized command '{}'".format(line)

    @command
    def do_delete(self, key):
        'delete <key>'
        result = self.server.delete(key)
        if not result:
            print "*** no such key: '{}'".format(key)

    @command
    def do_get(self, key, level=None):
        'get <key> [level]'
        level = get_level(level)
        if not level:
            return

        result = self.server.get(key, level)
        if result:
            print result.value
        else:
            print "*** no such key '{}'".format(key)

    @command
    def do_insert(self, key, value, level=None):
        'insert <key> <value> [level]'
        level = get_level(level)
        if not level:
            return

        if not self.server.insert(key, value, level):
            print ("*** key '{0}' already exists: use `update '{0}' '{1}'`"
                   " to change it").format(key, value)

    @command
    def do_update(self, key, value, level=None):
        'update <key> <value> [level]'
        level = get_level(level)
        if not level:
            return

        if not self.server.update(key, value, level):
            print ("*** no such key '{0}': use `insert '{0}' '{1}'` to add"
                   " it").format(key, value)

    @command
    def do_showall(self):
        '''showall
        show all stored keys'''
        for key, value in self.server.items():
            print "'{}' -> '{}'".format(key, value)

    @command
    def do_search(self, key):
        'search <key>'
        print "Servers for '{}'".format(key)
        for ip, port in self.server.owners(key):
            print "{}:{}".format(ip, port)

    def do_exit(self, s):
        'exit the interpreter'
        return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('config_file', type=file, nargs='?', default=0)
    parser.add_argument('--port', type=int, required=True)

    args = parser.parse_args()
    address = ('127.0.0.1', args.port)

    config = {'servers': [address], 'delays': []}

    try:
        config.update(json.load(args.config_file))
        args.config_file.close()
    except AttributeError:
        pass
    config['servers'] = map(tuple, config['servers'])
    if address not in config['servers']:
        print address
        print config
        print "*** specified port not one of the ones in the config file"
        return
    server = Server(address, config['servers'], config['delays'])
    server.start()

    Cmd(server).cmdloop()

if __name__ == '__main__':
    main()
