#!/usr/bin/python
import argparse
import json
import cmd
import shlex

from server import Server


def check_argc(args, *counts):
    plural = 'arguments'
    if len(counts) == 1 and counts[0] == 1:
        plural = 'argument'
    if len(args) not in counts:
        count_string = ' or '.join(str(count) for count in counts)
        print "*** expecting {} {}: got {}".format(
            count_string, plural, len(args))
        return False
    return True


def get_level(args, index, default='one'):
    try:
        level = args[index]
    except IndexError:
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

    def do_delete(self, s):
        'delete <key>'
        args = shlex.split(s)
        if not check_argc(args, 1):
            return
        key = args[0]
        result = self.server.delete(key)
        if not result:
            print "*** no such key: '{}'".format(key)

    def do_get(self, s):
        'get <key> [level]'
        args = shlex.split(s)
        if not check_argc(args, 1, 2):
            return
        key = args[0]
        level = get_level(args, 1)
        if not level:
            return

        result = self.server.get(key, level)
        if result:
            print result
        else:
            print "*** no such key '{}'".format(key)

    def do_insert(self, s):
        'insert <key> <value> [level]'
        args = shlex.split(s)
        if not check_argc(args, 2, 3):
            return
        key, value = args[0:2]
        level = get_level(args, 2)
        if not level:
            return

        if not self.server.insert(key, value, level):
            print ("*** key '{0}' already exists: use `update '{0}' '{1}'`"
                   " to change it").format(key, value)

    def do_update(self, s):
        'update <key> <value> [level]'
        args = shlex.split(s)
        if not check_argc(args, 2, 3):
            return
        key, value = args[0:2]
        level = get_level(args, 2)
        if not level:
            return

        if not self.server.update(key, value, level):
            print ("*** no such key '{0}': use `insert '{0}' '{1}'` to add"
                   " it").format(key, value)

    def do_showall(self, s):
        '''showall
        show all stored keys'''
        args = shlex.split(s)
        if not check_argc(args, 0):
            return

        print "TODO: show all keys"

    def do_search(self, s):
        'search <key>'
        args = shlex.split(s)
        if not check_argc(args, 1):
            return
        key = args[0]

        print "TODO: show info about key '{}'".format(key)

    def do_exit(self, s):
        'exit the interpreter'
        return True

    def do_iden(self, s):
        'current process\'s identifier (among its peers)'
        args = shlex.split(s)
        if not check_argc(args, 0):
            return

        print self.server.me


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('config_file', type=file, nargs='?', default=0)
    parser.add_argument('--port', type=int, required=True)

    args = parser.parse_args()
    address = ('127.0.0.1', args.port)

    config = {'servers': [address]}

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

    print config

    server = Server(address, config['servers'])
    server.start()

    Cmd(server).cmdloop()

if __name__ == '__main__':
    main()
