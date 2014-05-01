#!/usr/bin/python
'''Utility to launch several instances of mp3.py in different terminals based on info from a config file.'''
import config  # just import the config.py file explicitly
import os
import subprocess

procs = []
for ip, port in config.servers:
    if ip == '127.0.0.1':
        term = os.environ['TERM']
        binary = os.path.join(os.getcwd(), 'mp3.py')
        procs.append(
            subprocess.Popen([term, '-e', binary, '-p', str(port)]))
for p in procs:
    p.wait()
