#!/usr/bin/env python

import argparse
from speakeasy import Speakeasy
import time


def main():
  parser = argparse.ArgumentParser(description='Run Speakeasy server')
  parser.add_argument('-ms', '--metric-socket', required=True, help='Metric socket to listen on')
  parser.add_argument('-cp', '--cmd-port', required=True, help='Port to receive commands on')
  parser.add_argument('-pp', '--pub-port', required=True, help='Port to publish updates on')
  parser.add_argument('-e', '--emitter', required=False, help='Module to emit data through periodically')
  parser.add_argument('-ea', '--emitter-args', required=False, nargs='*', help='Module to emit data through periodically')
  parser.add_argument('-ei', '--emission-interval', required=False, type=float, help='Frequency to emit data (seconds)')

  args = parser.parse_args()

  server = Speakeasy(args.metric_socket, args.cmd_port, args.pub_port, args.emitter, args.emitter_args, args.emission_interval)
  server.start()

  while True:
    try:
      time.sleep(1)
    except (KeyboardInterrupt, Exception) as e:
      print "Exception... exiting - {0}".format(e)
      server.shutdown()
      break

if __name__=='__main__':
  main()
