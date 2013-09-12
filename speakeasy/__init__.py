import collections
import copy
import socket
import sys
import threading
import time
import ujson
import zmq

class Speakeasy(object):
  def __init__(self, metric_socket, cmd_port, pub_port, emitter_name, emitter_args, emission_interval):
    """ Aggregate metrics and emit. Also support live data querying. """

    self.metric_socket = metric_socket
    self.pub_port = pub_port
    self.cmd_port = cmd_port
    self.emitter_name = emitter_name
    self.emission_interval = emission_interval
    self.hostname = socket.getfqdn()

    # Process the args for emitter
    self.emitter_args = {}
    for arg in emitter_args:
      k,v = arg.split('=')
      self.emitter_args[k] = v

    # Setup the emitter
    self.emitter = import_emitter(self.emitter_name, **self.emitter_args)
    if not self.emitter:
      print "No emitter found"

    self.context = zmq.Context()

    # Listen for metrics
    self.recv_socket = self.context.socket(zmq.PULL)
    self.recv_socket.bind('ipc://{0}'.format(self.metric_socket))

    # Listen for commands
    self.cmd_socket = self.context.socket(zmq.REP)
    self.cmd_socket.bind('tcp://*:{0}'.format(self.cmd_port))

    # Publish metrics
    self.pub_socket = self.context.socket(zmq.PUB)
    self.pub_socket.bind('tcp://*:{0}'.format(self.pub_port))

    # Register sockets for polling
    self.poller = zmq.Poller()
    self.poller.register(self.recv_socket, zmq.POLLIN)
    self.poller.register(self.cmd_socket, zmq.POLLIN)

    # Setup poll and emit thread
    self.poll_thread = threading.Thread(target=self.poll_sockets, args=())
    self.emit_thread = threading.Thread(target=self.emit_metrics, args=())

    # Init metrics
    self.metrics = {}
    self.metrics['COUNTER'] = collections.defaultdict(int)
    self.metrics['GAUGE'] = collections.defaultdict(list)

  def process_metric(self, metric):
    """ Process metrics and store and publish """
    print "Received metric: {0}".format(metric)
    app_name, metric_name, metric_type, value = metric
    try:
      value = float(value)
    except ValueError:
      print "Bad metric"
      return

    full_metric = "{0}/{1}".format(app_name, metric_name)
    if metric_type == 'GAUGE':
      self.metrics['GAUGE'][full_metric].append(value)
      pub_val = sum(self.metrics['GAUGE'][full_metric])/len(self.metrics['GAUGE'][full_metric])
    elif metric_type == 'COUNTER':
      self.metrics['COUNTER'][full_metric] += value
      pub_val = self.metrics['COUNTER'][full_metric]
    else:
      print "Bad metric type"
      return

    msg = ujson.dumps([self.hostname, app_name, metric_name, metric_type, pub_val, time.time()])
    self.pub_socket.send(msg)

  def process_command(self, cmd):
    """ Process command and reply """
    pass

  def poll_sockets(self):
    """ Poll metrics socket and cmd socket for data """
    print "Start polling"
    while self.running:
      socks = dict(self.poller.poll(1000))
      if self.recv_socket in socks and socks[self.recv_socket] == zmq.POLLIN:
        metric = ujson.loads(self.recv_socket.recv())
        # Process metric
        self.process_metric(metric)

      if self.cmd_socket in socks and socks[self.cmd_socket] == zmq.POLLIN:
        cmd = ujson.loads(self.cmd_socket.recv())
        # Process command
        self.process_command(cmd)

    print "Stop polling"

  def emit_metrics(self):
    """ Emit metrics on emission interval """
    #time.sleep(self.emission_interval)
    while self.running:
      print "Emit metrics"

      # Grab "this is what the world looks like now" snapshot
      metrics_ss = self.snapshot()

      # Reset metrics
      self.reset_metrics()

      e_start = time.time()
      if self.emitter:
        self.emitter.emit(metrics_ss)
      e_end = time.time()

      # Sleep for 1 second interval until time to emit again
      if (e_end - e_start) < self.emission_interval:
        sleep_until = time.time() + (self.emission_interval - (e_end - e_start))
        while self.running:
          ct = time.time()
          if ct > sleep_until:
            break
          else:
            if sleep_until - ct < 1:
              time.sleep(sleep_until - ct)
            else:
              time.sleep(1)

    print "Stop emitting"

  def snapshot(self):
    """ Return a snapshot of current metrics """
    metrics = []
    ss = {}
    ss['COUNTER'] = copy.copy(self.metrics['COUNTER'])
    ss['GAUGE'] = copy.copy(self.metrics['GAUGE'])

    for m, val in ss['COUNTER'].iteritems():
      metrics.append((m, val, 'COUNTER', time.time()))

    for m, vals in ss['GAUGE'].iteritems():
      if vals:
        metrics.append((m, sum(vals) / float(len(vals)), 'GAUGE', time.time()))

    return metrics

  def reset_metrics(self):
    """ Reset metrics for next interval """
    self.metrics['GAUGE'] = collections.defaultdict(list)

  def start(self):
    self.__start()

  def shutdown(self):
    self.__stop()

  def __start(self):
    self.running = True
    self.poll_thread.start()
    self.emit_thread.start()

  def __stop(self):
    self.running = False
    print "Shutting down"
    if self.poll_thread:
      print "Waiting for poll thread to stop..."
      self.poll_thread.join()

    if self.emit_thread:
      print "Waiting for emit thread to stop..."
      self.emit_thread.join()

def import_emitter(name, **kwargs):
  namespace = 'speakeasy.emitter.'
  if namespace not in name:
    name = namespace + name

  print name

  try:
    emitter = __import__(name)
  except Exception, e:
    # app doesn't exist
    return

  module = sys.modules[name]

  return module.Emitter(**kwargs)

if __name__ == '__main__':
  server = Speakeasy('/var/tmp/metric_socket', '5001', '5002', 'simple', ['filename=/var/tmp/metrics.out'], 60)
  server.start()
  while True:
    try:
      time.sleep(1)
    except:
      print "Exception... exiting"
      server.shutdown()
      break
