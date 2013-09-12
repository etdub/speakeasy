import collections
import threading
import time
import zmq

class Speakeasy(object):
  def __init__(self, metric_socket, cmd_port, pub_port, emitter_name, emission_interval):
    """ Aggregate metrics and emit. Also support live data querying. """

    self.metric_socket = metric_socket
    self.pub_port = pub_port
    self.cmd_port = cmd_port
    self.emitter_name = emitter_name
    self.emission_interval = emission_interval

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
    self.counters = collections.defaultdict(int)
    self.gauges = collections.defaultdict(list)

  def process_metric(self, metric):
    """ Process metrics and store and publish """
    app_name, metric_name, metric_type, value = metric.split('|')
    try:
      value = float(value)
    except ValueError:
      print "Bad metric"
      return

    full_metric = "{0}/{1}".format(app_name, metric_name)
    if metric_type == 'GAUGE':
      self.gauges[full_metric].append(value)
      pub_val = sum(self.gauges[full_metric])/len(self.gauges[full_metric])
    elif metric_type == 'COUNTER':
      self.counters[full_metric] += value
      pub_val = self.counters[full_metric]
    else:
      print "Bad metric type"
      return

    self.pub_socket.send("{0}|{1}|{2}|{3}|{4}".format(app_name, metric_name, metric_type, pub_val, time.time()))

  def process_command(self, cmd):
    """ Process command and reply """
    pass

  def poll_sockets(self):
    """ Poll metrics socket and cmd socket for data """
    print "Start polling"
    while self.running:
      socks = dict(self.poller.poll(1000))
      if self.recv_socket in socks and socks[self.recv_socket] == zmq.POLLIN:
        metric = self.recv_socket.recv()
        # Process metric
        self.process_metric(metric)

      if self.cmd_socket in socks and socks[self.cmd_socket] == zmq.POLLIN:
        cmd = self.cmd_socket.recv()
        # Process command
        self.process_command(cmd)

    print "Stop polling"

  def emit_metrics(self):
    """ Emit metrics on emission interval """
    #time.sleep(self.emission_interval)
    while self.running:
      print "Emit metrics"
      e_start = time.time()
      #self.emitter.emit(self.metrics)
      pass
      e_end = time.time()

      if (e_end - e_start) < self.emission_interval:
        time.sleep(self.emission_interval - (e_end - e_start))
    print "Stop emitting"

  def start(self):
    self.__start()

  def shutdown(self):
    self.__stop()

  def __start(self):
    self.running = True
    self.poll_thread.start()
    if self.emitter_name:
      self.emit_thread.start()

  def __stop(self):
    self.running = False
    print "Shutting down"
    if self.poll_thread:
      print "Waiting for poll thread to stop..."
      self.poll_thread.join()

    if self.emit_thread and self.emitter_name:
      print "Waiting for emit thread to stop..."
      self.emit_thread.join()

if __name__ == '__main__':
  server = Speakeasy('/var/tmp/metrics_socket', '5000', '50001', None, 5)
  server.start()
  while True:
    try:
      time.sleep(1)
    except:
      print "Exception... exiting"
      server.shutdown()
      break
