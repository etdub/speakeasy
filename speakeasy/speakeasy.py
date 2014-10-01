import collections
import copy
import logging
import os
import Queue
import select
import socket
import sys
import threading
import time
import ujson
import zmq

import utils

logger = logging.getLogger(__name__)


class Speakeasy(object):
    def __init__(self, host, metric_socket, cmd_port, pub_port, emitter_name, emitter_args=None,
            emission_interval=60, legacy=None, hwm=20000):
        """ Aggregate metrics and emit. Also support live data querying. """
        self.metric_socket = metric_socket
        self.pub_port = pub_port
        self.cmd_port = cmd_port
        self.emitter_name = emitter_name
        self.emission_interval = emission_interval
        self.hostname = host
        self.legacy = legacy
        self.percentiles = [0.5, 0.75, 0.95, 0.99]
        self.metrics_queue = Queue.Queue()
        self.metrics_lock = threading.RLock()

        # Setup legacy socket if needed
        self.legacy_socket = None
        if self.legacy:
            if os.path.exists(self.legacy):
                logger.warn('Remove existing legacy socket and recreating'.format(self.legacy))
                os.remove(self.legacy)
            self.legacy_socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
            self.legacy_socket.bind(self.legacy)

        # Process the args for emitter
        self.emitter_args = {}
        if emitter_args:
                for arg in emitter_args:
                    k, v = arg.split('=')
                    self.emitter_args[k] = v

        # Setup the emitter
        self.emitter = import_emitter(self.emitter_name, **self.emitter_args)
        if not self.emitter:
            logger.warn("No emitter found")

        self.context = zmq.Context()

        # Listen for metrics
        self.recv_socket = self.context.socket(zmq.PULL)
        # Increase the HWM
        self.recv_socket.set_hwm(hwm)
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
        self.process_thread = threading.Thread(target=self.process_metrics_queue, args=())

        # Init metrics
        # Index metrics by appname
        self.metrics = {}

    def process_metrics_queue(self):
        logger.info("Start processing metrics queue")
        while self.running:
            try:
                metric, legacy = self.metrics_queue.get(block=False)
            except Queue.Empty:
                time.sleep(0.01)
                continue

            try:
              self.process_metric(metric, legacy=legacy)
            except Exception as e:
              logger.warn("Failed to process metric: {0}".format(e))

            self.metrics_queue.task_done()

    def process_metric(self, metric, legacy=False):
        """ Process metrics and store and publish """
        logger.debug("Received metric: {0}".format(metric))
        if legacy:
            # Legacy format for metrics is slightly different...
            # Index them under same "app name"
            app_name = '__LEGACY__'
            metric_name, value, metric_type = metric.split('|')
        else:
            app_name, metric_name, metric_type, value = metric

        try:
            value = float(value)
        except ValueError:
            logger.warn("Failed to cast metric value to float - {0}".format(metric))
            return

        if app_name not in self.metrics:
            self.init_app_metrics(app_name)

        pub_metrics = []
        if metric_type == 'GAUGE':

            with self.metrics_lock:
                self.metrics[app_name][metric_type][metric_name].append(value)
            # Publish the current running average
            pub_val = sum(self.metrics[app_name][metric_type][metric_name])/len(self.metrics[app_name][metric_type][metric_name])
            pub_metrics.append((self.hostname, app_name, metric_name, metric_type, pub_val, time.time()))

        elif metric_type == 'PERCENTILE' or metric_type == 'HISTOGRAM':
            # Kill off the HISTOGRAM type!!
            metric_type = 'PERCENTILE'
            with self.metrics_lock:
                self.metrics[app_name][metric_type][metric_name].append(value)
            # Publish the current running percentiles
            for p in self.percentiles:
                pub_metrics.append((self.hostname, app_name, '{0}{1}_percentile'.format(metric_name, int(p*100)), 'GAUGE', utils.percentile(self.metrics[app_name][metric_type][metric_name], p), time.time()))
            dp_len = len(self.metrics[app_name][metric_type][metric_name])
            if dp_len > 0:
              avg = sum(self.metrics[app_name][metric_type][metric_name])/dp_len
              pub_metrics.append((self.hostname, app_name, '{0}average'.format(metric_name), metric_type, avg, time.time()))

        elif metric_type == 'COUNTER':
            with self.metrics_lock:
                self.metrics[app_name][metric_type][metric_name] += value
            pub_val = self.metrics[app_name][metric_type][metric_name]
            # Publish the running count
            pub_metrics.append((self.hostname, app_name, metric_name, metric_type, pub_val, time.time()))

        else:
            logger.warn("Unrecognized metric type - {0}".format(metric))
            return

        msg = ujson.dumps(pub_metrics)
        self.pub_socket.send(msg)

    def process_command(self, cmd):
        """ Process command and reply """
        # TODO: Do something here
        pass

    def poll_sockets(self):
        """ Poll metrics socket and cmd socket for data """
        logger.info("Start polling")
        while self.running:
            socks = dict(self.poller.poll(1000))
            if self.recv_socket in socks and socks[self.recv_socket] == zmq.POLLIN:
                try:
                    metric = ujson.loads(self.recv_socket.recv())
                    # Put metric on metrics queue
                    self.metrics_queue.put((metric, False))
                except ValueError as e:
                    logger.warn("Error receving metric: {0}".format(e))

            if self.cmd_socket in socks and socks[self.cmd_socket] == zmq.POLLIN:
                cmd = ujson.loads(self.cmd_socket.recv())
                # Process command
                self.process_command(cmd)

            if self.legacy_socket:
                # Process legacy format
                try:
                    r, w, x = select.select([self.legacy_socket], [], [], 1)
                    if r:
                        data, addr = self.legacy_socket.recvfrom(8192)
                        self.metrics_queue.put((data, True))
                except socket.error, e:
                    logger.error('Error on legacy socket - {0}'.format(e))

        logger.info("Stop polling")

    def emit_metrics(self):
        """ Send snapshot of metrics through emitter """
        while self.running:
            logger.info("Emit metrics")

            # Grab "this is what the world looks like now" snapshot
            metrics_ss = self.snapshot()

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

        logger.info("Stop emitting")

    def snapshot(self):
        """
        Return a snapshot of current metrics

        [(app, metric, val, type, timestamp), ...]
        """
        metrics = []
        with self.metrics_lock:
            logger.debug("Inside of metrics lock")
            ss = copy.deepcopy(self.metrics)

        # Reset metrics
        self.reset_metrics()

        for app in ss:
            for m, val in ss[app]['COUNTER'].iteritems():
                metrics.append((app, m, val, 'COUNTER', time.time()))

            for m, vals in ss[app]['GAUGE'].iteritems():
                if len(vals) == 0:
                    logger.debug("No values for metric: {0}".format(m))
                    continue

                if vals:
                    metrics.append((app, m, sum(vals) / float(len(vals)), 'GAUGE', time.time()))

            for m, vals in ss[app]['PERCENTILE'].iteritems():
                if len(vals) == 0:
                    logger.debug("No values for metric: {0}".format(m))
                    continue

                # Emit 50%, 75%, 95%, 99% as GAUGE
                for p in self.percentiles:
                    # Assume the metric name has a trailing separator to append the percentile to
                    metrics.append((app, '{0}{1}_percentile'.format(m, int(p*100)), utils.percentile(vals, p), 'GAUGE', time.time()))
                metrics.append((app, '{0}average'.format(m), sum(vals) / float(len(vals)), 'GAUGE', time.time()))

        return metrics

    def reset_metrics(self):
        """ Reset metrics for next interval """
        for app in self.metrics:
            with self.metrics_lock:
                self.metrics[app]['GAUGE'] = collections.defaultdict(list)
                self.metrics[app]['PERCENTILE'] = collections.defaultdict(list)

    def init_app_metrics(self, app):
        """ Setup initial metric structure for new app """
        if app not in self.metrics:
            with self.metrics_lock:
                self.metrics[app] = {'GAUGE': collections.defaultdict(list), 'COUNTER': collections.defaultdict(int),
                        'PERCENTILE': collections.defaultdict(list)}

    def start(self):
        self.__start()

    def shutdown(self):
        self.__stop()

    def __start(self):
        self.running = True
        self.poll_thread.start()
        self.emit_thread.start()
        self.process_thread.start()

    def __stop(self):
        self.running = False
        logger.info("Shutting down")
        if self.poll_thread:
            logger.info("Waiting for poll thread to stop...")
            self.poll_thread.join()

        if self.emit_thread:
            logger.info("Waiting for emit thread to stop...")
            self.emit_thread.join()

        if self.process_thread:
            logger.info("Waiting for process thread to stop...")
            self.process_thread.join()

        self.__cleanup()

    def __cleanup(self):
        if self.legacy:
            if os.path.exists(self.legacy):
                logger.info('Cleaning up legacy socket')
                os.remove(self.legacy)


def import_emitter(name, **kwargs):
    namespace = 'speakeasy.emitter.'
    if namespace not in name:
        name = namespace + name

    try:
        emitter = __import__(name)
    except Exception, e:
        # app doesn't exist
        return

    module = sys.modules[name]

    return module.Emitter(**kwargs)

if __name__ == '__main__':
    server = Speakeasy('0.0.0.0', '/var/tmp/metric_socket', '5001', '5002',
                       'simple', ['filename=/var/tmp/metrics.out'], 60,
                       '/var/tmp/metric_socket2')
    server.start()
    while True:
        try:
            time.sleep(1)
        except:
            print "Exception... exiting"
            server.shutdown()
            break
