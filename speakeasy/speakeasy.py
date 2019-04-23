from __future__ import absolute_import
import collections
import copy
import logging
import os
import six.moves.queue
import socket
import sys
import threading
import time
import ujson
import bisect
import zmq
import resource
from . import utils
import signal
import six

logger = logging.getLogger(__name__)

# Unused
MAX_RECEIVE_HWM = 20000

# How often should we poll the socket for metrics
POLLING_TIMEOUT_MS = 500

# How long should we wait on metric queue to events. Affects responsiveness to server shutdown
QUEUE_WAIT_SECS = 5


# After these many loops of POLLING_TIMEOUT_MS, metric processed counts
# will be updated in stats
METRIC_CHECKPOINT_INTERVAL = 20

# max rss before dying
MAX_RSS = 2000000000  # 2GB


def _gauge_pair():
    """ Track gauge as a pair of (sum, count) """
    return [0, 0]


class Speakeasy(object):
    "A Speakeasy instance"

    def __init__(self, host, metric_socket, cmd_port, pub_port, emitter_name,
                 emitter_args=None, emission_interval=60, legacy=None,
                 hwm=MAX_RECEIVE_HWM, socket_mod=None):
        """ Aggregate metrics and emit. Also support live data querying. """
        self.metric_socket = metric_socket
        self.pub_port = pub_port
        self.cmd_port = cmd_port
        self.emitter_name = emitter_name
        self.emission_interval = emission_interval
        self.hostname = host
        self.legacy = legacy
        self.percentiles = [0.5, 0.75, 0.95, 0.99]
        self.metrics_queue = six.moves.queue.Queue()
        self.metrics_lock = threading.RLock()
        self.stop = threading.Event()

        # Setup legacy socket if needed
        # If socket file name exists, will delete and  recreate it
        self.legacy_socket = None
        if self.legacy:
            if os.path.exists(self.legacy):
                logger.warn('Remove existing legacy socket "{0}" and recreating'.format(self.legacy))
                os.remove(self.legacy)
            self.legacy_socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
            try:
                self.legacy_socket.bind(self.legacy)
            except OSError as e:
                logger.fatal("Error changing perms: %s", e)
            self.legacy_socket_fno = self.legacy_socket.fileno()
            logger.info("Will read legacy input from %s", self.legacy)

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

        logger.info("Using emitter '%s' with args %s", self.emitter_name, self.emitter_args)

        self.context = zmq.Context()

        # Listen for metrics
        self.recv_socket = self.context.socket(zmq.PULL)

        # Increase the HWM
        # NOTE: According to http://api.zeromq.org/2-1:zmq-socket, this has no effect on a PULL socket
        self.recv_socket.set_hwm(hwm)
        try:
            self.recv_socket.bind('ipc://{0}'.format(self.metric_socket))
            if socket_mod:
                os.chmod(self.metric_socket, socket_mod)
            logger.info("Will receive client metrics from %s", self.metric_socket)
        except zmq.error.ZMQError as e:
            logger.fatal("Could not bind receive socket: %s", e)
        except OSError as e:
            logger.fatal("Error changing perms: %s", e)

        # Listen for commands
        self.cmd_socket = self.context.socket(zmq.REP)
        try:
            self.cmd_socket.bind('tcp://*:{0}'.format(self.cmd_port))
            logger.info("Will receive commands from *:%s", self.cmd_port)
        except zmq.error.ZMQError as e:
            logger.fatal("Could not bind cmd socket: %s", e)

        # Publish metrics
        if self.pub_port:
            self.pub_socket = self.context.socket(zmq.PUB)
            try:
                self.pub_socket.bind('tcp://*:{0}'.format(self.pub_port))
                logger.info("Will publish events to PUB socket at *:%s", self.pub_port)
            except zmq.error.ZMQError as e:
                logger.fatal("Could not bind pub socket: %s", e)

        # Register sockets for polling
        self.poller = zmq.Poller()
        self.poller.register(self.recv_socket, zmq.POLLIN)
        self.poller.register(self.cmd_socket, zmq.POLLIN)
        if self.legacy_socket:
            self.poller.register(self.legacy_socket, zmq.POLLIN)

        # Setup poll and emit thread
        self.poll_thread = threading.Thread(target=self.poll_sockets, args=(), name="poll_input")
        self.emit_thread = threading.Thread(target=self.emit_metrics, args=(), name="emit_metrics")
        self.process_thread = threading.Thread(target=self.process_metrics_queue, args=(), name="process_metrics_queue")

        # Init metrics
        # Index metrics by appname
        self.metrics = {}

    def __str__(self):
        return "Speakeasy({0}, sock={1}, cmd={2}, pub={3}, legacy={4}, emitter={5})".format(
            self.hostname, self.metric_socket,
            self.cmd_port if self.cmd_port else "-",
            self.pub_port if self.pub_port else "-",
            self.legacy if self.legacy else "-",
            self.emitter_name
        )

    def process_metrics_queue(self):
        logger.info("Starting to process metrics queue")
        while not self.stop.is_set():
            try:
                metric, legacy = self.metrics_queue.get(timeout=QUEUE_WAIT_SECS)
            except six.moves.queue.Empty:
                continue

            try:
                self.process_metric(metric, legacy=legacy)
            except Exception as e:
                logger.warn("Failed to process metric [{0}]: {1}".format(metric, e))

            self.metrics_queue.task_done()
        logger.info("Stopping processing metrics queue")

    def gauge_append(self, lst, value):
        """Add a value to the gauge. This adds to a running sum of the values and a counter for how many values were
        added"""
        lst[0] += value
        lst[1] += 1

    def is_shutdown(self):
        return self.stop.is_set()

    def gauge_sum(self, lst):
        "Returns the computed mean for the gauge values added till now"
        return float(lst[0]) / lst[1]

    def process_gauge_metric(self, app_name, metric_name, value):
        "Updates the gauge value to the internal counters and returns the mean of the values till now"
        with self.metrics_lock:
            dp = self.metrics[app_name]['GAUGE'][metric_name]
            self.gauge_append(dp, value)
            return self.gauge_sum(dp)

    def process_counter_metric(self, app_name, metric_name, value):
        "Add to the internal counter and return the current running total"
        with self.metrics_lock:
            self.metrics[app_name]['COUNTER'][metric_name] += value
            return self.metrics[app_name]['COUNTER'][metric_name]

    def update_internal_counters(self, app_name, metric_type):
        "Track speakeasy stats"
        if app_name == "speakeasy":
            return

        # common
        self.process_metric(["speakeasy", "metrics._type_.{0}.total".format(metric_type), "COUNTER", 1])
        self.process_metric(["speakeasy", "metrics.total", "COUNTER", 1])

        # app
        self.process_metric(["speakeasy", "metrics._app_.{0}.total".format(app_name), "COUNTER", 1])
        self.process_metric(["speakeasy", "metrics._app_.{0}._type_.{1}.total".format(app_name, metric_type),
                             "COUNTER", 1])

    def process_metric(self, metric, legacy=False):
        """Add metric value to internal structures, and publish the current values to the publish socket if asked"""
        if legacy:
            # Legacy format for metrics is slightly different...
            # Index them under same "app name"
            app_name = '__LEGACY__'
            metric_name, value, metric_type = metric.split('|')
        else:
            app_name, metric_name, metric_type, value = metric

        if app_name != "speakeasy":  # avoid loop
            self.update_internal_counters(app_name, metric_type)

        try:
            value = float(value)
        except ValueError:
            logger.warn("Failed to cast metric value to float: app=%s metric_name=%s metric_type=%s value=%s",
                        app_name, metric_name, metric_type, value)
            return

        if app_name not in self.metrics:
            self.init_app_metrics(app_name)

        dp = None
        if self.pub_port:
            pub_metrics = []
        else:
            pub_metrics = None

        pub_val = None
        if metric_type == 'GAUGE':
            pub_val = self.process_gauge_metric(app_name, metric_name, value)
            # Publish the current running average

        elif metric_type == 'PERCENTILE' or metric_type == 'HISTOGRAM':
            # Kill off the HISTOGRAM type!!
            metric_type = 'PERCENTILE'
            if not metric_name.endswith("."):
                metric_name += "."
            # Track average value separately
            avg_pub_val = self.process_gauge_metric(app_name, metric_name + 'average', value)
            with self.metrics_lock:
                dp = self.metrics[app_name][metric_type][metric_name]
                # dp must be sorted before passing to utils.percentile
                bisect.insort(dp, value)
            # Publish the current running percentiles
            if self.pub_port:
                cur_time = time.time()
                for p in self.percentiles:
                    pub_metrics.append((self.hostname, app_name,
                                        '{0}{1}_percentile'.format(metric_name, int(p * 100)),
                                        'GAUGE', utils.percentile(dp, p),
                                        cur_time))
                pub_metrics.append((self.hostname, app_name, metric_name + 'average', 'GAUGE', avg_pub_val, cur_time))
        elif metric_type == 'COUNTER':
            pub_val = self.process_counter_metric(app_name, metric_name, value)
            # Publish the running count
        else:
            logger.warn("Unrecognized metric type - {0}".format(metric))
            return

        if self.pub_port:
            if metric_type != 'PERCENTILE':
                pub_metrics.append((self.hostname, app_name, metric_name,
                                    metric_type, pub_val, time.time()))
            msg = ujson.dumps(pub_metrics, ensure_ascii=False)
            self.pub_socket.send_string(msg)

    def process_command(self, cmd):
        """ Process command and reply """
        # TODO: Do something here
        pass

    def watch_memory(self):
        rss_usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # https://github.com/kmike/scrapy/commit/a83c6f545c440edb901e706beb2cb4e6c24b8381
        if sys.platform != "darwin":
            rss_usage *= 1024
        self.process_metric(["speakeasy", "mem.rss", "GAUGE", rss_usage])
        if not self.stop.is_set() and (rss_usage > MAX_RSS):
            logger.error("RSS usage (%d) > limit (%d). Killing myself.", rss_usage, MAX_RSS)
            os.kill(os.getpid(), signal.SIGTERM)

    def poll_sockets(self):
        """ Poll metrics socket and cmd socket for data """
        logger.info("Start polling")
        loop = 0
        received_count = 0
        good_count = 0
        decode_errors = 0
        empty_packets = 0
        while not self.stop.is_set():
            loop += 1
            if not (loop % METRIC_CHECKPOINT_INTERVAL):  # 20 * 0.5s
                self.watch_memory()
                self.process_metric(["speakeasy", "metrics_queue.length", "GAUGE", self.metrics_queue.qsize()])

                self.process_metric(["speakeasy", "received_metrics.total", "COUNTER", received_count])
                self.process_metric(["speakeasy", "received_metrics.empty.total", "COUNTER", empty_packets])
                self.process_metric(["speakeasy", "received_metrics.good.total", "COUNTER", good_count])
                self.process_metric(["speakeasy", "received_metrics.decode_errors.total", "COUNTER", decode_errors])

                received_count = good_count = decode_errors = empty_packets = 0

            socks = dict(self.poller.poll(POLLING_TIMEOUT_MS))

            if socks.get(self.recv_socket) == zmq.POLLIN:
                try:
                    data = self.recv_socket.recv_string()
                    received_count += 1
                    if not data:
                        logger.warn("Got an empty packet (possibly rumrunner ping)")
                        empty_packets += 1
                        continue
                    metric = ujson.loads(data)
                    # Put metric on metrics queue
                    self.metrics_queue.put((metric, False))
                    good_count += 1
                except ValueError as e:
                    logger.warn("Error receiving metric [data=%s]: %s", data, e)
                    decode_errors += 1
                    continue
                # logger.debug("Received and processed %d metrics from socket", count)

            if socks.get(self.cmd_socket) == zmq.POLLIN:
                while True:
                    try:
                        data = self.cmd_socket.recv_string(zmq.NOBLOCK)
                        cmd = ujson.loads(data)
                        # Process command
                        self.process_command(cmd)
                    except zmq.ZMQError:
                        break
                    except ValueError as e:
                        logger.warn("Error receiving command [data=%s]: %s", data, e)

            if self.legacy and socks.get(self.legacy_socket_fno) == zmq.POLLIN:
                # Process legacy format
                try:
                    data, addr = self.legacy_socket.recvfrom(8192)
                    self.metrics_queue.put((data.decode("utf-8"), True))
                except Exception as e:
                    logger.error('Error on legacy socket - {0}'.format(e))

        logger.info("Stopped polling")

    def emit_metrics(self):
        """ Send snapshot of metrics through emitter """
        logger.info("Started emitting")
        while not self.stop.is_set():
            e_start = time.time()

            # Grab "this is what the world looks like now" snapshot
            metrics_ss = self.snapshot()

            if self.emitter:
                self.emitter.emit(metrics_ss)
            e_end = time.time()

            duration = e_end - e_start
            sleep_time = self.emission_interval - duration
            logger.info("Emitted %d metrics in %.2f seconds. Will sleep for %.2f seconds",
                        len(metrics_ss), duration, sleep_time)
            self.stop.wait(sleep_time)

        logger.info("Stopped emitting")

    def snapshot(self):
        """
        Return a snapshot of current metrics

        [(app, metric, val, type, timestamp), ...]
        """
        metrics = []
        with self.metrics_lock:
            ss = copy.deepcopy(self.metrics)

        # Reset metrics
        self.reset_metrics()

        for app in ss:
            for m, val in six.iteritems(ss[app]['COUNTER']):
                metrics.append((app, m, val, 'COUNTER', time.time()))

            for m, vals in six.iteritems(ss[app]['GAUGE']):
                if vals[1] == 0:
                    logger.debug("No values for GAUGE metric: app=%s metric_name=%s", app, m)
                    continue

                if vals:
                    metrics.append((app, m, vals[0] / float(vals[1]), 'GAUGE', time.time()))

            for m, vals in six.iteritems(ss[app]['PERCENTILE']):
                if len(vals) == 0:
                    logger.debug("No values for PERCENTILE metric: app=%s metric_name=%s", app, m)
                    continue

                # Emit 50%, 75%, 95%, 99% as GAUGE
                for p in self.percentiles:
                    # Assume the metric name has a trailing separator to append
                    # the percentile to
                    metrics.append((app, '{0}{1}_percentile'.format(m, int(p * 100)),
                                    utils.percentile(vals, p), 'GAUGE', time.time()))
        return metrics

    def reset_metrics(self):
        """ Reset metrics for next interval """
        with self.metrics_lock:
            for app in self.metrics:
                self.metrics[app]['GAUGE'] = collections.defaultdict(_gauge_pair)
                self.metrics[app]['PERCENTILE'] = collections.defaultdict(list)

    def init_app_metrics(self, app):
        """ Setup initial metric structure for new app """
        if app not in self.metrics:
            with self.metrics_lock:
                self.metrics[app] = {
                    'GAUGE': collections.defaultdict(_gauge_pair),
                    'COUNTER': collections.defaultdict(int),
                    'PERCENTILE': collections.defaultdict(list)
                }

    def start(self):
        self.__start()

    def shutdown(self):
        logger.info("Shutting down")
        self.__stop()

    def __start(self):
        self.poll_thread.start()
        self.emit_thread.start()
        self.process_thread.start()

    def __stop(self):
        if not self.stop.is_set():
            self.stop.set()
            if self.poll_thread and self.poll_thread.isAlive():
                logger.info("Waiting for poll thread to stop...")
                self.poll_thread.join()

            if self.emit_thread and self.emit_thread.isAlive():
                logger.info("Waiting for emit thread to stop...")
                self.emit_thread.join()

            if self.process_thread and self.process_thread.isAlive():
                logger.info("Waiting for metrics queue processing thread to stop...")
                self.process_thread.join()

        self.cleanup()

    def cleanup(self):
        try:
            if self.legacy:
                self.legacy_socket.close()
                if os.path.exists(self.legacy):
                    os.remove(self.legacy)
                    logger.info('Deleted legacy socket at %s', self.legacy)

            self.recv_socket.close()
            os.remove(self.metric_socket)
            logger.info('Deleted metrics socket at %s', self.metric_socket)

            self.cmd_socket.close()
            if self.pub_port:
                self.pub_socket.close()
            self.context.term()
        except Exception as e:
            logger.error(e)


def import_emitter(name, **kwargs):
    """Returns an instance of an emitter with a class `Emitter` in the module `speakeasy.emitter.$name`,
    or `None` if no such module could be found"""

    namespace = 'speakeasy.emitter.'
    if namespace not in name:
        name = namespace + name

    try:
        __import__(name, level=0)
    except Exception as e:
        # app doesn't exist
        logger.error("Error loading module %s: %s", name, e)
        return None

    module = sys.modules[name]

    try:
        return module.Emitter(**kwargs)
    except Exception as e:  # if the Emitter class is not present, or fails to instantiate
        logger.error("Error initializing emitter %s: %s", name, e)
        return None


if __name__ == '__main__':
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setFormatter(
        logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S'))
    logging.getLogger().addHandler(stderr_handler)
    logging.getLogger().setLevel(logging.INFO)

    server = Speakeasy('0.0.0.0', '/var/tmp/metrics_socket', '55001', '55002',
                       'simple', ['filename=stderr'], 10,
                       '/var/tmp/metric_socket2')
    server.start()
    logger.info("Started")
    while True:
        try:
            time.sleep(1)
        except Exception as e:
            logger.info("Exception (%s)... exiting", e)
            break
        except KeyboardInterrupt:
            logger.info("ctrl-c ... quitting")
            break
    server.shutdown()
