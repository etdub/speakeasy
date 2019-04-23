#!/usr/bin/env python
# -*- coding:utf-8 -*-

from __future__ import absolute_import
import unittest2 as unittest
import zmq
import ujson as json
import time
import random
import os
from mock import Mock
from speakeasy.speakeasy import Speakeasy
from test_util import get_random_free_port, filtered_metric_recv, filtered_metrics_from_emitter
import speakeasy.speakeasy as speakeasy
import logging
import logging.handlers

# custom logger to bypass nose logging capture
logger = logging.getLogger()
handler = logging.handlers.RotatingFileHandler("test_zmq_spec.log")
handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] (%(funcName)s) %(message)s',
                     datefmt='%Y-%m-%d %H:%M:%S'))
logger.addHandler(handler)

speakeasy.QUEUE_WAIT_SECS = 1

random.seed(time.time())
G_SPEAKEASY_HOST = '0.0.0.0'
G_METRIC_SOCKET = '/var/tmp/test_zmq_spec_metric_sock{0}'.format(random.random())
G_LEGACY_METRIC_SOCKET = '/var/tmp/test_zmq_spec_legacy_metric_{0}'.format(random.random())
G_EMITTER_LOG = '/var/tmp/test_zmq_spec_metrics_{0}.out'.format(random.random())
G_PUB_PORT = str(get_random_free_port())
G_CMD_PORT = str(get_random_free_port())


def gen_speakeasy_server():
    return Speakeasy(G_SPEAKEASY_HOST, G_METRIC_SOCKET, G_CMD_PORT, G_PUB_PORT,
                     'simple', ['filename=' + G_EMITTER_LOG], 1, G_LEGACY_METRIC_SOCKET)


class TestEmitter(object):
    def __init__(self, **kwargs):
        pass

    def emit(self, metrics):
        """ Ship the metrics off """
        pass


class TestZmqSpec(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.srv = gen_speakeasy_server()
        logger.info("Server generated for test: %s", self.srv)
        # patch emitter
        self.srv.emitter = TestEmitter()
        self.send_socket = zmq.Context().socket(zmq.PUSH)
        self.send_socket.set_hwm(5000)
        self.send_socket.connect('ipc://{0}'.format(G_METRIC_SOCKET))
        self.send_socket.setsockopt(zmq.LINGER, 0)

        self.sub_socket = zmq.Context().socket(zmq.SUB)
        self.sub_socket.connect('tcp://localhost:{0}'.format(G_PUB_PORT))
        self.sub_socket.setsockopt_string(zmq.SUBSCRIBE, u'')
        self.sub_socket.setsockopt(zmq.LINGER, 0)

        self.poller = zmq.Poller()
        self.poller.register(self.sub_socket, zmq.POLLIN)

        self.srv.start()

    @classmethod
    def tearDownClass(self):
        logger.info("tearing down test class")
        self.srv.shutdown()
        self.send_socket.close()
        self.sub_socket.close()
        zmq.Context().term()
        for f in (G_METRIC_SOCKET, G_LEGACY_METRIC_SOCKET, G_EMITTER_LOG):
            if os.path.exists(f):
                os.remove(f)

    def setUp(self):
        pass

    def tearDown(self):
        with self.srv.metrics_lock:
            self.srv.metrics = {}

    def clear_sub_socket(self):
        while True:
            try:
                socks = dict(self.poller.poll(500))
                if self.sub_socket in socks:
                    self.sub_socket.recv()  # suck out zmq pub event
                else:
                    break
            except zmq.Again:
                pass
            except zmq.ZMQError as e:
                logger.error("Got error clearing socket: %s", e)

    def test_send_gauge_to_emitter(self):
        self.srv.emitter.emit = Mock()
        self.send_socket.send_string(
            json.dumps(['test_app', 'test_gauge', 'GAUGE', '2']))
        time.sleep(2)
        # call_list = self.srv.emitter.emit.call_args_list
        filtered_metrics = filtered_metrics_from_emitter(self.srv.emitter.emit.call_args_list)
        self.assertGreater(len(filtered_metrics), 0, "There should be at least one metric being emitted")
        m = filtered_metrics[0]
        self.assertEqual(len(m), 5)
        self.assertEqual(m[0], u'test_app')
        self.assertEqual(m[1], u'test_gauge')
        self.assertEqual(m[2], 2.0)
        self.assertEqual(m[3], 'GAUGE')

    def test_send_counter_to_emitter(self):
        self.srv.emitter.emit = Mock()
        self.send_socket.send_string(
            json.dumps(['test_app', 'test_counter', 'COUNTER', '15']))
        time.sleep(2)
        filtered_metrics = filtered_metrics_from_emitter(self.srv.emitter.emit.call_args_list)
        m = filtered_metrics[0]
        self.assertEqual(len(m), 5)
        self.assertEqual(m[0], u'test_app')
        self.assertEqual(m[1], u'test_counter')
        self.assertEqual(m[2], 15.0)
        self.assertEqual(m[3], 'COUNTER')

    def test_send_gauge_to_pub_socket(self):
        self.clear_sub_socket()
        self.send_socket.send_string(
            json.dumps(['test_app2', 'test_gauge', 'GAUGE', '5']))
        self.assertGreater(len(dict(self.poller.poll(500))), 0)
        metrics = filtered_metric_recv(self.sub_socket)
        self.assertEqual(len(metrics), 1)
        self.assertEqual(metrics[0][0], G_SPEAKEASY_HOST)
        self.assertEqual(metrics[0][1], u'test_app2')
        self.assertEqual(metrics[0][2], u'test_gauge')
        self.assertEqual(metrics[0][3], 'GAUGE')
        self.assertEqual(metrics[0][4], 5.0)

    def test_send_counter_to_pub_socket(self):
        try:
            self.clear_sub_socket()
        except Exception as e:
            logger.fatal("Could not clear socket: %s", e)
        self.send_socket.send_string(
            json.dumps(['test_app2', 'test_counter', 'COUNTER', '1']))
        self.assertGreater(len(dict(self.poller.poll(500))), 0)
        metrics = filtered_metric_recv(self.sub_socket)
        self.assertEqual(len(metrics), 1)
        self.assertEqual(metrics[0][0], G_SPEAKEASY_HOST)
        self.assertEqual(metrics[0][1], u'test_app2')
        self.assertEqual(metrics[0][2], u'test_counter')
        self.assertEqual(metrics[0][3], 'COUNTER')
        self.assertEqual(metrics[0][4], 1)

    def test_send_percentile_to_pub_socket(self):
        self.clear_sub_socket()
        self.send_socket.send_string(
            json.dumps(['test_app2', 'test_metric', 'PERCENTILE', '13']))
        self.assertGreater(len(dict(self.poller.poll(500))), 0)
        metrics = filtered_metric_recv(self.sub_socket)
        self.assertEqual(len(metrics), 5)
        self.assertEqual(metrics[0][0], G_SPEAKEASY_HOST)
        self.assertEqual(metrics[0][1], u'test_app2')
        self.assertEqual(metrics[0][2], u'test_metric.50_percentile')
        self.assertEqual(metrics[0][3], 'GAUGE')
        self.assertEqual(metrics[0][4], 13)
        self.assertEqual(metrics[1][0], G_SPEAKEASY_HOST)
        self.assertEqual(metrics[1][1], u'test_app2')
        self.assertEqual(metrics[1][2], u'test_metric.75_percentile')
        self.assertEqual(metrics[1][3], 'GAUGE')
        self.assertEqual(metrics[1][4], 13)
        self.assertEqual(metrics[2][0], G_SPEAKEASY_HOST)
        self.assertEqual(metrics[2][1], u'test_app2')
        self.assertEqual(metrics[2][2], u'test_metric.95_percentile')
        self.assertEqual(metrics[2][3], 'GAUGE')
        self.assertEqual(metrics[2][4], 13)
        self.assertEqual(metrics[3][0], G_SPEAKEASY_HOST)
        self.assertEqual(metrics[3][1], u'test_app2')
        self.assertEqual(metrics[3][2], u'test_metric.99_percentile')
        self.assertEqual(metrics[3][3], 'GAUGE')
        self.assertEqual(metrics[3][4], 13)
        self.assertEqual(metrics[4][0], G_SPEAKEASY_HOST)
        self.assertEqual(metrics[4][1], u'test_app2')
        self.assertEqual(metrics[4][2], u'test_metric.average')
        self.assertEqual(metrics[4][3], 'GAUGE')
        self.assertEqual(metrics[4][4], 13)

        self.send_socket.send_string(
            json.dumps(['test_app2', 'test_metric', 'PERCENTILE', '1']))
        self.assertGreater(len(dict(self.poller.poll(500))), 0)
        metrics = filtered_metric_recv(self.sub_socket)
        self.assertEqual(len(metrics), 5)
        self.assertEqual(metrics[0][0], G_SPEAKEASY_HOST)
        self.assertEqual(metrics[0][1], u'test_app2')
        self.assertEqual(metrics[0][2], u'test_metric.50_percentile')
        self.assertEqual(metrics[0][3], 'GAUGE')
        self.assertEqual(metrics[0][4], 7.0)
        self.assertEqual(metrics[1][0], G_SPEAKEASY_HOST)
        self.assertEqual(metrics[1][1], u'test_app2')
        self.assertEqual(metrics[1][2], u'test_metric.75_percentile')
        self.assertEqual(metrics[1][3], 'GAUGE')
        self.assertEqual(metrics[1][4], 10.0)
        self.assertEqual(metrics[2][0], G_SPEAKEASY_HOST)
        self.assertEqual(metrics[2][1], u'test_app2')
        self.assertEqual(metrics[2][2], u'test_metric.95_percentile')
        self.assertEqual(metrics[2][3], 'GAUGE')
        self.assertEqual(metrics[2][4], 12.4)
        self.assertEqual(metrics[3][0], G_SPEAKEASY_HOST)
        self.assertEqual(metrics[3][1], u'test_app2')
        self.assertEqual(metrics[3][2], u'test_metric.99_percentile')
        self.assertEqual(metrics[3][3], 'GAUGE')
        self.assertEqual(metrics[3][4], 12.880000000000001)
        self.assertEqual(metrics[4][0], G_SPEAKEASY_HOST)
        self.assertEqual(metrics[4][1], u'test_app2')
        self.assertEqual(metrics[4][2], u'test_metric.average')
        self.assertEqual(metrics[4][3], 'GAUGE')
        self.assertEqual(metrics[4][4], 7)

        self.send_socket.send_string(
            json.dumps(['test_app2', 'test_metric', 'PERCENTILE', '5']))
        self.assertGreater(len(dict(self.poller.poll(500))), 0)
        metrics = filtered_metric_recv(self.sub_socket)
        self.assertEqual(len(metrics), 5)
        self.assertEqual(metrics[0][0], G_SPEAKEASY_HOST)
        self.assertEqual(metrics[0][1], u'test_app2')
        self.assertEqual(metrics[0][2], u'test_metric.50_percentile')
        self.assertEqual(metrics[0][3], 'GAUGE')
        self.assertEqual(metrics[0][4], 5.0)
        self.assertEqual(metrics[1][0], G_SPEAKEASY_HOST)
        self.assertEqual(metrics[1][1], u'test_app2')
        self.assertEqual(metrics[1][2], u'test_metric.75_percentile')
        self.assertEqual(metrics[1][3], 'GAUGE')
        self.assertEqual(metrics[1][4], 9.0)
        self.assertEqual(metrics[2][0], G_SPEAKEASY_HOST)
        self.assertEqual(metrics[2][1], u'test_app2')
        self.assertEqual(metrics[2][2], u'test_metric.95_percentile')
        self.assertEqual(metrics[2][3], 'GAUGE')
        self.assertEqual(metrics[2][4], 12.199999999999999)
        self.assertEqual(metrics[3][0], G_SPEAKEASY_HOST)
        self.assertEqual(metrics[3][1], u'test_app2')
        self.assertEqual(metrics[3][2], u'test_metric.99_percentile')
        self.assertEqual(metrics[3][3], 'GAUGE')
        self.assertEqual(metrics[3][4], 12.84)
        self.assertEqual(metrics[4][0], G_SPEAKEASY_HOST)
        self.assertEqual(metrics[4][1], u'test_app2')
        self.assertEqual(metrics[4][2], u'test_metric.average')
        self.assertEqual(metrics[4][3], 'GAUGE')
        self.assertEqual(metrics[4][4], 6.3333333332999997)

    def test_gauge_avg_calculation(self):
        self.clear_sub_socket()
        self.send_socket.send_string(
            json.dumps(['test_app', 'test_gauge', 'GAUGE', '5']))
        self.assertGreater(len(dict(self.poller.poll(500))), 0)
        metrics = filtered_metric_recv(self.sub_socket)
        self.assertIsNotNone(metrics)
        self.assertEqual(len(metrics), 1)
        self.assertEqual(metrics[0][:-1],
                         [G_SPEAKEASY_HOST, u'test_app', u'test_gauge',
                          u'GAUGE', 5.0])

        self.send_socket.send_string(
            json.dumps(['test_app', 'test_gauge', 'GAUGE', '3']))
        self.assertGreater(len(dict(self.poller.poll(500))), 0)
        metrics = filtered_metric_recv(self.sub_socket)
        self.assertEqual(len(metrics), 1)
        self.assertEqual(metrics[0][:-1],
                         [G_SPEAKEASY_HOST, u'test_app', u'test_gauge',
                          u'GAUGE', 4.0])

        self.send_socket.send_string(
            json.dumps(['test_app', 'test_gauge', 'GAUGE', '13']))
        self.assertGreater(len(dict(self.poller.poll(500))), 0)
        metrics = filtered_metric_recv(self.sub_socket)
        self.assertEqual(len(metrics), 1)
        self.assertEqual(metrics[0][:-1],
                         [G_SPEAKEASY_HOST, u'test_app', u'test_gauge',
                          u'GAUGE', 7.0])

        self.send_socket.send_string(
            json.dumps(['test_app', 'test_gauge', 'GAUGE', '3']))
        self.assertGreater(len(dict(self.poller.poll(500))), 0)
        metrics = filtered_metric_recv(self.sub_socket)
        self.assertEqual(len(metrics), 1)
        self.assertEqual(metrics[0][:-1],
                         [G_SPEAKEASY_HOST, u'test_app', u'test_gauge',
                          u'GAUGE', 6.0])


if __name__ == '__main__':
    unittest.main()
