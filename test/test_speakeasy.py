from __future__ import absolute_import
import unittest2 as unittest
import os
import stat
import random
import zmq
from test_util import get_random_free_port
from speakeasy.speakeasy import Speakeasy
import speakeasy.speakeasy as speakeasy
import logging
import logging.handlers

# custom logger to bypass nose logging capture
logger = logging.getLogger()
handler = logging.handlers.RotatingFileHandler("test_speakeasy.log")
handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] (%(funcName)s) %(message)s',
                                       datefmt='%Y-%m-%d %H:%M:%S'))
logger.addHandler(handler)

G_SPEAKEASY_HOST = '0.0.0.0'
G_PUB_PORT = str(get_random_free_port())
G_CMD_PORT = str(get_random_free_port())
G_METRIC_SOCKET = '/var/tmp/test_metric_socket_{0}'.format(random.random())
G_EMITTER_LOG = '/var/tmp/test_metrics_{0}.out'.format(random.random())
speakeasy.QUEUE_WAIT_SECS = 1


def gen_speakeasy_server():
    return Speakeasy(G_SPEAKEASY_HOST, G_METRIC_SOCKET, G_CMD_PORT, G_PUB_PORT,
                     'simple', ['filename=' + G_EMITTER_LOG], 60)


class TestSpeakeasy(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.srv = gen_speakeasy_server()
        logger.info("Server generated: %s", self.srv)
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
        self.sub_socket.close()
        if os.path.exists(G_METRIC_SOCKET):
            os.remove(G_METRIC_SOCKET)
        if os.path.exists(G_EMITTER_LOG):
            os.remove(G_EMITTER_LOG)

    def setUp(self):
        pass

    def tearDown(self):
        with self.srv.metrics_lock:
            self.srv.metrics = {}

    def clear_sub_socket(self):
        while True:
            socks = dict(self.poller.poll(500))
            if self.sub_socket in socks:
                self.sub_socket.recv()  # suck out zmq pub event
            else:
                break

    # we use _0_ to force init test run first
    def test_0_server_init(self):
        self.assertEqual(self.srv.metric_socket, G_METRIC_SOCKET)
        self.assertEqual(self.srv.cmd_port, G_CMD_PORT)
        self.assertEqual(self.srv.pub_port, G_PUB_PORT)
        self.assertEqual(self.srv.emission_interval, 60)
        self.assertEqual(self.srv.emitter_args['filename'], G_EMITTER_LOG)
        self.assertEqual(len(self.srv.metrics), 0)
        self.assertFalse(self.srv.is_shutdown())

    def test_process_metric(self):
        self.srv.process_metric(['test_app', 'test_metric', 'GAUGE', 1])
        metric = self.srv.metrics['test_app']['GAUGE']['test_metric']
        self.assertTrue(metric == [1.0, 1])

    def test_process_command(self):
        req_sock = zmq.Context().socket(zmq.REQ)
        req_sock.connect('tcp://localhost:{0}'.format(G_CMD_PORT))
        req_sock.send_string('{}')
        req_sock.close()
        # TODO: fill this when process_command is implemented

    def test_socket_cleanup(self):
        tmp_metric_socket = '/var/tmp/test_metric_socket_cleanup{0}'.format(random.random())
        srv = Speakeasy(G_SPEAKEASY_HOST, tmp_metric_socket, str(get_random_free_port()), str(get_random_free_port()),
                        'simple', ['filename=' + G_EMITTER_LOG], 60, socket_mod=0o666)
        logger.info("Server generated: %s", srv)
        srv.start()
        self.assertTrue(os.path.exists(tmp_metric_socket))
        srv.shutdown()
        self.assertFalse(os.path.exists(tmp_metric_socket))

    def test_socket_mod(self):
        tmp_metric_socket = '/var/tmp/test_metric_mod{0}'.format(random.random())

        srv = Speakeasy(G_SPEAKEASY_HOST, tmp_metric_socket, str(get_random_free_port()), str(get_random_free_port()),
                        'simple', ['filename=' + G_EMITTER_LOG], 60, socket_mod=0o666)
        logger.info("Server generated: %s", srv)
        self.assertEqual((stat.S_IMODE(os.stat(tmp_metric_socket).st_mode)), 0o666)
        srv.shutdown()

        srv = Speakeasy(G_SPEAKEASY_HOST, tmp_metric_socket, str(get_random_free_port()), str(get_random_free_port()),
                        'simple', ['filename=' + G_EMITTER_LOG], 60)
        logger.info("Server generated: %s", srv)
        self.assertNotEqual((stat.S_IMODE(os.stat(tmp_metric_socket).st_mode)), 0o666)
        srv.shutdown()

    def test_start_without_pub_port(self):
        tmp_metric_socket = '/var/tmp/test_metric_wo_pub_port{0}'.format(random.random())
        s = Speakeasy(G_SPEAKEASY_HOST, tmp_metric_socket, str(get_random_free_port()), None,
                      'simple', ['filename=' + G_EMITTER_LOG], 60)
        logger.info("Server generated: %s", s)
        self.assertEqual(s.metric_socket, tmp_metric_socket)
        self.assertEqual(s.pub_port, None)
        self.assertEqual(s.emission_interval, 60)
        self.assertEqual(s.emitter_args['filename'], G_EMITTER_LOG)
        self.assertEqual(len(s.metrics), 0)
        s.shutdown()


if __name__ == '__main__':
    unittest.main()
