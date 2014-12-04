import unittest2 as unittest
import random
import socket
import json
import zmq
from test_util import get_random_free_port
from speakeasy.speakeasy import Speakeasy

G_SPEAKEASY_HOST = '0.0.0.0'
G_PUB_PORT = str(get_random_free_port())
G_CMD_PORT = str(get_random_free_port())
G_METRIC_SOCKET = '/var/tmp/test_metric_{0}'.format(random.random())
G_LEGACY_METRIC_SOCKET = '/var/tmp/legacy_metric_{0}'.format(random.random())


def gen_speakeasy_server():
    return Speakeasy(G_SPEAKEASY_HOST, G_METRIC_SOCKET, G_CMD_PORT, G_PUB_PORT,
                     'simple', ['filename=/var/tmp/test_metrics.out'],
                     60, G_LEGACY_METRIC_SOCKET)


class TestSpeakeasy(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.srv = gen_speakeasy_server()

        self.sub_socket = zmq.Context().socket(zmq.SUB)
        self.sub_socket.connect('tcp://localhost:{0}'.format(G_PUB_PORT))
        self.sub_socket.setsockopt(zmq.SUBSCRIBE, '')

        self.poller = zmq.Poller()
        self.poller.register(self.sub_socket, zmq.POLLIN)

        self.srv.start()

    @classmethod
    def tearDownClass(self):
        self.srv.shutdown()

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
    def test_legacy_socket(self):
        self.clear_sub_socket()
        legacy_sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        legacy_sock.connect(G_LEGACY_METRIC_SOCKET)
        legacy_sock.send('test_cnt3|10|COUNTER')
        self.assertGreater(len(dict(self.poller.poll(500))), 0)
        metrics = json.loads(self.sub_socket.recv())
        self.assertEqual(len(metrics), 1)
        self.assertEqual(metrics[0][0], G_SPEAKEASY_HOST)
        self.assertEqual(metrics[0][1], u'__LEGACY__')
        self.assertEqual(metrics[0][2], u'test_cnt3')
        self.assertEqual(metrics[0][3], 'COUNTER')
        self.assertEqual(metrics[0][4], 10)


if __name__ == '__main__':
    unittest.main()
