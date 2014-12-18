import unittest2 as unittest
import os
import stat
import random
import zmq
from test_util import get_random_free_port
from speakeasy.speakeasy import Speakeasy

G_SPEAKEASY_HOST = '0.0.0.0'
G_PUB_PORT = str(get_random_free_port())
G_CMD_PORT = str(get_random_free_port())
G_METRIC_SOCKET = '/var/tmp/test_metric_{0}'.format(random.random())


def gen_speakeasy_server():
    return Speakeasy(G_SPEAKEASY_HOST, G_METRIC_SOCKET, G_CMD_PORT, G_PUB_PORT,
                     'simple', ['filename=/var/tmp/test_metrics.out'], 60)


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
    def test_0_server_init(self):
        self.assertEqual(self.srv.metric_socket, G_METRIC_SOCKET)
        self.assertEqual(self.srv.cmd_port, G_CMD_PORT)
        self.assertEqual(self.srv.pub_port, G_PUB_PORT)
        self.assertEqual(self.srv.emission_interval, 60)
        self.assertEqual(self.srv.emitter_args['filename'],
                         '/var/tmp/test_metrics.out')
        self.assertEqual(len(self.srv.metrics), 0)
        self.assertEqual(self.srv.running, True)

    def test_process_metric(self):
        self.srv.process_metric(['test_app', 'test_metric', 'GAUGE', 1])
        metric = self.srv.metrics['test_app']['GAUGE']['test_metric']
        self.assertTrue(sorted(metric) == [1.0])

    def test_process_command(self):
        req_sock = zmq.Context().socket(zmq.REQ)
        req_sock.connect('tcp://localhost:{0}'.format(G_CMD_PORT))
        req_sock.send('{}')
        # TODO: fill this when process_command is implemented

    def test_socket_cleanup(self):
        tmp_metric_socket = '/var/tmp/tmp_test_metric_socket{0}'.format(
                                random.random())
        srv = Speakeasy(G_SPEAKEASY_HOST, tmp_metric_socket,
                      str(get_random_free_port()),
                      str(get_random_free_port()), 'simple',
                      ['filename=/var/tmp/test_metrics.out'],
                      60, socket_mod=0666)
        srv.start()
        self.assertTrue(os.path.exists(tmp_metric_socket))
        srv.shutdown()
        self.assertFalse(os.path.exists(tmp_metric_socket))

    def test_socket_mod(self):
        tmp_metric_socket = '/var/tmp/tmp_test_metric_{0}'.format(
                                random.random())
        s1 = Speakeasy(G_SPEAKEASY_HOST, tmp_metric_socket,
                       str(get_random_free_port()),
                       str(get_random_free_port()), 'simple',
                       ['filename=/var/tmp/test_metrics.out'],
                       60, socket_mod=0666)
        self.assertEqual(oct(stat.S_IMODE(os.stat(tmp_metric_socket).st_mode)),
                         '0666')

        s2 = Speakeasy(G_SPEAKEASY_HOST, tmp_metric_socket,
                       str(get_random_free_port()),
                       str(get_random_free_port()), 'simple',
                       ['filename=/var/tmp/test_metrics.out'], 60)
        self.assertNotEqual(
            oct(stat.S_IMODE(os.stat(tmp_metric_socket).st_mode)), '0666')

    def test_start_without_pub_port(self):
        tmp_metric_socket = '/var/tmp/tmp_test_metric_{0}'.format(
                                random.random())
        s = Speakeasy(G_SPEAKEASY_HOST, tmp_metric_socket,
                       str(get_random_free_port()), None, 'simple',
                       ['filename=/var/tmp/test_metrics.out'], 60)
        self.assertEqual(s.metric_socket, tmp_metric_socket)
        self.assertEqual(s.pub_port, None)
        self.assertEqual(s.emission_interval, 60)
        self.assertEqual(s.emitter_args['filename'],
                         '/var/tmp/test_metrics.out')
        self.assertEqual(len(s.metrics), 0)


if __name__ == '__main__':
    unittest.main()
