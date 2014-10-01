import unittest2 as unittest
import zmq
from speakeasy.speakeasy import Speakeasy


def gen_speakeasy_server():
    return Speakeasy('0.0.0.0', '/var/tmp/test_metric_socket', '50001', '50002',
                     'simple', ['filename=/var/tmp/test_metrics.out'], 60,
                     '/var/tmp/test_metric_socket2')

class TestSpeakeasy(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.srv = gen_speakeasy_server()
        self.srv.start()

    @classmethod
    def tearDownClass(self):
        self.srv.shutdown()

    def setUp(self):
        pass

    def tearDown(self):
        self.srv.metrics = {}

    # we use _0_ to force init test run first
    def test_0_server_init(self):
        self.assertEqual(self.srv.metric_socket, '/var/tmp/test_metric_socket')
        self.assertEqual(self.srv.legacy, '/var/tmp/test_metric_socket2')
        self.assertEqual(self.srv.cmd_port, '50001')
        self.assertEqual(self.srv.pub_port, '50002')
        self.assertEqual(self.srv.emission_interval, 60)
        self.assertEqual(self.srv.emitter_args['filename'],
                         '/var/tmp/test_metrics.out')
        self.assertEqual(len(self.srv.metrics), 0)
        self.assertEqual(self.srv.running, True)

    def test_process_metric(self):
        self.srv.process_metric(['test_app', 'test_metric', 'GAUGE', 1])
        metric = self.srv.metrics['test_app']['GAUGE']['test_metric']
        self.assertTrue(sorted(metric) == [1.0])


if __name__ == '__main__':
    unittest.main()
