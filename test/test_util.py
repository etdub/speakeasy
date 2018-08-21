#!/usr/bin/env python
# -*- coding:utf-8 -*-
import zmq
import json
import logging


def get_random_free_port():
    import socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
    sock.bind(('', 0))
    sock.listen(socket.SOMAXCONN)
    ipaddr, port = sock.getsockname()
    sock.close()
    return port


def filtered_metric_recv(sub_socket):
    "Receive a single non-speakeasy metric"
    while True:  # skip internal metrics
        try:
            metrics = json.loads(sub_socket.recv(zmq.NOBLOCK))
            if metrics[0][1] != 'speakeasy':
                return metrics
        except zmq.ZMQError as e:
            logging.error("**TESTS**: Got error receiving zmq event: %s", e)
