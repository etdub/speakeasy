#!/usr/bin/env python
# -*- coding:utf-8 -*-
from __future__ import absolute_import
import zmq
import json
import logging

logger = logging.getLogger(__name__)


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
            metrics = json.loads(sub_socket.recv_string(zmq.NOBLOCK))
            if metrics[0][1] != 'speakeasy':
                return metrics
        except zmq.Again:
            pass
        except zmq.ZMQError as e:
            logging.error("**TESTS**: Got error receiving zmq event: %s", e)


def filtered_metrics_from_emitter(call_args_list):
    "From the mocked calls to the emitters extract all the non-internal (app speakeasy) metrics"
    filtered_metrics = []
    for call in call_args_list:
        args, kwargs = call
        for metric in args[0]:
            if metric[0] != "speakeasy":
                filtered_metrics.append(metric)
    logger.info("**TESTS**: Filtered emitter metrics=%s", filtered_metrics)
    return filtered_metrics
