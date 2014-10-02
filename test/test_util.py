#!/usr/bin/env python
# -*- coding:utf-8 -*-

def get_random_free_port():
    import socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
    sock.bind(('', 0))
    sock.listen(socket.SOMAXCONN)
    ipaddr, port = sock.getsockname()
    sock.close()
    return port
