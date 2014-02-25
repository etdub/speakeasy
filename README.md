speakeasy [![Build Status](https://travis-ci.org/etdub/speakeasy.png?branch=master)](https://travis-ci.org/etdub/speakeasy)
=========

Speakeasy is a metrics aggregation server that listens for data locally over a named pipe. Received data is periodically
aggregated, formatted, and shipped off to central metrics collectors through the pluggable emitter system.

Speakeasy also has the abilility to publish the data it receives it for possible real-time analysis.

Why?
====

Speakeasy was created to help instrument a python webapp running with Apache and mod_wsgi. Instead of sending
metrics per Apache child to central metrics collectors, the data needed to be aggregated on box and then sent out. This
allows tracking QPS, latency, error rate, etc. in an aggregated view rather than per process.

Types of data
=============

There are 3 types of metrics supported by speakeasy:

* COUNTER - Values received for COUNTER metrics are aggregated and **not** reset at each emission interval.
* GAUGE - Values received for GAUGE metrics are averaged and reset at each emission interval.
* PERCENTILE - 75th, 95th, and 99th percentiles are calculated based on the values received during each emission interval and
handed off to the emitter as GAUGE metrics.

Emitters
========

After each emission interval, defined by the user, speakeasy will perform the necessary aggregation over the values received
during the period and call the loaded emitter object. The emitter object is responsible for sending the metrics to central
collection.

A simple emitter is provided for example that will write metrics out to a configuarable file.

Running speakeasy
=================

See help for command line arguments
> speakeasy --help

The following command will start a speakeasy server listening to metrics on /var/tmp/metrics_socket. It will listen for admin commands
(not yet implemented) on port 9839 and publish all incoming data to port 9840. Every 60 seconds (emitter-interval flag), speakeasy
will process the received metrics and send them to the 'simple' emitter, which will write the metrics out to /var/tmp/metrics.out
> speakeasy --metric-socket /var/tmp/metrics_socket --cmd-port 9839 --pub-port 9840 --emitter simple --emitter-args filename=/var/tmp/metrics.out --emission-interval 60

Sending data to speakeasy
=========================

See python client, [rumrunner](http://www.github.com/etdub/rumrunner)

Reading metrics in real-time from speakeasy
===========================================

See python client, [imbibe](http://www.github.com/etdub/imbibe)

Installation
============

Speakeasy is available from [PyPI](https://pypi.python.org/pypi/speakeasy)
> pip install speakeasy

Requirements
============

The following libraries are required for speakeasy:

* argparse
* pyzmq
* ujson

Testing
=======

TBD
