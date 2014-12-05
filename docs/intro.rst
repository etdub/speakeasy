============
Introduction
============

Why?
----

Speakeasy was created to help instrument a python webapp running with Apache
and mod_wsgi. Instead of sending metrics per Apache child to central metrics
collectors, the data needed to be aggregated on box and then sent out. This
allows tracking QPS, latency, error rate, etc. in an aggregated view rather
than per process.

Types of data
-------------

There are 3 types of metrics supported by speakeasy:

COUNTER
    Values received for COUNTER metrics are aggregated and **not** reset at
    each emission interval.

GAUGE
    Values received for GAUGE metrics are averaged and reset at each emission
    interval.

PERCENTILE
    75th, 95th, and 99th percentiles are calculated based on the values
    received during each emission interval and handed off to the emitter as
    GAUGE metrics.

Emitters
--------

After each emission interval, defined by the user, speakeasy will perform the
necessary aggregation over the values received during the period and call the
loaded emitter object. The emitter object is responsible for sending the
metrics to central collection.

A simple emitter is provided for example that will write metrics out to a
configuarable file.
