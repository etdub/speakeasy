==========
Quickstart
==========

Running speakeasy
-----------------

See help for command line arguments

.. code-block:: bash

    speakeasy --help

The following command will start a speakeasy server listening to metrics on
``/var/tmp/metrics_socket``. It will listen for admin commands (not yet
implemented) on port 9839 and publish all incoming data to port 9840. Every 60
seconds (emitter-interval flag), speakeasy will process the received metrics
and send them to the 'simple' emitter, which will write the metrics out to
``/var/tmp/metrics.out``.

.. code-block:: bash

    speakeasy --metric-socket /var/tmp/metrics_socket --cmd-port 9839 --pub-port 9840 --emitter simple --emitter-args filename=/var/tmp/metrics.out --emission-interval 60


Sending data to speakeasy
-------------------------

See python client rumrunner_.

.. _rumrunner: http://www.github.com/etdub/rumrunner


Reading metrics in real-time from speakeasy
-------------------------------------------

See python client imbibe_.

.. _imbibe: http://www.github.com/etdub/imbibe


Installation
------------

Speakeasy is available from PyPI_.

.. _PyPI: https://pypi.python.org/pypi/speakeasy

.. code-block:: bash

    pip install speakeasy


Testing
-------

.. code-block:: bash

    tox
