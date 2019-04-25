"""The simple emitter writes metrics every interval to a file.

To use this emitter provide the emitter arg `filename` to write to. If an arg is not provided,
this emitter will attempt to write to `metrics.out` barring any permissions issue.

.. code-block:: bash

    speakeasy ... --emitter simple --emitter-args filename=/var/tmp/metrics.out

The special names `stdout` or `stderr` will send outputs to the appropriate stream
"""
from __future__ import absolute_import
import logging
import sys

logger = logging.getLogger(__name__)

DEFAULT_FILENAME = "metrics.out"
DEFAULT_SEPARATOR = "|"


class Emitter(object):

    def __init__(self, **kwargs):
        self.filename = kwargs.get('filename', DEFAULT_FILENAME)
        self.separator = kwargs.get('separator', DEFAULT_SEPARATOR)

    def emit(self, metrics):
        fh = None
        if self.filename == "stderr":
            fh = sys.stderr
        elif self.filename == "stdout":
            fh = sys.stdout

        if fh:
            self._emit(fh, metrics)
        else:
            with open(self.filename, 'a') as fh:
                self._emit(fh, metrics)

    def _emit(self, fh, metrics):
        """ Ship the metrics off """
        for metric in metrics:
            mline = self.separator.join([str(m) for m in metric])
            fh.write(mline + '\n')
