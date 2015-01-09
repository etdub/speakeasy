speakeasy
=========

[![Build Status](https://travis-ci.org/etdub/speakeasy.png?branch=master)](https://travis-ci.org/etdub/speakeasy)
[![Coverage Status](https://coveralls.io/repos/etdub/speakeasy/badge.png)](https://coveralls.io/r/etdub/speakeasy)
[![Docs Status](https://readthedocs.org/projects/speakeasy/badge/?version=latest&style=not-flat)](http://speakeasy.readthedocs.org)
[![PyPI version](https://badge.fury.io/py/speakeasy.svg)](http://badge.fury.io/py/speakeasy)

Speakeasy is a metrics aggregation server that listens for data locally over a
named pipe. Received data is periodically aggregated, formatted, and shipped
off to central metrics collectors through the pluggable emitter system.

Speakeasy also has the abilility to publish the data it receives it for
possible real-time analysis.
