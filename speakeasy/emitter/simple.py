import os

class Emitter(object):
  def __init__(self, **kwargs):
    self.filename = kwargs['filename']

  def emit(self, metrics):
    """ Ship the metrics off """
    with open(self.filename, 'a') as fh:
      for metric in metrics:
        mline = '|'.join([str(m) for m in metric])
        fh.write(mline+'\n')
