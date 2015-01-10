from setuptools import setup
import speakeasy

setup(
    name='speakeasy',
    version=speakeasy.__version__,
    description='Metrics aggregation server',
    author='Eric Wong',
    install_requires=[
        'argparse',
        'pyzmq',
        'ujson',
    ],
    packages=['speakeasy', 'speakeasy.emitter'],
    scripts=['bin/speakeasy'],
    url='https://github.com/etdub/speakeasy',
    classifiers=(
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
    ),
)
