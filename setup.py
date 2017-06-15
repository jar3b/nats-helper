from setuptools import setup

setup(
    name='nats-helper',
    version='0.0.1',
    packages=['nats_helper'],
    url='https://github.com/jar3b/nats-helper',
    license='MIT',
    author='jar3b',
    author_email='hellotan@live.ru',
    description='Simple class-based helper for working with NATS in Python language',
    install_requires=[
        'asyncio==3.4.3',
        'asyncio-nats-client==0.5.0'
    ]
)
