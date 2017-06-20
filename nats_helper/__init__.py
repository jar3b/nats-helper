import asyncio
import signal
import time

from nats.aio.client import Client as NatsClient


class NatsHelper(object):
    _nc = None
    _loop = None
    _log = None
    _name = None

    def __init__(self, loop, logger, name=None):
        """
        :param loop: Asyncio event loop 
        :param logger: logger instance, logging.getLogger(...)
        :param name: Client name
        """
        self._name = name
        self._nc = NatsClient()
        self._loop = loop
        self._log = logger

    @property
    def connected(self):
        return self._nc.is_connected

    async def _connect(self, *args, **kwargs):
        # callbacks
        async def error_cb(e):
            self._log.error("NATS error: %s" % str(e))

        async def close_cb():
            self._log.warning("connection to NATS closed.")
            self.shutdown()

        async def disconnected_cb():
            self._log.error("NATS disconnected!")

        async def reconnected_cb():
            self._log.info("NATS reconnected!")

        options = {
            'name': self._name,
            'servers': ['nats://{username}:{password}@{host}:{port}'.format(**kwargs)],
            'io_loop': self._loop,
            'closed_cb': close_cb,
            'reconnected_cb': reconnected_cb,
            'error_cb': error_cb,
            'disconnected_cb': disconnected_cb
        }

        await self._nc.connect(**options)

    def connect(self, *args, **kwargs):
        """
        :param args: not used
        :param kwargs:
         username - NATS username
         password - NATS password
         host - NATS hostname (domain or IP)
         port - NATS port (without default, must be specified) 
        """
        self._loop.run_until_complete(self._connect(*args, **kwargs))

    async def _subscribe(self, *args, **kwargs):
        await self._nc.subscribe(*args, **kwargs, is_async=True)

    def subscribe(self, *args, **kwargs):
        orig_sub = kwargs['cb']

        async def subscriber(msg):
            await orig_sub(msg, self)

        kwargs['cb'] = subscriber
        self._loop.run_until_complete(self._subscribe(*args, **kwargs))

    def start(self):
        for sig in ("SIGHUP", "SIGTERM", "SIGINT"):
            self._loop.add_signal_handler(getattr(signal, sig), self.shutdown)

        self._loop.run_forever()

    # Signal handler
    def shutdown(self):
        self._log.info("nats-helper closing...")
        if self._nc.is_closed:
            return

        time.sleep(1)
        self._loop.stop()
        self._log.info("loop stopped, wait 1s for shutdown...")
        time.sleep(1)
        self._loop.create_task(self._nc.close())
        self._log.info("bye!")

    # publish functions
    def timed_request(self, *args, **kwargs):
        return self._loop.run_until_complete(self._nc.timed_request(*args, **kwargs))

    async def publish_async(self, *args, **kwargs):
        return await self._nc.publish(*args, **kwargs)

    def publish(self, *args, **kwargs):
        yield from self._nc.publish(*args, **kwargs)

    def publish_request(self, *args, **kwargs):
        yield from self._nc.publish_request(*args, **kwargs)