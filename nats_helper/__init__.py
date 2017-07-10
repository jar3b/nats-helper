import signal
import time

from nats.aio.client import Client as NatsClient
import functools


def require_connect_async(func):
    async def wrapper(self, *args, **kwargs):
        if not self.connected and self._connect_params is not None:
            await self.connect_async(**self._connect_params)
        return await func(self, *args, **kwargs)

    return wrapper


def require_connect(func):
    def wrapper(self, *args, **kwargs):
        if not self.connected and self._connect_params is not None:
            self.connect(**self._connect_params)
        return func(self, *args, **kwargs)

    return wrapper


class NatsHelper(object):
    _nc = None
    _loop = None
    _log = None
    _name = None
    _connect_params = None
    _run_exclusive = None

    def __init__(self, loop, logger, name=None, connect_params=None):
        """
        :param loop: Asyncio event loop 
        :param logger: logger instance, logging.getLogger(...)
        :param name: Client name
        """
        self._name = name
        self._nc = NatsClient()
        self._loop = loop
        self._log = logger
        self._connect_params = connect_params

    @property
    def connected(self):
        return self._nc.is_connected

    async def connect_async(self, *args, **kwargs):
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
        self._loop.run_until_complete(self.connect_async(*args, **kwargs))

    @require_connect_async
    async def _subscribe(self, *args, **kwargs):
        await self._nc.subscribe(*args, **kwargs, is_async=True)

    def subscribe(self, *args, **kwargs):
        orig_sub = kwargs['cb']

        async def subscriber(msg):
            await orig_sub(msg, self)

        kwargs['cb'] = subscriber
        self._loop.run_until_complete(self._subscribe(*args, **kwargs))

    def start(self, exclusive=True):
        for sig in ("SIGHUP", "SIGTERM", "SIGINT"):
            self._loop.add_signal_handler(getattr(signal, sig), self.shutdown)

        self._run_exclusive = True
        if exclusive:
            self._loop.run_forever()

    # Signal handler
    def shutdown(self):
        if self._run_exclusive is None:
            self._log.info("nats-helper isn't started!")
            return

        if self._nc.is_closed:
            self._log.info("nats-helper was already closed!")
            return

        if self._run_exclusive:
            self._log.info("nats-helper closing...")
            time.sleep(1)
            self._loop.stop()
            self._log.info("loop stopped, wait 1s for shutdown...")
            time.sleep(1)
            self._run_exclusive = None

        self._loop.create_task(self._nc.close())
        self._log.info("bye!")

    # timed_request
    @require_connect
    def timed_request(self, *args, **kwargs):
        return self._loop.run_until_complete(self._nc.timed_request(*args, **kwargs))

    @require_connect
    def timed_request_soon(self, *args, **kwargs):
        return self._loop.call_soon(functools.partial(self._nc.timed_request, *args, **kwargs))

    @require_connect_async
    async def timed_request_async(self, *args, **kwargs):
        return await self._nc.timed_request(*args, **kwargs)

    # publish
    @require_connect
    def publish(self, *args, **kwargs):
        return self._loop.run_until_complete(self._nc.publish(*args, **kwargs))

    @require_connect
    def publish_soon(self, *args, **kwargs):
        return self._loop.call_soon(functools.partial(self._nc.publish, *args, **kwargs))

    @require_connect_async
    async def publish_async(self, *args, **kwargs):
        return await self._nc.publish(*args, **kwargs)

    # publish_request
    @require_connect
    def publish_request(self, *args, **kwargs):
        return self._loop.run_until_complete(self._nc.publish_request(*args, **kwargs))

    @require_connect
    def publish_request_soon(self, *args, **kwargs):
        return self._loop.call_soon(functools.partial(self._nc.publish_request, *args, **kwargs))

    @require_connect_async
    async def publish_request_async(self, *args, **kwargs):
        return await self._nc.publish_request(*args, **kwargs)
