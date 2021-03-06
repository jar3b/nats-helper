import asyncio
import signal
import time

from nats.aio.client import Client as NatsClient


def require_connect_async(func):
    async def wrapper(self, *args, **kwargs):
        if not self.connected and self._connect_params is not None:
            await self.connect_async(**self._connect_params)
        return await func(self, *args, **kwargs)

    return wrapper


def require_connect_threadsafe(func):
    def wrapper(self, *args, **kwargs):
        if not self.connected and self._connect_params is not None:
            asyncio.run_coroutine_threadsafe(self.connect_async(**self._connect_params), self._loop).result()

        return func(self, *args, **kwargs)

    return wrapper


class NatsHelper(object):
    __slots__ = ['_nc', '_loop', '_log', '_name', '_connect_params', '_run_exclusive', '_reconnect_count',
                 '_reconnect_timeout', '_subscriptions', '_threaded']

    def __init__(self, loop, logger, name=None, connect_params=None, reconnect_count=10, reconnect_timeout=5):
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

        self._reconnect_count = reconnect_count
        self._reconnect_timeout = reconnect_timeout
        self._run_exclusive = None
        self._threaded = False

        self._subscriptions = {}

    @property
    def connected(self):
        return self._nc.is_connected

    @property
    def threaded(self):
        return self._threaded

    @threaded.setter
    def threaded(self, value):
        self._threaded = value

    async def connect_async(self, *args, **kwargs):
        # callbacks
        async def error_cb(e):
            self._log.error("NATS error: %s" % str(e))

        async def close_cb():
            self._log.warning("connection to NATS closed.")

            if self._nc.is_closed:
                return

            if self._reconnect_count != 0 and self._connect_params is not None:
                for i in range(1, self._reconnect_count):
                    try:
                        self._log.info("Try to reconnecting, %d of %d..." % (i, self._reconnect_count))
                        await self.connect_async(**self._connect_params)
                    except Exception as e:
                        self._log.info("Cannot auto-reconnect: %r, sleeping for %ds" % (e, self._reconnect_timeout))
                        await asyncio.sleep(self._reconnect_timeout)
                    else:
                        self._log.info("Reconnected over %d try!" % i)
                        for sub_name, sub_params in self._subscriptions.items():
                            self._log.info("Resubscribe for %s" % sub_name)
                            await self._subscribe(*sub_params[0], **sub_params[1])
                        return
            else:
                self.shutdown('')

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

        self._connect_params = kwargs
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
        self._subscriptions[args[0]] = (args, kwargs)
        await self._nc.subscribe(*args, **kwargs, is_async=True)

    def subscribe(self, *args, **kwargs):
        orig_sub = kwargs['cb']

        async def subscriber(msg):
            await orig_sub(msg, self)

        kwargs['cb'] = subscriber
        self._loop.run_until_complete(self._subscribe(*args, **kwargs))

    def start(self, exclusive=True, use_sig=True):
        if use_sig:
            for sig in ("SIGHUP", "SIGTERM", "SIGINT"):
                self._loop.add_signal_handler(getattr(signal, sig), self.shutdown, sig)

        self._run_exclusive = exclusive
        if exclusive:
            self._loop.run_forever()

    # Signal handler
    def shutdown(self, sig):
        if self._run_exclusive is None:
            self._log.info("nats-helper isn't started!")
            return

        if not self._nc.is_closed:
            if self._threaded:
                asyncio.run_coroutine_threadsafe(self._nc.close(), self._loop)
            else:
                self._reconnect_count = 0
                try:
                    asyncio.get_event_loop().run_until_complete(self._nc.close())
                except:
                    pass
        else:
            self._log.info("nats-helper was already closed!")

        if self._loop.is_running():
            self._log.info("nats-helper closing...")
            time.sleep(1)
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._log.info("loop stopped, wait 1s for shutdown...")
            time.sleep(1)
            self._run_exclusive = None

        self._log.info("bye!")

    # timed_request
    @require_connect_threadsafe
    def timed_request_threadsafe(self, *args, **kwargs):
        return asyncio.run_coroutine_threadsafe(self._nc.timed_request(*args, **kwargs), self._loop).result()

    @require_connect_async
    async def timed_request_async(self, *args, **kwargs):
        return await self._nc.timed_request(*args, **kwargs)

    # publish
    @require_connect_threadsafe
    def publish_threadsafe(self, *args, **kwargs):
        return asyncio.run_coroutine_threadsafe(self._nc.publish(*args, **kwargs), self._loop).result()

    @require_connect_async
    async def publish_async(self, *args, **kwargs):
        return await self._nc.publish(*args, **kwargs)

    # publish_request
    @require_connect_threadsafe
    def publish_request_threadsafe(self, *args, **kwargs):
        return asyncio.run_coroutine_threadsafe(self._nc.publish_request(*args, **kwargs), self._loop).result()

    @require_connect_async
    async def publish_request_async(self, *args, **kwargs):
        return await self._nc.publish_request(*args, **kwargs)
