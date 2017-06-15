import signal
import time
from nats.aio.client import Client as NatsClient


class NatsHelper(object):
    _nc = None
    _loop = None
    _log = None

    def __init__(self, loop, logger):
        """
        :param loop: Asyncio event loop 
        :param logger: logger instance, logging.getLogger(...)
        """
        self.nc = NatsClient()
        self._loop = loop
        self.log = logger

    @property
    def connected(self):
        return self._nc.is_connected

    async def _connect(self, *args, **kwargs):
        await self._nc.connect(io_loop=self._loop,
                              servers=['nats://{username}:{password}@{host}:{port}'.format(**kwargs)])

    def connect(self, *args, **kwargs):
        """
        :param args: not used
        :param kwargs:
         username - NATS username
         password - NATS password
         host - NATS hostname (domain or IP)
         port - NATS port (without default, must be specified) 
        :return: 
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
        self._loop.add_signal_handler(signal.SIGHUP, self.shutdown)
        self._loop.add_signal_handler(signal.SIGTERM, self.shutdown)
        self._loop.add_signal_handler(signal.SIGINT, self.shutdown)

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
