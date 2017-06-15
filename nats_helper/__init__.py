import signal
import time
from nats.aio.client import Client as NatsClient


class NatsHelper(object):
    id = None
    nc = None
    loop = None
    log = None

    def __init__(self, loop, logger):
        """
        :param loop: Asyncio event loop 
        :param logger: logger instance, logging.getLogger(...)
        """
        self.id = str(id(self)).encode("ASCII")
        self.nc = NatsClient()
        self.loop = loop
        self.log = logger

    async def _connect(self, *args, **kwargs):
        await self.nc.connect(io_loop=self.loop,
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
        self.loop.run_until_complete(self._connect(*args, **kwargs))

    async def _subscribe(self, *args, **kwargs):
        await self.nc.subscribe(*args, **kwargs, is_async=True)

    def subscribe(self, subject, cb, one_of=False):
        if one_of:
            subject = "%s.%s" % (self.id.decode("ASCII"), subject)

        async def subscriber(msg):
            await cb(msg, self)

        self.loop.run_until_complete(self._subscribe(subject, cb=subscriber))

    def start(self):
        self.loop.add_signal_handler(signal.SIGHUP, self.shutdown)
        self.loop.add_signal_handler(signal.SIGTERM, self.shutdown)
        self.loop.add_signal_handler(signal.SIGINT, self.shutdown)

        self.loop.run_forever()

    # Signal handler
    def shutdown(self):
        self.log.info("nats-helper closing...")
        if self.nc.is_closed:
            return

        time.sleep(1)
        self.loop.stop()
        self.log.info("loop stopped, wait 1s for shutdown...")
        time.sleep(1)
        self.loop.create_task(self.nc.close())
        self.log.info("bye!")
