import asyncio
import json

import logging
from nats.aio.client import Msg

from nats_helper import NatsHelper


def to_json(entity):
    return json.dumps(entity).encode('utf-8')


def wrap_exception(e):
    return to_json({'error': '%s: %s' % (e.__class__, str(e))})


# SUBSCRIBERS
async def client_echo_sub(msg: Msg, nats_server):
    # All instances of program will answering to this message
    await nats_server.publish_async(msg.reply, nats_server.id)


async def get_info_sub(msg: Msg, nats_server):
    # Only one instance will send a answer
    try:
        await nats_server.publish_async(msg.reply, to_json({'msg': 'Hello'}))
    except Exception as e:
        await nats_server.publish_async(msg.reply, wrap_exception(e))


# MAIN CODE
def start():
    log = logging.getLogger(__name__)

    log.info("starting...")
    loop = asyncio.get_event_loop()
    nats_server = NatsHelper(loop, log, {'username': 'admin', 'password': 'pass', 'host': 'localhost', 'port': '4221'})
    # Can manual connect on auto-connect if pub-sub operation executed
    #nats_server.connect(username='admin', password='pass', host='localhost', port='4221')
    nats_server.subscribe("test.echo", cb=client_echo_sub)
    nats_server.subscribe("test.info", cb=get_info_sub, one_of=True)
    log.info("started")
    nats_server.start()


if __name__ == '__main__':
    start()
