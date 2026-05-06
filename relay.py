import asyncio
import logging
import os

import websockets

try:
    import uvloop
except ImportError:
    uvloop = None


HOST = os.getenv("RELAY_HOST", "0.0.0.0")
PORT = int(os.getenv("RELAY_PORT", "80"))
AGENT_QUEUE_SIZE = int(os.getenv("RELAY_AGENT_QUEUE_SIZE", "1024"))
WS_MAX_QUEUE = int(os.getenv("RELAY_WS_MAX_QUEUE", "64"))
PAIR_TIMEOUT = float(os.getenv("RELAY_PAIR_TIMEOUT", "15"))
PING_INTERVAL = float(os.getenv("RELAY_PING_INTERVAL", "20"))
PING_TIMEOUT = float(os.getenv("RELAY_PING_TIMEOUT", "20"))
OPEN_TIMEOUT = float(os.getenv("RELAY_OPEN_TIMEOUT", "20"))
CLOSE_TIMEOUT = float(os.getenv("RELAY_CLOSE_TIMEOUT", "3"))

agents = asyncio.Queue(maxsize=AGENT_QUEUE_SIZE)
active_pairs = 0
total_pairs = 0


def configure_logging():
    kwargs = {
        "level": os.getenv("RELAY_LOG_LEVEL", "INFO"),
        "format": "%(asctime)s %(levelname)s %(message)s",
    }
    log_file = os.getenv("RELAY_LOG_FILE", "")
    if log_file:
        kwargs["filename"] = log_file
    logging.basicConfig(**kwargs)


def is_open(ws):
    if getattr(ws, "closed", False):
        return False
    if getattr(ws, "close_code", None) is not None:
        return False
    state = getattr(ws, "state", None)
    if getattr(state, "name", "OPEN") != "OPEN":
        return False
    return True


async def put_agent(ws):
    try:
        await asyncio.wait_for(agents.put(ws), timeout=PAIR_TIMEOUT)
        return True
    except asyncio.TimeoutError:
        return False


async def get_live_agent():
    deadline = asyncio.get_running_loop().time() + PAIR_TIMEOUT
    while True:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise asyncio.TimeoutError

        agent = await asyncio.wait_for(agents.get(), timeout=remaining)
        if is_open(agent):
            return agent


async def pipe(src, dst):
    async for msg in src:
        await dst.send(msg)


async def close_pair(client, agent):
    await asyncio.gather(
        client.close(),
        agent.close(),
        return_exceptions=True,
    )


async def pair(client, agent):
    global active_pairs, total_pairs

    active_pairs += 1
    total_pairs += 1
    pair_id = total_pairs
    logging.info("paired #%d active=%d queued_agents=%d", pair_id, active_pairs, agents.qsize())

    t1 = asyncio.create_task(pipe(client, agent))
    t2 = asyncio.create_task(pipe(agent, client))
    try:
        await asyncio.wait([t1, t2], return_when=asyncio.FIRST_COMPLETED)
    finally:
        for task in (t1, t2):
            task.cancel()
        await asyncio.gather(t1, t2, return_exceptions=True)
        await close_pair(client, agent)
        active_pairs -= 1
        logging.info("pair #%d closed active=%d", pair_id, active_pairs)


async def handler(ws):
    path = ws.request.path

    if path == "/agent":
        if not await put_agent(ws):
            logging.warning("agent queue full; rejecting")
            await ws.close(code=1013, reason="agent queue full")
            return

        logging.info("agent ready queued=%d", agents.qsize())
        await ws.wait_closed()
        return

    if path == "/client":
        try:
            agent = await get_live_agent()
        except asyncio.TimeoutError:
            logging.warning("no agent available")
            await ws.close(code=1013, reason="no agent")
            return

        await pair(ws, agent)
        return

    await ws.send("relay alive")
    await ws.close()


async def stats():
    while True:
        await asyncio.sleep(10)
        logging.info("stats active_pairs=%d total_pairs=%d queued_agents=%d", active_pairs, total_pairs, agents.qsize())


async def main():
    configure_logging()
    asyncio.create_task(stats())
    logging.info("relay listening on %s:%d queue=%d ws_max_queue=%d", HOST, PORT, AGENT_QUEUE_SIZE, WS_MAX_QUEUE)
    async with websockets.serve(
        handler,
        HOST,
        PORT,
        max_size=None,
        max_queue=WS_MAX_QUEUE,
        compression=None,
        ping_interval=PING_INTERVAL,
        ping_timeout=PING_TIMEOUT,
        open_timeout=OPEN_TIMEOUT,
        close_timeout=CLOSE_TIMEOUT,
    ):
        await asyncio.Future()


if __name__ == "__main__":
    if uvloop is not None:
        uvloop.install()
    asyncio.run(main())
