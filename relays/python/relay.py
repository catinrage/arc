import asyncio
import logging
import os
from collections import deque

import websockets

try:
    import uvloop
except ImportError:
    uvloop = None


HOST = os.getenv("RELAY_HOST", "0.0.0.0")
PORT = int(os.getenv("RELAY_PORT", "80"))
AGENT_QUEUE_SIZE = int(os.getenv("RELAY_AGENT_QUEUE_SIZE", "1024"))
WS_MAX_QUEUE = int(os.getenv("RELAY_WS_MAX_QUEUE", "64"))
PAIR_TIMEOUT = float(os.getenv("RELAY_PAIR_TIMEOUT", "60"))
PING_INTERVAL = float(os.getenv("RELAY_PING_INTERVAL", "20"))
PING_TIMEOUT = float(os.getenv("RELAY_PING_TIMEOUT", "20"))
OPEN_TIMEOUT = float(os.getenv("RELAY_OPEN_TIMEOUT", "20"))
CLOSE_TIMEOUT = float(os.getenv("RELAY_CLOSE_TIMEOUT", "3"))

LEGACY_AGENT_PATH = "/agent"
LEGACY_CLIENT_PATH = "/client"
MUX_AGENT_PATH = "/agent-v2"
MUX_CLIENT_PATH = "/client-v2"
RAW_AGENT_PATH = "/agent-raw"
RAW_CLIENT_PATH = "/client-raw"

active_pairs = 0
total_pairs = 0


class AgentPool:
    def __init__(self, max_size):
        self.max_size = max_size
        self.items = deque()
        self.condition = asyncio.Condition()

    def qsize(self):
        return len(self.items)

    async def put(self, ws):
        async with self.condition:
            self._prune_locked()
            if len(self.items) >= self.max_size:
                return False
            self.items.append(ws)
            self.condition.notify()
            return True

    async def remove(self, ws):
        async with self.condition:
            try:
                self.items.remove(ws)
            except ValueError:
                return
            self.condition.notify()

    async def get(self, timeout):
        deadline = asyncio.get_running_loop().time() + timeout
        async with self.condition:
            while True:
                self._prune_locked()
                while self.items:
                    agent = self.items.popleft()
                    if is_open(agent):
                        return agent

                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    raise asyncio.TimeoutError
                await asyncio.wait_for(self.condition.wait(), timeout=remaining)

    def _prune_locked(self):
        self.items = deque(ws for ws in self.items if is_open(ws))


pools = {
    LEGACY_AGENT_PATH: AgentPool(AGENT_QUEUE_SIZE),
    MUX_AGENT_PATH: AgentPool(AGENT_QUEUE_SIZE),
    RAW_AGENT_PATH: AgentPool(AGENT_QUEUE_SIZE),
}


def pool_for_client_path(path):
    if path == LEGACY_CLIENT_PATH:
        return LEGACY_AGENT_PATH, pools[LEGACY_AGENT_PATH]
    if path == MUX_CLIENT_PATH:
        return MUX_AGENT_PATH, pools[MUX_AGENT_PATH]
    if path == RAW_CLIENT_PATH:
        return RAW_AGENT_PATH, pools[RAW_AGENT_PATH]
    return None, None


def pool_for_agent_path(path):
    return pools.get(path)


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


async def put_agent(pool, ws):
    try:
        return await asyncio.wait_for(pool.put(ws), timeout=PAIR_TIMEOUT)
    except asyncio.TimeoutError:
        return False


async def get_live_agent(pool):
    return await pool.get(PAIR_TIMEOUT)


async def pipe(src, dst):
    async for msg in src:
        await dst.send(msg)


async def close_pair(client, agent):
    await asyncio.gather(
        client.close(),
        agent.close(),
        return_exceptions=True,
    )


async def pair(client, agent, pool_name, pool):
    global active_pairs, total_pairs

    active_pairs += 1
    total_pairs += 1
    pair_id = total_pairs
    logging.info(
        "paired #%d pool=%s active=%d queued_agents=%d",
        pair_id,
        pool_name,
        active_pairs,
        pool.qsize(),
    )

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
        logging.info("pair #%d closed pool=%s active=%d", pair_id, pool_name, active_pairs)


async def handler(ws):
    path = ws.request.path
    agent_pool = pool_for_agent_path(path)

    if agent_pool is not None:
        if not await put_agent(agent_pool, ws):
            logging.warning("agent queue full path=%s; rejecting", path)
            await ws.close(code=1013, reason="agent queue full")
            return

        logging.info("agent ready path=%s queued=%d", path, agent_pool.qsize())
        try:
            await ws.wait_closed()
        finally:
            await agent_pool.remove(ws)
        return

    pool_name, client_pool = pool_for_client_path(path)
    if client_pool is not None:
        try:
            agent = await get_live_agent(client_pool)
        except asyncio.TimeoutError:
            logging.warning("no agent available path=%s pool=%s", path, pool_name)
            await ws.close(code=1013, reason="no agent")
            return

        await pair(ws, agent, pool_name, client_pool)
        return

    await ws.send("relay alive")
    await ws.close()


async def stats():
    while True:
        await asyncio.sleep(10)
        logging.info(
            "stats active_pairs=%d total_pairs=%d queued_legacy=%d queued_mux=%d queued_raw=%d",
            active_pairs,
            total_pairs,
            pools[LEGACY_AGENT_PATH].qsize(),
            pools[MUX_AGENT_PATH].qsize(),
            pools[RAW_AGENT_PATH].qsize(),
        )


async def main():
    configure_logging()
    asyncio.create_task(stats())
    logging.info(
        "relay listening on %s:%d queue=%d ws_max_queue=%d legacy=(%s,%s) mux=(%s,%s) raw=(%s,%s)",
        HOST,
        PORT,
        AGENT_QUEUE_SIZE,
        WS_MAX_QUEUE,
        LEGACY_AGENT_PATH,
        LEGACY_CLIENT_PATH,
        MUX_AGENT_PATH,
        MUX_CLIENT_PATH,
        RAW_AGENT_PATH,
        RAW_CLIENT_PATH,
    )
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
