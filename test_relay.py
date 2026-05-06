import asyncio
import sys
import types
import unittest
from unittest import mock

sys.modules.setdefault("websockets", types.SimpleNamespace(serve=None))

import relay


class FakeWebSocket:
    def __init__(self, closed=False):
        self.closed = closed
        self.close_calls = 0

    async def close(self, *args, **kwargs):
        self.close_calls += 1
        self.closed = True


class RelayTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        while not relay.agents.empty():
            relay.agents.get_nowait()

    async def test_get_live_agent_skips_closed(self):
        closed = FakeWebSocket(closed=True)
        live = FakeWebSocket()
        await relay.agents.put(closed)
        await relay.agents.put(live)

        got = await relay.get_live_agent()
        self.assertIs(got, live)

    async def test_put_agent_times_out_when_full(self):
        with mock.patch.object(relay, "agents", asyncio.Queue(maxsize=1)):
            await relay.agents.put(FakeWebSocket())
            with mock.patch.object(relay, "PAIR_TIMEOUT", 0.001):
                ok = await relay.put_agent(FakeWebSocket())
        self.assertFalse(ok)

    async def test_close_pair_closes_both(self):
        client = FakeWebSocket()
        agent = FakeWebSocket()
        await relay.close_pair(client, agent)
        self.assertTrue(client.closed)
        self.assertTrue(agent.closed)


if __name__ == "__main__":
    unittest.main()
