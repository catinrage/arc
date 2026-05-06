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
        relay.pools = {
            relay.LEGACY_AGENT_PATH: relay.AgentPool(relay.AGENT_QUEUE_SIZE),
            relay.MUX_AGENT_PATH: relay.AgentPool(relay.AGENT_QUEUE_SIZE),
        }

    async def test_get_live_agent_skips_closed(self):
        pool = relay.AgentPool(10)
        closed = FakeWebSocket(closed=True)
        live = FakeWebSocket()
        await pool.put(closed)
        await pool.put(live)

        got = await relay.get_live_agent(pool)
        self.assertIs(got, live)

    async def test_put_agent_times_out_when_full(self):
        pool = relay.AgentPool(1)
        await pool.put(FakeWebSocket())
        with mock.patch.object(relay, "PAIR_TIMEOUT", 0.001):
            ok = await relay.put_agent(pool, FakeWebSocket())
        self.assertFalse(ok)

    async def test_remove_unpaired_agent(self):
        pool = relay.AgentPool(10)
        agent = FakeWebSocket()
        await pool.put(agent)
        self.assertEqual(pool.qsize(), 1)
        await pool.remove(agent)
        self.assertEqual(pool.qsize(), 0)

    async def test_paths_use_separate_pools(self):
        legacy_name, legacy_pool = relay.pool_for_client_path(relay.LEGACY_CLIENT_PATH)
        mux_name, mux_pool = relay.pool_for_client_path(relay.MUX_CLIENT_PATH)

        self.assertEqual(legacy_name, relay.LEGACY_AGENT_PATH)
        self.assertEqual(mux_name, relay.MUX_AGENT_PATH)
        self.assertIsNot(legacy_pool, mux_pool)

    async def test_close_pair_closes_both(self):
        client = FakeWebSocket()
        agent = FakeWebSocket()
        await relay.close_pair(client, agent)
        self.assertTrue(client.closed)
        self.assertTrue(agent.closed)


if __name__ == "__main__":
    unittest.main()
