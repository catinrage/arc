import asyncio
import websockets

agents = asyncio.Queue()

async def pipe(a, b):
    try:
        async for msg in a:
            await b.send(msg)
    except Exception:
        pass

async def handler(ws):
    path = ws.request.path
    print("new connection path:", path)

    if path == "/agent":
        print("GE agent connected")
        await agents.put(ws)
        print("agent queue:", agents.qsize())
        await ws.wait_closed()
        print("GE agent closed")
        return

    if path == "/client":
        print("IR client connected, queue:", agents.qsize())

        try:
            agent = await asyncio.wait_for(agents.get(), timeout=60)
        except asyncio.TimeoutError:
            print("no agent available")
            await ws.close(code=1013, reason="no agent")
            return

        print("paired IR <-> GE")

        t1 = asyncio.create_task(pipe(ws, agent))
        t2 = asyncio.create_task(pipe(agent, ws))

        done, pending = await asyncio.wait([t1, t2], return_when=asyncio.FIRST_COMPLETED)

        for t in pending:
            t.cancel()

        await ws.close()
        await agent.close()
        print("pair closed")
        return

    await ws.send("relay alive")
    await ws.close()

async def main():
    print("relay listening on :80")
    async with websockets.serve(
        handler,
        "0.0.0.0",
        80,
        max_size=None,
        max_queue=None,
        compression=None,
        ping_interval=30,
        ping_timeout=30,
        open_timeout=60,
    ):
        await asyncio.Future()

asyncio.run(main())
