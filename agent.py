import asyncio
import websockets
import socket
import time

RELAY = "wss://ciyn-4f0b00602d-rain.apps.ir-central1.arvancaas.ir/agent"

MIN_POOL = 10
MAX_POOL = 100
RAMP_STEP = 5
RAMP_INTERVAL = 3

CONNECT_TIMEOUT = 10
BUFFER_SIZE = 65536

started_workers = 0

async def pipe_reader_to_ws(reader, ws):
    try:
        while data := await reader.read(BUFFER_SIZE):
            await ws.send(data)
    except Exception:
        pass

async def pipe_ws_to_writer(ws, writer):
    try:
        async for msg in ws:
            if isinstance(msg, str):
                msg = msg.encode()
            writer.write(msg)
            await writer.drain()
    except Exception:
        pass

async def worker(i):
    while True:
        try:
            print(f"agent {i}: connecting to relay")

            async with websockets.connect(
                RELAY,
                max_size=None,
                max_queue=512,
                compression=None,
                ping_interval=30,
                ping_timeout=30,
                open_timeout=60,
                close_timeout=3,
            ) as ws:
                print(f"agent {i}: ready")

                msg = await ws.recv()
                if isinstance(msg, bytes):
                    msg = msg.decode()

                parts = msg.strip().split()
                if len(parts) != 3 or parts[0] != "CONNECT":
                    print(f"agent {i}: bad command: {msg}")
                    continue

                host = parts[1]
                port = int(parts[2])

                print(f"agent {i}: connecting to {host}:{port}")

                try:
                    reader, writer = await asyncio.wait_for(
                        asyncio.open_connection(host, port),
                        timeout=CONNECT_TIMEOUT,
                    )

                    sock = writer.get_extra_info("socket")
                    if sock:
                        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

                    await ws.send("OK")
                    print(f"agent {i}: connected {host}:{port}")

                except Exception as e:
                    print(f"agent {i}: target failed {host}:{port}: {e}")
                    await ws.send(f"ERR {e}")
                    continue

                t1 = asyncio.create_task(pipe_reader_to_ws(reader, ws))
                t2 = asyncio.create_task(pipe_ws_to_writer(ws, writer))

                done, pending = await asyncio.wait(
                    [t1, t2],
                    return_when=asyncio.FIRST_COMPLETED,
                )

                for t in pending:
                    t.cancel()

                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    pass

                print(f"agent {i}: session closed")

        except Exception as e:
            print(f"agent {i}: relay error: {e}")

        await asyncio.sleep(0.5)

async def ramp_workers():
    global started_workers

    while started_workers < MIN_POOL:
        asyncio.create_task(worker(started_workers))
        started_workers += 1
        await asyncio.sleep(0.2)

    print(f"started minimum pool: {started_workers}")

    while started_workers < MAX_POOL:
        await asyncio.sleep(RAMP_INTERVAL)

        for _ in range(RAMP_STEP):
            if started_workers >= MAX_POOL:
                break

            asyncio.create_task(worker(started_workers))
            started_workers += 1
            await asyncio.sleep(0.2)

        print(f"ramped agents: {started_workers}/{MAX_POOL}")

async def stats():
    while True:
        await asyncio.sleep(10)
        print(f"stats: started_workers={started_workers}, max={MAX_POOL}")

async def main():
    print(
        f"GE agent ramping: min={MIN_POOL}, max={MAX_POOL}, "
        f"step={RAMP_STEP}, interval={RAMP_INTERVAL}s"
    )

    await asyncio.gather(
        ramp_workers(),
        stats(),
    )

asyncio.run(main())
