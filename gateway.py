import asyncio
import socket
import struct
import websockets

RELAY = "wss://ciyn-4f0b00602d-rain.apps.ir-central1.arvancaas.ir/client"

LISTEN_HOST = "127.0.0.1"
LISTEN_PORT = 1080

BUFFER_SIZE = 65536
RELAY_CONNECT_RETRIES = 8
RELAY_RETRY_DELAY = 0.25
CONNECT_REPLY_TIMEOUT = 60

active = 0
total = 0

async def read_exact(reader, n):
    return await reader.readexactly(n)

async def pipe_local_to_ws(reader, ws):
    try:
        while True:
            data = await reader.read(BUFFER_SIZE)
            if not data:
                break
            await ws.send(data)
    except Exception:
        pass

async def pipe_ws_to_local(ws, writer):
    try:
        async for msg in ws:
            if isinstance(msg, str):
                msg = msg.encode()
            writer.write(msg)
            await writer.drain()
    except Exception:
        pass

async def connect_relay():
    last_error = None

    for attempt in range(RELAY_CONNECT_RETRIES):
        try:
            return await websockets.connect(
                RELAY,
                max_size=None,
                max_queue=512,
                compression=None,
                ping_interval=30,
                ping_timeout=30,
                open_timeout=30,
                close_timeout=3,
            )
        except Exception as e:
            last_error = e
            print(f"relay connect retry {attempt + 1}/{RELAY_CONNECT_RETRIES}: {e}")
            await asyncio.sleep(RELAY_RETRY_DELAY)

    raise last_error

async def socks_fail(writer, code=b"\x05"):
    try:
        writer.write(b"\x05" + code + b"\x00\x01\x00\x00\x00\x00\x00\x00")
        await writer.drain()
    except Exception:
        pass

async def handle(reader, writer):
    global active, total

    active += 1
    total += 1
    conn_id = total

    try:
        sock = writer.get_extra_info("socket")
        if sock:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

        ver, nmethods = await read_exact(reader, 2)
        methods = await read_exact(reader, nmethods)

        if ver != 5:
            return

        writer.write(b"\x05\x00")
        await writer.drain()

        ver, cmd, rsv, atyp = await read_exact(reader, 4)

        if ver != 5 or cmd != 1:
            await socks_fail(writer, b"\x07")
            return

        if atyp == 1:
            host = socket.inet_ntoa(await read_exact(reader, 4))
        elif atyp == 3:
            ln = (await read_exact(reader, 1))[0]
            host = (await read_exact(reader, ln)).decode()
        elif atyp == 4:
            host = socket.inet_ntop(socket.AF_INET6, await read_exact(reader, 16))
        else:
            await socks_fail(writer, b"\x08")
            return

        port = struct.unpack(">H", await read_exact(reader, 2))[0]

        print(f"#{conn_id} SOCKS {host}:{port} active={active}")

        ws = await connect_relay()

        async with ws:
            await ws.send(f"CONNECT {host} {port}")

            try:
                reply = await asyncio.wait_for(ws.recv(), timeout=CONNECT_REPLY_TIMEOUT)
            except asyncio.TimeoutError:
                print(f"#{conn_id} GE connect timeout")
                await socks_fail(writer, b"\x05")
                return

            if isinstance(reply, bytes):
                reply = reply.decode()

            if reply != "OK":
                print(f"#{conn_id} GE connect failed: {reply}")
                await socks_fail(writer, b"\x05")
                return

            writer.write(b"\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00")
            await writer.drain()

            t1 = asyncio.create_task(pipe_local_to_ws(reader, ws))
            t2 = asyncio.create_task(pipe_ws_to_local(ws, writer))

            done, pending = await asyncio.wait(
                [t1, t2],
                return_when=asyncio.FIRST_COMPLETED,
            )

            for t in pending:
                t.cancel()

    except Exception as e:
        print(f"#{conn_id} client error: {e}")

    finally:
        active -= 1
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass

async def stats():
    while True:
        await asyncio.sleep(10)
        print(f"stats active={active} total={total}")

async def main():
    asyncio.create_task(stats())

    server = await asyncio.start_server(
        handle,
        LISTEN_HOST,
        LISTEN_PORT,
        backlog=4096,
    )

    print(f"SOCKS5 listening on {LISTEN_HOST}:{LISTEN_PORT}")

    async with server:
        await server.serve_forever()

asyncio.run(main())
