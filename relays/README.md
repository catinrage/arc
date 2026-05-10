# Arc Relays

Arc relays are intentionally interchangeable. Both implementations expose the same WebSocket paths:

```text
/agent
/client
/agent-v2
/client-v2
/agent-raw
/client-raw
```

Use `/agent-v2` and `/client-v2` for the default mux transport. Use `/agent-raw` and `/client-raw` for raw paired lanes, where each SOCKS TCP connection consumes one paired WebSocket lane.

Use the Python relay on Python container platforms:

```bash
RELAY_PORT=80 python3 relays/python/relay.py
```

Use the Go relay on Go container platforms:

```bash
go build -o bin/arc-relay-go ./relays/go
RELAY_PORT=80 ./bin/arc-relay-go
```

For a minimal manual Go container, copy only these files into one directory:

```text
go.mod
relays/go/main.go
```

Then run from that directory:

```bash
go build -o arc-relay-go .
RELAY_PORT=80 ./arc-relay-go
```

Gateway and agent configs do not change when switching relay implementations.

Both relays support the same environment names. For CDN/proxy stability, keep WebSocket pings enabled:

```text
RELAY_PING_INTERVAL=20
RELAY_PING_TIMEOUT=20
```
