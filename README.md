# Multiplexed WebSocket SOCKS5 Relay Tunnel

This project runs a SOCKS5 tunnel through a public Python WebSocket relay.

The relay remains Python. The gateway and agent are Go binaries that share a framed multiplexing protocol, so each long-lived WebSocket can carry many logical TCP streams instead of burning one WebSocket per SOCKS connection.

## Components

- `relay.py`
  - Python public relay.
  - Pairs one `/client` WebSocket with one `/agent` WebSocket.
  - Forwards binary messages between the pair.
  - Uses bounded queues and configurable timeouts.

- `cmd/gateway`
  - Go SOCKS5 server for the Iran gateway.
  - Keeps several persistent `/client` WebSockets open.
  - Multiplexes local SOCKS TCP streams over those sessions.

- `cmd/agent`
  - Go Germany-side egress agent.
  - Keeps several persistent `/agent` WebSockets open.
  - Receives logical `OPEN` frames, dials target TCP servers, and relays data.

## Protocol

Shared code lives in `internal/protocol` and `internal/mux`.

Each WebSocket binary message is:

```text
1 byte   message type
4 bytes  stream id
4 bytes  payload length
N bytes  payload
```

Message types:

```text
OPEN
OPEN_OK
DATA
CLOSE
ERROR
```

The gateway only sends SOCKS success after the agent has actually connected to the target and replied with `OPEN_OK`.

## Build

```bash
/usr/local/go/bin/go test ./...
/usr/local/go/bin/go build -o bin/arc-gateway ./cmd/gateway
/usr/local/go/bin/go build -o bin/arc-agent ./cmd/agent
```

## Relay

Install Python dependencies on the relay server:

```bash
python3 -m venv venv
. venv/bin/activate
pip install websockets uvloop
```

Run:

```bash
RELAY_PORT=80 RELAY_WS_MAX_QUEUE=64 RELAY_AGENT_QUEUE_SIZE=1024 python3 relay.py
```

Useful relay environment:

```text
RELAY_HOST=0.0.0.0
RELAY_PORT=80
RELAY_AGENT_QUEUE_SIZE=1024
RELAY_WS_MAX_QUEUE=64
RELAY_PAIR_TIMEOUT=15
RELAY_PING_INTERVAL=20
RELAY_PING_TIMEOUT=20
RELAY_OPEN_TIMEOUT=20
RELAY_CLOSE_TIMEOUT=3
RELAY_LOG_LEVEL=INFO
RELAY_LOG_FILE=
```

## Gateway

Edit `gateway.example.json`, then run:

```bash
./bin/arc-gateway -config gateway.example.json
```

Important config:

```json
{
  "relay_url": "wss://your-relay.example.com/client",
  "listen_host": "127.0.0.1",
  "listen_port": 1080,
  "connections": 8,
  "buffer_size": 65536,
  "open_timeout": "10s",
  "log_file": "arc-gateway.log",
  "log_level": "info"
}
```

Increase `connections` if one WebSocket pair becomes a bottleneck or if the relay is sharded.

## Agent

Edit `agent.example.json`, then run:

```bash
./bin/arc-agent -config agent.example.json
```

Important config:

```json
{
  "relay_url": "wss://your-relay.example.com/agent",
  "connections": 8,
  "buffer_size": 65536,
  "target_connect_timeout": "10s",
  "log_file": "arc-agent.log",
  "log_level": "info"
}
```

Run at least as many agent connections as gateway connections. Extra agent connections wait in the relay queue and get paired when gateway sessions reconnect.

## Service Manager

Release packages include `arc-agent`, `arc-gateway`, `relay.py`, config examples, and `service.sh`.

Initialize and start a systemd service:

```bash
./service.sh init agent
./service.sh init gateway
```

Manage one role:

```bash
./service.sh agent status
./service.sh agent restart
./service.sh gateway stop
./service.sh gateway enable
```

Manage all initialized roles:

```bash
./service.sh status
./service.sh restart
```

Update from the latest GitHub release, preserve local configs, and restart active services:

```bash
./service.sh update
```

Useful environment:

```text
ARC_UPDATE_REPO=catinrage/arc
ARC_UPDATE_INCLUDE_PRERELEASE=1
ARC_SERVICE_NAMESPACE=name
ARC_SERVICE_NONINTERACTIVE=1
SERVICE_USER=root
```

`init` expects binaries named `arc-agent` and `arc-gateway` in the same directory as `service.sh`. If `config.agent.json` or `config.gateway.json` is missing, it is created from the matching example file.

For debugging failed SOCKS connects, temporarily set both service configs to:

```json
{
  "log_level": "debug"
}
```

Then restart and inspect:

```bash
./service.sh agent restart
./service.sh gateway restart
tail -f arc-agent.log arc-gateway.log
```

## GitHub Release Pipeline

CI runs on pushes to `main`, pull requests, and manual dispatch. The release workflow runs on every commit to `main` and on `v*` tags.

Branch builds publish prereleases named like:

```text
0.0.<run-number>-<short-sha>
```

Release assets:

```text
arc_<version>_linux_amd64.tar.gz
arc_<version>_linux_amd64.tar.gz.sha256
relay.py
```

## Tests

Go:

```bash
/usr/local/go/bin/go test ./...
```

Relay helpers:

```bash
python3 -m unittest test_relay.py
```

## Network Tuning

Run on both gateway and agent servers:

```bash
sudo tee /etc/sysctl.d/99-arc-tunnel.conf >/dev/null <<'EOF'
net.core.somaxconn = 65535
net.core.netdev_max_backlog = 250000
net.ipv4.tcp_max_syn_backlog = 65535
net.ipv4.ip_local_port_range = 1024 65535
net.ipv4.tcp_tw_reuse = 1
net.ipv4.tcp_fin_timeout = 15
net.ipv4.tcp_keepalive_time = 30
net.ipv4.tcp_keepalive_intvl = 10
net.ipv4.tcp_keepalive_probes = 3
net.ipv4.tcp_congestion_control = bbr
net.core.default_qdisc = fq
EOF
sudo sysctl --system
```

Raise file descriptors for the service user:

```bash
ulimit -n 1048576
```

For systemd services:

```ini
[Service]
LimitNOFILE=1048576
```

Check whether BBR is available:

```bash
sysctl net.ipv4.tcp_available_congestion_control
sysctl net.ipv4.tcp_congestion_control
```

## Expected Impact

- Multiplexing over persistent WebSockets: biggest win, usually `2x-10x` better short-connection throughput and much lower connection setup latency.
- Go gateway and agent: usually `5%-30%` more end-to-end throughput if endpoints were CPU-bound, plus lower memory per connection.
- Bounded relay queues: lower p95/p99 latency under overload and prevents unbounded relay memory growth.
- `uvloop` on relay: typically `10%-30%` more Python relay event-loop throughput on Linux.
- OS tuning: `10%-50%` improvement when defaults were limiting accept queues, ephemeral ports, or TCP recovery.
