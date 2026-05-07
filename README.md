# Multiplexed WebSocket SOCKS5 Relay Tunnel

This project runs a SOCKS5 tunnel through a public Python WebSocket relay.

The relay remains Python. The gateway and agent are Go binaries that share a framed multiplexing protocol, so each long-lived WebSocket can carry many logical TCP streams instead of burning one WebSocket per SOCKS connection.

## Components

- `relay.py`
  - Python public relay.
  - Pairs one `/client-v2` WebSocket with one `/agent-v2` WebSocket for the Go multiplexed protocol.
  - Keeps legacy `/client` and `/agent` pools separate so old agents cannot be paired with the Go gateway.
  - Forwards binary messages between the pair.
  - Uses bounded queues and configurable timeouts.

- `cmd/gateway`
  - Go SOCKS5 server for the Iran gateway.
  - Keeps several persistent `/client-v2` WebSockets open.
  - Multiplexes local SOCKS TCP streams over those sessions.

- `cmd/agent`
  - Go Germany-side egress agent.
  - Keeps several persistent `/agent-v2` WebSockets open.
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
  "relay_url": "wss://your-relay.example.com/client-v2",
  "listen_host": "127.0.0.1",
  "listen_port": 1080,
  "connections": 32,
  "burst_connections": 96,
  "max_streams_per_session": 1,
  "buffer_size": 65536,
  "open_timeout": "10s",
  "relay_handshake_timeout": "30s",
  "connect_ramp_interval": "500ms",
  "log_file": "arc-gateway.log",
  "log_level": "info"
}
```

For maximum throughput, keep `max_streams_per_session` at `1`. `connections` are always-on lanes; `burst_connections` are temporary one-shot lanes used when all always-on lanes are busy. This avoids multiplexing head-of-line blocking while still handling browser-style connection spikes.

## Agent

Edit `agent.example.json`, then run:

```bash
./bin/arc-agent -config agent.example.json
```

Important config:

```json
{
  "relay_url": "wss://your-relay.example.com/agent-v2",
  "connections": 128,
  "buffer_size": 65536,
  "target_connect_timeout": "10s",
  "relay_handshake_timeout": "30s",
  "connect_ramp_interval": "500ms",
  "log_file": "arc-agent.log",
  "log_level": "info"
}
```

Run enough agent connections for both gateway steady lanes and burst lanes. For the default highway profile, gateway uses `32 + 96`, so agent uses `128`.

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

If relay handshakes time out at the CDN, reduce `connections` temporarily to `2`, keep `relay_handshake_timeout` at `30s` or higher, and keep `connect_ramp_interval` at `500ms` or `1s`. The debug log will show the exact phase: TCP dial, TLS handshake, upgrade write, or upgrade read.

## X-UI / Xray

Arc gateway is currently a TCP SOCKS5 tunnel. Xray may send SOCKS5 `UDP ASSOCIATE` when DNS, UDP, or QUIC traffic is routed through a SOCKS outbound. The gateway will reject that with SOCKS reply `0x07` and log `unsupported socks command udp_associate(3)`.

For X-UI, use Arc only for TCP traffic and route UDP/DNS separately. In practice:

```json
{
  "tag": "arc-tcp",
  "protocol": "socks",
  "settings": {
    "servers": [
      {
        "address": "127.0.0.1",
        "port": 1080
      }
    ]
  }
}
```

Then avoid routing UDP network traffic to this outbound. If you need DNS through Arc, use TCP DNS or DoH/DoT over TCP instead of UDP DNS.

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

- One stream per persistent WebSocket: best raw throughput profile on lossy/CDN paths because streams do not share TCP congestion or head-of-line blocking.
- Go gateway and agent: lower endpoint CPU and memory while preserving many physical relay lanes.
- Bounded relay queues: lower p95/p99 latency under overload and prevents unbounded relay memory growth.
- `uvloop` on relay: typically `10%-30%` more Python relay event-loop throughput on Linux.
- OS tuning: `10%-50%` improvement when defaults were limiting accept queues, ephemeral ports, or TCP recovery.
