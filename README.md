# knx-nats-bridge

Publish KNX telegrams to NATS JetStream with DPT decoding.

## What it does

Connects to a KNX/IP gateway (tunneling or routing), decodes each incoming
telegram using the KNX Data-Point-Type (DPT) declared in a user-provided
**KNX catalog** (group-address → name/DPT plus optional ETS metadata
`room`/`function`/`description`), and publishes the decoded value as JSON to
a NATS JetStream subject (`<prefix>.<main>.<middle>.<sub>`).

Native types are preserved on the wire: DPT 1.x → `bool`, DPT 5.x/7.x/13.x →
`int`, DPT 9.x/14.x → `float`. Consumers can cast as needed.

## Status

Alpha. Built for a personal homelab first, structured to be reusable.
No hardcoded values; everything comes from env vars, config files, or mounted
secrets.

## Install & run (Docker)

```sh
docker run --rm \
  -e KNX_CONNECTION_TYPE=tunneling_tcp \
  -e KNX_GATEWAY_HOST=192.0.2.10 \
  -e KNX_GATEWAY_PORT=3671 \
  -e NATS_SERVERS=nats://nats:4222 \
  -e NATS_SUBJECT_PREFIX=knx \
  -e KNX_NATS_CATALOG_PATH=/etc/knx-nats-bridge/knx-catalog.yaml \
  -v $(pwd)/knx-catalog.yaml:/etc/knx-nats-bridge/knx-catalog.yaml:ro \
  -v $(pwd)/nats.creds:/etc/knx-nats-bridge/nats.creds:ro \
  ghcr.io/alexander-zimmermann/knx-nats-bridge:latest
```

See [examples/docker-compose.yaml](examples/docker-compose.yaml) for a fuller
example and [examples/k8s/](examples/k8s/) for Kubernetes snippets.

## Configuration

All configuration comes from environment variables (pydantic-settings).
Secrets are read from files, never from env vars.

### KNX

| Var                        | Default                                 | Description                                       |
| -------------------------- | --------------------------------------- | ------------------------------------------------- |
| `KNX_CONNECTION_TYPE`      | `tunneling_tcp`                         | `tunneling_tcp`, `tunneling_udp`, or `routing`    |
| `KNX_GATEWAY_HOST`         | —                                       | Gateway IP or hostname (not needed for `routing`) |
| `KNX_GATEWAY_PORT`         | `3671`                                  |                                                   |
| `KNX_LOCAL_IP`             | —                                       | Optional, for multicast routing                   |
| `KNX_INDIVIDUAL_ADDRESS`   | —                                       | Optional, e.g. `1.1.250`                          |
| `KNX_SECURE_KEYRING_FILE`  | —                                       | Optional, path to `.knxkeys` for KNX/IP Secure    |
| `KNX_NATS_CATALOG_PATH`    | `/etc/knx-nats-bridge/knx-catalog.yaml` | KNX catalog file                                  |
| `KNX_NATS_UNMAPPED_POLICY` | `skip`                                  | `skip`, `warn`, or `raw`                          |

### NATS

| Var                       | Default                 | Description                                                 |
| ------------------------- | ----------------------- | ----------------------------------------------------------- |
| `NATS_SERVERS`            | `nats://localhost:4222` | Comma-separated NATS URLs                                   |
| `NATS_SUBJECT_PREFIX`     | `knx`                   | Subject prefix, `<prefix>.<a>.<b>.<c>`                      |
| `NATS_CREDS_FILE`         | —                       | NATS `.creds` file (JWT bundle, full operator/account auth) |
| `NATS_NKEY_SEED_FILE`     | —                       | NKey seed file (`SU…`), for decentralized nkey auth         |
| `NATS_USER`               | —                       | Username, for user/password auth                            |
| `NATS_USER_PASSWORD_FILE` | —                       | Path to password file, for user/password auth               |

Auth precedence: `NATS_CREDS_FILE` > `NATS_NKEY_SEED_FILE` > `NATS_USER` + `NATS_USER_PASSWORD_FILE`. Configure exactly one form; the others can stay unset.

### Observability

| Var            | Default | Description                             |
| -------------- | ------- | --------------------------------------- |
| `METRICS_PORT` | `9090`  | HTTP port for `/metrics` and `/healthz` |
| `LOG_LEVEL`    | `INFO`  |                                         |
| `LOG_FORMAT`   | `json`  | `json` or `text`                        |

## KNX catalog format

YAML file keyed by group address (`<main>/<middle>/<sub>`). Each entry has
two required fields (`name`, `dpt`) and three optional ones extracted from
ETS (`room`, `function`, `description`). Validated against
[src/knx_nats_bridge/\_schemas/knx-catalog.schema.json](src/knx_nats_bridge/_schemas/knx-catalog.schema.json)
at startup.

```yaml
# examples/knx-catalog.example.yaml
"1/2/3":
  name: "Beleuchtung.OG.Schlafzimmer.Decke.Schalten"
  dpt: "1.001"
  room: "Schlafzimmer"
  function: "Beleuchtung"
  description: "Deckenleuchte schalten"
"2/1/5":
  name: "Sensorik.OG.Schlafzimmer.Temperatur"
  dpt: "9.001"
  room: "Schlafzimmer"
  function: "Sensorik"
"3/2/7":
  name: "Versorgungstechnik.KG.Heizung.Energiezähler"
  dpt: "13.013"
```

The bridge itself only uses `name` and `dpt`; the optional fields are
consumed by downstream services (iot-mcp-bridge for SQL joins by room,
state-projector for Redis keying by function).

Use `knxproj-to-yaml` (requires the `[tools]` extra: `pip install
"knx-nats-bridge[tools]"`) to generate the catalog from an ETS `.knxproj`
export. It walks the ETS Building hierarchy for `room`, the ETS Functions
table for `function`, and uses the GA's `description`/`comment` field
verbatim:

```sh
knxproj-to-yaml --input project.knxproj --output knx-catalog.yaml \
                [--password 'ets-password']
```

## Payload format

Flat JSON, one object per telegram. Native DPT types preserved.

```json
{"ga":"1/2/3","name":"Hallway light","dpt":"1.001","value":true,"ts":"2026-04-22T12:34:56.789123Z"}
{"ga":"2/1/5","name":"Living room temperature","dpt":"9.001","value":21.5,"ts":"..."}
{"ga":"3/2/7","name":"Heat pump energy counter","dpt":"13.013","value":123456,"ts":"..."}
```

Timestamp is RFC3339 UTC with microsecond precision (Python `datetime` upper
bound). KNX can deliver several telegrams per second; microsecond resolution
keeps a `(time, ga)` primary key collision-free downstream.

Payloads are validated against
[src/knx_nats_bridge/\_schemas/event.schema.json](src/knx_nats_bridge/_schemas/event.schema.json)
before publishing.

## JetStream expectations

The bridge publishes with `js.publish()` (synchronous ack, not fire-and-forget)
so a mid-flight pod crash does not lose a telegram. A JetStream stream covering
the subject must exist before the bridge starts — otherwise publishes fail
with `no stream matches subject` and the bridge logs loudly and retries.

Suggested stream config (create with the NATS CLI):

```sh
nats stream add KNX \
  --subjects 'knx.>' \
  --storage file \
  --retention limits \
  --max-age 7d \
  --duplicate-window 2m \
  --defaults
```

Retention and storage size depend on your consumer patterns.

## Metrics

Exposed on `http://<pod>:${METRICS_PORT}/metrics`:

- `knx_telegrams_received_total` — counter, every GroupValue Write/Response telegram seen on the bus (regardless of mapping coverage)
- `knx_telegrams_decoded_total{dpt}` — counter, telegrams successfully decoded by DPT (subset of received)
- `knx_telegrams_unmapped_total` — counter, telegrams whose group address has no mapping entry
- `knx_telegrams_published_total` — counter, payloads successfully ack'd by NATS JetStream
- `knx_publish_errors_total{reason}` — counter, reasons: `timeout`, `schema`, `no_stream`, `nak`, `other`
- `knx_tunnel_connected` — gauge 0/1
- `nats_connected` — gauge 0/1
- `knx_last_telegram_received_timestamp` — gauge, Unix seconds — use for gap-detection alerts

`GET /healthz` returns 200 when KNX tunnel, NATS client, and JetStream stream
are all reachable. No "silence-based" health gate — a quiet bus is not a
failure. Add a Prometheus alert on `knx_last_telegram_received_timestamp` for
that.

## Singleton

Run exactly one instance. Two bridges would open two tunnel sessions and
publish every telegram twice. Kubernetes users: `replicas: 1` with
`strategy: Recreate`.

## Network considerations

- `tunneling_tcp` / `tunneling_udp`: regular TCP/UDP to the gateway, no
  special networking needed.
- `routing`: uses KNX/IP multicast (`224.0.23.12:3671`). Most Kubernetes CNIs
  (Cilium, Calico, Flannel) drop multicast by default. Either configure the
  CNI to allow it or run the pod with `hostNetwork: true`. Multicast routes
  on the host must reach the KNX-IP subnet.

## License

GPL-2.0-or-later. See [LICENSE](LICENSE).

`xknxproject` (used by the `knxproj-to-yaml` tool) is GPL-2.0-only; combining
it with this project is fine under GPL-2.0-or-later.
