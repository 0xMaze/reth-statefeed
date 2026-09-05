# Metrics

`reth-statefeed` registers metrics in Reth's global recorder. Enable the normal Reth endpoint with
`--metrics <address:port>`. The Prometheus exporter may normalize dots to underscores.

| Metric | Type | Meaning |
| --- | --- | --- |
| `statefeed.engine.extract.duration_seconds` | histogram | Watch filtering on the validator thread |
| `statefeed.engine.enqueue.duration_seconds` | histogram | Non-blocking queue operation |
| `statefeed.engine.queue.depth` | gauge | Current engine-to-publisher queue depth |
| `statefeed.engine.events.queued_total` | counter | Successfully queued internal events |
| `statefeed.engine.events.dropped_total` | counter | Events dropped by queue overflow or held behind a pending gap marker |
| `statefeed.publisher.projection.duration_seconds` | histogram | Parent copy, delta application, and publication |
| `statefeed.publisher.encode.duration_seconds` | histogram | One protobuf encode shared by all consumers |
| `statefeed.publisher.frame_bytes` | histogram | Encoded length-prefixed frame size |
| `statefeed.publisher.frames_total` | counter | Live frames published into the broadcast ring |
| `statefeed.latency.end_to_end_seconds{event}` | histogram | Observer entry through successful frame publication; ignored/deferred events are excluded |
| `statefeed.snapshot.duration_seconds` | histogram | Anchored internal-provider snapshot reads |
| `statefeed.socket.send.duration_seconds` | histogram | Per-consumer UDS writes, including local backpressure |
| `statefeed.events.total{type}` | counter | Published live events by lifecycle type |
| `statefeed.candidates.cached` | gauge | Candidate metadata records retained for forks/reorgs; full projections have a separately configured bound |
| `statefeed.candidates.projections_cached` | gauge | Full packed candidate projections currently retained |
| `statefeed.candidates.retired_total` | counter | Exact candidate hashes retired by cache, TTL, finality, or source lifecycle |
| `statefeed.candidates.parent_cache_misses_total` | counter | Candidates whose parent projection is absent from the bounded cache |
| `statefeed.consumers.connected` | gauge | Active Unix-socket consumers |
| `statefeed.consumer.gaps_total` | counter | Consumers disconnected after falling behind the ring |
| `statefeed.config.generation` | gauge | Active watch-set generation |

For latency alerts, separate the validator hot path (`extract` + `enqueue`) from publisher/transport
latency. A high socket-send tail with a low publisher tail identifies a slow consumer rather than
an execution-thread regression.
