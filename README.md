# agent-event-bus

[![Crates.io](https://img.shields.io/crates/v/agent-event-bus.svg)](https://crates.io/crates/agent-event-bus)
[![Docs.rs](https://docs.rs/agent-event-bus/badge.svg)](https://docs.rs/agent-event-bus)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-green.svg)](#license)

**Tiny in-process pub/sub for agent loop events.** Sync-only Rust mirror of
the Python [`agent-event-bus`](https://pypi.org/project/agent-event-bus/)
library.

Agent code grows messy fast. The LLM call site ends up knowing about the
logger, the dashboard, the cost tracker, the alert hook, and the audit
writer all at once. This crate is the small bus that decouples them:
producers `emit("llm.call.start", ...)`, subscribers register for the
event types they care about, and nobody has to import anyone else.

Not a real message queue. No persistence, no cross-process delivery. Use
Redis, NATS, or RabbitMQ if you need that. Use this if you just want
clean wiring inside one Rust process.

## Sync-only

This crate is intentionally sync. The Python sibling has both `emit` and
`emit_async`; the Rust async story pulls in a runtime (tokio/async-std)
and tangles the dispatch model, so it lives only in Python. If you want
async dispatch, spawn it on top of this bus.

## Install

```toml
[dependencies]
agent-event-bus = "0.1"
serde_json = "1"
```

## Basic use

```rust
use agent_event_bus::EventBus;
use serde_json::json;
use std::sync::{Arc, Mutex};

let bus = EventBus::new();
let seen = Arc::new(Mutex::new(Vec::<String>::new()));

let cl = Arc::clone(&seen);
bus.on("llm.call.start", move |event| {
    cl.lock().unwrap().push(event.name.clone());
});

bus.emit("llm.call.start", json!({"model": "claude-opus-4-7", "tokens_in": 1000}));
bus.emit("llm.call.end", json!({"ms": 842}));
```

## Subscriptions

`on` / `on_once` return an opaque `Subscription` handle. Pass it back to
`off` to remove just that subscriber.

```rust
use agent_event_bus::EventBus;

let bus = EventBus::new();
let sub = bus.on("a", |_| {});
assert!(bus.off(sub));   // returns true on first call
```

`off_event("a")` removes every subscriber for `"a"`; `off_all()` clears
the whole bus and returns the count.

## One-shot

```rust
use agent_event_bus::EventBus;
use serde_json::Value;

let bus = EventBus::new();
bus.on_once("ready", |_| println!("fired exactly once"));
bus.emit("ready", Value::Null);  // fires
bus.emit("ready", Value::Null);  // no-op; the subscription is gone
```

## Wildcard (firehose)

Pass `"*"` (or the `WILDCARD` const) as the event name to receive every
emit. There is one wildcard, by design — the Python version supports
segment-based `*` / `**` patterns; in Rust we keep it small.

```rust
use agent_event_bus::{EventBus, WILDCARD};

let bus = EventBus::new();
bus.on(WILDCARD, |event| println!("[bus] {} -> {}", event.name, event.payload));
```

## Error isolation

Subscriber panics are caught with `std::panic::catch_unwind` and dispatch
continues. Install an `on_error` callback to be told which subscription
panicked on which event. The panic payload itself is intentionally not
surfaced — `Box<dyn Any>` is awkward to thread through a stable API and
most callers only need to know that *something* went wrong.

```rust
use agent_event_bus::EventBus;
use serde_json::Value;

let bus = EventBus::new();
bus.set_on_error(Box::new(|sub, event| {
    eprintln!("handler {} on {} panicked", sub.id(), event.name);
}));
bus.on("e", |_| panic!("boom"));
bus.emit("e", Value::Null);
```

## Listing subscribers

```rust
use agent_event_bus::EventBus;

let bus = EventBus::new();
bus.on("a", |_| {});
bus.on("a", |_| {});
bus.on("b", |_| {});

assert_eq!(bus.subscribers(Some("a")).len(), 2);
assert_eq!(bus.subscribers(None).len(), 3);
assert_eq!(bus.len(), 3);
```

## What it does NOT do

- No persistence, no cross-process delivery. Single process only.
- No async dispatch. Subscribers run inline on the caller's thread.
- No backpressure, queues, priorities, or middleware chain. Dispatch
  order is registration order.
- No segment-based wildcards (`llm.*`, `llm.**`). The Python sibling has
  them; the Rust mirror keeps it to a single `"*"` firehose.

## Siblings

- [`claude-cost`](https://crates.io/crates/claude-cost) — cache-aware
  cost calculator for Claude API responses.
- [`agenttrace-rs`](https://crates.io/crates/agenttrace-rs) — whole-run
  cost + latency aggregator. Plug it into the bus as a subscriber.
- [`agentsnap-rs`](https://crates.io/crates/agentsnap-rs) — Jest-style
  snapshots for agent runs.

The bus is the wiring; the siblings are some of the things you wire
into it.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or
  <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or
  <http://opensource.org/licenses/MIT>)

at your option.
