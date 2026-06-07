//! # agent-event-bus
//!
//! Tiny in-process pub/sub for agent loop events. Sync-only Rust mirror of
//! the Python [`agent-event-bus`](https://pypi.org/project/agent-event-bus/)
//! library.
//!
//! Producers emit events by name; subscribers register a handler for the
//! event name they care about. Pass `"*"` as the name to subscribe to every
//! event (the "firehose"). One-shot handlers are supported via
//! [`EventBus::on_once`].
//!
//! Subscribers are dispatched inline on the caller's thread, in registration
//! order. A handler that panics is caught with [`std::panic::catch_unwind`]
//! so the bus keeps dispatching to the rest. An optional `on_error`
//! callback is invoked with the offending [`Subscription`] and [`Event`].
//!
//! This crate is sync-only. The Python sibling also exposes an `emit_async`
//! path; that is intentionally not mirrored here because it would pull in a
//! runtime (tokio/async-std) and tangle the dispatch model. If you want
//! async dispatch, spawn it on top of this bus.
//!
//! ## Quick example
//!
//! ```
//! use agent_event_bus::EventBus;
//! use std::sync::{Arc, Mutex};
//!
//! let bus = EventBus::new();
//! let seen = Arc::new(Mutex::new(Vec::<String>::new()));
//!
//! let seen_cl = Arc::clone(&seen);
//! bus.on("llm.call.start", move |event| {
//!     seen_cl.lock().unwrap().push(event.name.clone());
//! });
//!
//! bus.emit("llm.call.start", serde_json::json!({"model": "claude-opus-4-7"}));
//! assert_eq!(seen.lock().unwrap().as_slice(), &["llm.call.start"]);
//! ```
//!
//! ## Wildcard (firehose)
//!
//! ```
//! use agent_event_bus::EventBus;
//! use std::sync::{Arc, Mutex};
//!
//! let bus = EventBus::new();
//! let counter = Arc::new(Mutex::new(0u32));
//! let cl = Arc::clone(&counter);
//! bus.on("*", move |_| { *cl.lock().unwrap() += 1; });
//! bus.emit("a", serde_json::Value::Null);
//! bus.emit("b", serde_json::Value::Null);
//! assert_eq!(*counter.lock().unwrap(), 2);
//! ```
//!
//! ## Not a real message queue
//!
//! No persistence, no cross-process delivery, no backpressure. Reach for
//! Redis, NATS, or RabbitMQ if you need those. This crate is just clean
//! wiring inside one Rust process.

#![deny(missing_docs)]

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use serde_json::Value;

/// Wildcard event name that matches every emit.
pub const WILDCARD: &str = "*";

/// A single dispatched event.
#[derive(Debug, Clone)]
pub struct Event {
    /// The event name passed to [`EventBus::emit`].
    pub name: String,
    /// The arbitrary JSON payload attached to the event.
    pub payload: Value,
}

/// Boxed handler type. Subscribers must be `Send + Sync + 'static` so the
/// bus can be shared across threads.
type Handler = Box<dyn Fn(&Event) + Send + Sync + 'static>;

/// Boxed error callback, invoked once per handler that panics during
/// [`EventBus::emit`].
pub type OnError = Box<dyn Fn(&Subscription, &Event) + Send + Sync + 'static>;

/// Opaque handle returned from [`EventBus::on`] / [`EventBus::on_once`].
///
/// Pass it back to [`EventBus::off`] to remove the underlying subscriber.
/// Cheap to clone (`u64` id + short event name).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Subscription {
    id: u64,
    event: String,
}

impl Subscription {
    /// The monotonic id assigned at registration time.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// The event name this subscription was registered against
    /// (`"*"` for the firehose).
    pub fn event(&self) -> &str {
        &self.event
    }
}

struct Slot {
    sub: Subscription,
    handler: Handler,
    once: bool,
}

struct Inner {
    /// Handlers keyed by event name. The special key `"*"` is the firehose.
    /// We use a `Vec<(String, Vec<Slot>)>` instead of `HashMap` to keep
    /// the file small and because registration count is usually tiny;
    /// linear scan is fine.
    buckets: Vec<(String, Vec<Slot>)>,
    on_error: Option<OnError>,
}

impl Inner {
    fn new() -> Self {
        Self {
            buckets: Vec::new(),
            on_error: None,
        }
    }

    fn bucket_mut(&mut self, event: &str) -> &mut Vec<Slot> {
        if let Some(idx) = self.buckets.iter().position(|(k, _)| k == event) {
            return &mut self.buckets[idx].1;
        }
        self.buckets.push((event.to_string(), Vec::new()));
        &mut self.buckets.last_mut().unwrap().1
    }

    fn len(&self) -> usize {
        self.buckets.iter().map(|(_, v)| v.len()).sum()
    }
}

/// In-process pub/sub bus.
///
/// `EventBus` is `Send + Sync` — clone an `Arc<EventBus>` to share it across
/// threads. All methods take `&self` and lock an internal mutex.
pub struct EventBus {
    inner: Mutex<Inner>,
    next_id: AtomicU64,
}

impl EventBus {
    /// Create a new empty bus.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner::new()),
            next_id: AtomicU64::new(1),
        }
    }

    /// Install a panic-aware error callback.
    ///
    /// The callback fires once for each subscriber whose handler panicked
    /// during an [`emit`](Self::emit). The panic payload is intentionally
    /// not surfaced — most agent code only needs to know that *something*
    /// went wrong with a given subscription, and the payload type
    /// (`Box<dyn Any>`) is awkward to thread through a stable API.
    pub fn set_on_error(&self, cb: OnError) {
        self.inner.lock().unwrap().on_error = Some(cb);
    }

    /// Remove any installed error callback.
    pub fn clear_on_error(&self) {
        self.inner.lock().unwrap().on_error = None;
    }

    /// Register a handler for `event_name`.
    ///
    /// Pass `"*"` (or [`WILDCARD`]) to fire on every event.
    pub fn on<F>(&self, event_name: &str, handler: F) -> Subscription
    where
        F: Fn(&Event) + Send + Sync + 'static,
    {
        self.register(event_name, Box::new(handler), false)
    }

    /// Register a one-shot handler. It fires at most once, then is removed
    /// automatically before the next [`emit`](Self::emit) of any event.
    pub fn on_once<F>(&self, event_name: &str, handler: F) -> Subscription
    where
        F: Fn(&Event) + Send + Sync + 'static,
    {
        self.register(event_name, Box::new(handler), true)
    }

    fn register(&self, event_name: &str, handler: Handler, once: bool) -> Subscription {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let sub = Subscription {
            id,
            event: event_name.to_string(),
        };
        let mut inner = self.inner.lock().unwrap();
        let bucket = inner.bucket_mut(event_name);
        bucket.push(Slot {
            sub: sub.clone(),
            handler,
            once,
        });
        sub
    }

    /// Remove a single subscription by handle. Returns `true` if it was
    /// found and removed, `false` if the subscription was already gone
    /// (off-twice is a no-op).
    pub fn off(&self, sub: Subscription) -> bool {
        let mut inner = self.inner.lock().unwrap();
        if let Some((_, slots)) = inner.buckets.iter_mut().find(|(k, _)| k == &sub.event) {
            if let Some(pos) = slots.iter().position(|s| s.sub.id == sub.id) {
                slots.remove(pos);
                return true;
            }
        }
        false
    }

    /// Remove every subscriber registered against `event_name`. Returns the
    /// number removed.
    pub fn off_event(&self, event_name: &str) -> usize {
        let mut inner = self.inner.lock().unwrap();
        if let Some(idx) = inner.buckets.iter().position(|(k, _)| k == event_name) {
            let removed = inner.buckets[idx].1.len();
            inner.buckets.remove(idx);
            return removed;
        }
        0
    }

    /// Remove every subscriber on the bus. Returns the total number
    /// removed.
    pub fn off_all(&self) -> usize {
        let mut inner = self.inner.lock().unwrap();
        let removed = inner.len();
        inner.buckets.clear();
        removed
    }

    /// Emit an event. Fires all handlers for the exact `event_name`, then
    /// all firehose handlers registered against `"*"`, in registration
    /// order within each bucket.
    ///
    /// Handler panics are caught; dispatch continues. If an [`OnError`]
    /// callback is installed via [`set_on_error`](Self::set_on_error), it
    /// is invoked once per panicking handler.
    ///
    /// One-shot subscribers registered with [`on_once`](Self::on_once) are
    /// removed after they fire — even if they panicked.
    ///
    /// Returns the [`Event`] that was dispatched (cheaply clonable).
    pub fn emit(&self, event_name: &str, payload: Value) -> Event {
        let event = Event {
            name: event_name.to_string(),
            payload,
        };

        // Snapshot the (bucket, id) pairs to fire. We can't move boxed
        // handlers out of the bus, so each handler call re-locks and
        // looks up by subscription id. Handlers run while holding the
        // mutex, which means same-thread re-entry into the bus will
        // deadlock — a deliberate trade-off to keep this crate small.
        //
        // The exact-name bucket is collected before the wildcard bucket so
        // the documented "exact handlers, then firehose handlers" order
        // holds regardless of which bucket was created first.
        let to_fire: Vec<(String, u64)> = {
            let inner = self.inner.lock().unwrap();
            let mut out = Vec::new();
            if event_name != WILDCARD {
                if let Some((key, slots)) = inner.buckets.iter().find(|(k, _)| k == event_name) {
                    for slot in slots {
                        out.push((key.clone(), slot.sub.id));
                    }
                }
            }
            if let Some((key, slots)) = inner.buckets.iter().find(|(k, _)| k == WILDCARD) {
                for slot in slots {
                    out.push((key.clone(), slot.sub.id));
                }
            }
            out
        };

        let mut errored: Vec<Subscription> = Vec::new();
        let mut fired_once: Vec<(String, u64)> = Vec::new();

        for (bucket_key, id) in to_fire {
            let result = {
                let inner = self.inner.lock().unwrap();
                let bucket = inner.buckets.iter().find(|(k, _)| k == &bucket_key);
                let slot = bucket.and_then(|(_, slots)| slots.iter().find(|s| s.sub.id == id));
                slot.map(|slot| {
                    let panicked =
                        catch_unwind(AssertUnwindSafe(|| (slot.handler)(&event))).is_err();
                    (slot.sub.clone(), slot.once, panicked)
                })
            };

            if let Some((sub, once, panicked)) = result {
                if panicked {
                    errored.push(sub.clone());
                }
                if once {
                    fired_once.push((bucket_key, sub.id));
                }
            }
        }

        // Remove one-shot subscribers that fired.
        if !fired_once.is_empty() {
            let mut inner = self.inner.lock().unwrap();
            for (bucket_key, id) in fired_once {
                if let Some((_, slots)) = inner.buckets.iter_mut().find(|(k, _)| k == &bucket_key) {
                    if let Some(pos) = slots.iter().position(|s| s.sub.id == id) {
                        slots.remove(pos);
                    }
                }
            }
        }

        // Notify the error callback after dispatch so it doesn't
        // interleave with normal handler invocation order.
        if !errored.is_empty() {
            let inner = self.inner.lock().unwrap();
            if let Some(cb) = inner.on_error.as_ref() {
                for sub in &errored {
                    cb(sub, &event);
                }
            }
        }

        event
    }

    /// Return the list of subscription handles registered against an
    /// event name. Pass `None` to list every subscription on the bus
    /// (across all event names, including the firehose).
    pub fn subscribers(&self, event_name: Option<&str>) -> Vec<Subscription> {
        let inner = self.inner.lock().unwrap();
        let mut out = Vec::new();
        match event_name {
            Some(name) => {
                if let Some((_, slots)) = inner.buckets.iter().find(|(k, _)| k == name) {
                    for slot in slots {
                        out.push(slot.sub.clone());
                    }
                }
            }
            None => {
                for (_, slots) in &inner.buckets {
                    for slot in slots {
                        out.push(slot.sub.clone());
                    }
                }
            }
        }
        out
    }

    /// Total number of subscribers across every event name (including
    /// the firehose).
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    /// `true` if no subscribers are registered.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for EventBus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventBus")
            .field("subscribers", &self.len())
            .finish()
    }
}
