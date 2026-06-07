//! End-to-end coverage for the sync EventBus.

use std::sync::{Arc, Mutex};
use std::thread;

use agent_event_bus::{Event, EventBus, Subscription, WILDCARD};
use serde_json::{json, Value};

// ---------- helpers ----------

fn make_seen() -> (
    Arc<Mutex<Vec<String>>>,
    impl Fn(&Event) + Send + Sync + 'static,
) {
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let cl = Arc::clone(&seen);
    let handler = move |e: &Event| cl.lock().unwrap().push(e.name.clone());
    (seen, handler)
}

// ---------- registration + emit ----------

#[test]
fn register_then_emit_fires_handler() {
    let bus = EventBus::new();
    let (seen, h) = make_seen();
    bus.on("llm.call.start", h);
    bus.emit("llm.call.start", json!({"x": 1}));
    assert_eq!(seen.lock().unwrap().as_slice(), &["llm.call.start"]);
}

#[test]
fn on_returns_subscription_with_id_and_event() {
    let bus = EventBus::new();
    let sub = bus.on("a", |_| {});
    assert!(sub.id() > 0);
    assert_eq!(sub.event(), "a");
}

#[test]
fn subscription_ids_are_monotonic() {
    let bus = EventBus::new();
    let s1 = bus.on("a", |_| {});
    let s2 = bus.on("a", |_| {});
    let s3 = bus.on("b", |_| {});
    assert!(s1.id() < s2.id());
    assert!(s2.id() < s3.id());
}

#[test]
fn non_matching_event_does_not_fire() {
    let bus = EventBus::new();
    let (seen, h) = make_seen();
    bus.on("a", h);
    bus.emit("b", Value::Null);
    assert!(seen.lock().unwrap().is_empty());
}

// ---------- wildcard ----------

#[test]
fn star_wildcard_fires_for_every_event() {
    let bus = EventBus::new();
    let (seen, h) = make_seen();
    bus.on(WILDCARD, h);
    bus.emit("a", Value::Null);
    bus.emit("b", Value::Null);
    bus.emit("c.d.e", Value::Null);
    assert_eq!(seen.lock().unwrap().as_slice(), &["a", "b", "c.d.e"]);
}

#[test]
fn wildcard_const_matches_literal_star() {
    assert_eq!(WILDCARD, "*");
    let bus = EventBus::new();
    let (seen, h) = make_seen();
    bus.on("*", h);
    bus.emit("anything", Value::Null);
    assert_eq!(seen.lock().unwrap().as_slice(), &["anything"]);
}

#[test]
fn wildcard_and_exact_both_fire() {
    let bus = EventBus::new();
    let order = Arc::new(Mutex::new(Vec::<&'static str>::new()));
    let o1 = Arc::clone(&order);
    let o2 = Arc::clone(&order);
    bus.on("e", move |_| o1.lock().unwrap().push("exact"));
    bus.on("*", move |_| o2.lock().unwrap().push("firehose"));
    bus.emit("e", Value::Null);
    assert_eq!(order.lock().unwrap().as_slice(), &["exact", "firehose"]);
}

#[test]
fn exact_fires_before_wildcard_even_when_wildcard_registered_first() {
    // Dispatch order must follow the documented "exact then firehose"
    // contract regardless of bucket creation order.
    let bus = EventBus::new();
    let order = Arc::new(Mutex::new(Vec::<&'static str>::new()));
    let o1 = Arc::clone(&order);
    let o2 = Arc::clone(&order);
    bus.on("*", move |_| o1.lock().unwrap().push("firehose"));
    bus.on("e", move |_| o2.lock().unwrap().push("exact"));
    bus.emit("e", Value::Null);
    assert_eq!(order.lock().unwrap().as_slice(), &["exact", "firehose"]);
}

#[test]
fn emitting_wildcard_name_fires_firehose_handlers_once() {
    // Emitting the literal "*" name should fire firehose handlers exactly
    // once (not twice via both the exact and wildcard match paths).
    let bus = EventBus::new();
    let count = Arc::new(Mutex::new(0u32));
    let cl = Arc::clone(&count);
    bus.on("*", move |_| *cl.lock().unwrap() += 1);
    bus.emit("*", Value::Null);
    assert_eq!(*count.lock().unwrap(), 1);
}

// ---------- order ----------

#[test]
fn multiple_handlers_run_in_registration_order() {
    let bus = EventBus::new();
    let seen = Arc::new(Mutex::new(Vec::<&'static str>::new()));
    let s1 = Arc::clone(&seen);
    let s2 = Arc::clone(&seen);
    let s3 = Arc::clone(&seen);
    bus.on("e", move |_| s1.lock().unwrap().push("a"));
    bus.on("e", move |_| s2.lock().unwrap().push("b"));
    bus.on("e", move |_| s3.lock().unwrap().push("c"));
    bus.emit("e", Value::Null);
    assert_eq!(seen.lock().unwrap().as_slice(), &["a", "b", "c"]);
}

// ---------- on_once ----------

#[test]
fn on_once_fires_exactly_once() {
    let bus = EventBus::new();
    let counter = Arc::new(Mutex::new(0u32));
    let cl = Arc::clone(&counter);
    bus.on_once("a", move |_| *cl.lock().unwrap() += 1);
    bus.emit("a", Value::Null);
    bus.emit("a", Value::Null);
    bus.emit("a", Value::Null);
    assert_eq!(*counter.lock().unwrap(), 1);
}

#[test]
fn on_once_does_not_affect_regular_handlers() {
    let bus = EventBus::new();
    let one = Arc::new(Mutex::new(0u32));
    let many = Arc::new(Mutex::new(0u32));
    let o = Arc::clone(&one);
    let m = Arc::clone(&many);
    bus.on_once("a", move |_| *o.lock().unwrap() += 1);
    bus.on("a", move |_| *m.lock().unwrap() += 1);
    bus.emit("a", Value::Null);
    bus.emit("a", Value::Null);
    assert_eq!(*one.lock().unwrap(), 1);
    assert_eq!(*many.lock().unwrap(), 2);
}

#[test]
fn on_once_on_wildcard_fires_for_first_event_only() {
    let bus = EventBus::new();
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let cl = Arc::clone(&seen);
    bus.on_once("*", move |e| cl.lock().unwrap().push(e.name.clone()));
    bus.emit("first", Value::Null);
    bus.emit("second", Value::Null);
    bus.emit("third", Value::Null);
    assert_eq!(seen.lock().unwrap().as_slice(), &["first"]);
}

// ---------- emit returns ----------

#[test]
fn emit_returns_event() {
    let bus = EventBus::new();
    let evt = bus.emit("a.b", json!({"hello": "world"}));
    assert_eq!(evt.name, "a.b");
    assert_eq!(evt.payload, json!({"hello": "world"}));
}

#[test]
fn payload_preserved_in_handler() {
    let bus = EventBus::new();
    let got = Arc::new(Mutex::new(Value::Null));
    let cl = Arc::clone(&got);
    bus.on("e", move |evt| *cl.lock().unwrap() = evt.payload.clone());
    bus.emit("e", json!({"k": [1, 2, 3], "nested": {"q": true}}));
    assert_eq!(
        *got.lock().unwrap(),
        json!({"k": [1, 2, 3], "nested": {"q": true}})
    );
}

// ---------- off / off_event / off_all ----------

#[test]
fn off_by_subscription_removes_one() {
    let bus = EventBus::new();
    let (seen, h) = make_seen();
    let sub = bus.on("a", h);
    bus.emit("a", Value::Null);
    assert_eq!(seen.lock().unwrap().len(), 1);
    let removed = bus.off(sub.clone());
    assert!(removed);
    bus.emit("a", Value::Null);
    assert_eq!(seen.lock().unwrap().len(), 1);
    // off again is no-op
    assert!(!bus.off(sub));
}

#[test]
fn off_does_not_affect_siblings() {
    let bus = EventBus::new();
    let keep = Arc::new(Mutex::new(0u32));
    let k = Arc::clone(&keep);
    let drop_sub = bus.on("a", |_| {});
    bus.on("a", move |_| *k.lock().unwrap() += 1);
    assert!(bus.off(drop_sub));
    bus.emit("a", Value::Null);
    assert_eq!(*keep.lock().unwrap(), 1);
}

#[test]
fn off_event_removes_all_for_event() {
    let bus = EventBus::new();
    bus.on("a", |_| {});
    bus.on("a", |_| {});
    bus.on("a", |_| {});
    bus.on("b", |_| {});
    let removed = bus.off_event("a");
    assert_eq!(removed, 3);
    assert_eq!(bus.len(), 1);
    // off_event for unknown returns 0
    assert_eq!(bus.off_event("nope"), 0);
}

#[test]
fn off_all_clears_everything() {
    let bus = EventBus::new();
    bus.on("a", |_| {});
    bus.on("b", |_| {});
    bus.on("*", |_| {});
    assert_eq!(bus.len(), 3);
    let cleared = bus.off_all();
    assert_eq!(cleared, 3);
    assert_eq!(bus.len(), 0);
    assert!(bus.is_empty());
}

// ---------- subscribers / len ----------

#[test]
fn subscribers_filter_by_event() {
    let bus = EventBus::new();
    bus.on("a", |_| {});
    bus.on("b", |_| {});
    bus.on("a", |_| {});
    bus.on("*", |_| {});

    let a_subs = bus.subscribers(Some("a"));
    assert_eq!(a_subs.len(), 2);
    assert!(a_subs.iter().all(|s| s.event() == "a"));

    let star_subs = bus.subscribers(Some("*"));
    assert_eq!(star_subs.len(), 1);

    let all = bus.subscribers(None);
    assert_eq!(all.len(), 4);
}

#[test]
fn len_tracks_total_subscribers() {
    let bus = EventBus::new();
    assert_eq!(bus.len(), 0);
    let s = bus.on("a", |_| {});
    assert_eq!(bus.len(), 1);
    bus.on("b", |_| {});
    assert_eq!(bus.len(), 2);
    bus.off(s);
    assert_eq!(bus.len(), 1);
}

// ---------- emit selectivity ----------

#[test]
fn emit_only_dispatches_matching_handlers() {
    let bus = EventBus::new();
    let a_fired = Arc::new(Mutex::new(0u32));
    let b_fired = Arc::new(Mutex::new(0u32));
    let a = Arc::clone(&a_fired);
    let b = Arc::clone(&b_fired);
    bus.on("a", move |_| *a.lock().unwrap() += 1);
    bus.on("b", move |_| *b.lock().unwrap() += 1);
    bus.emit("a", Value::Null);
    assert_eq!(*a_fired.lock().unwrap(), 1);
    assert_eq!(*b_fired.lock().unwrap(), 0);
    bus.emit("b", Value::Null);
    assert_eq!(*a_fired.lock().unwrap(), 1);
    assert_eq!(*b_fired.lock().unwrap(), 1);
}

// ---------- panic isolation ----------

#[test]
fn panic_in_handler_is_caught_and_other_handlers_continue() {
    let bus = EventBus::new();
    let after = Arc::new(Mutex::new(0u32));
    let a1 = Arc::clone(&after);
    let a2 = Arc::clone(&after);

    bus.on("e", move |_| *a1.lock().unwrap() += 1);
    bus.on("e", |_| panic!("boom"));
    bus.on("e", move |_| *a2.lock().unwrap() += 1);

    let _ = bus.emit("e", Value::Null);
    assert_eq!(*after.lock().unwrap(), 2);
}

#[test]
fn panic_does_not_poison_bus() {
    let bus = EventBus::new();
    bus.on("e", |_| panic!("boom"));
    let _ = bus.emit("e", Value::Null);
    // Bus should still be usable after the panic.
    let seen = Arc::new(Mutex::new(0u32));
    let cl = Arc::clone(&seen);
    bus.on("e", move |_| *cl.lock().unwrap() += 1);
    let _ = bus.emit("e", Value::Null);
    assert_eq!(*seen.lock().unwrap(), 1);
}

// ---------- on_error callback ----------

#[test]
fn on_error_invoked_with_subscription_and_event() {
    let bus = EventBus::new();
    let captured: Arc<Mutex<Vec<(Subscription, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let cl = Arc::clone(&captured);
    bus.set_on_error(Box::new(move |sub, evt| {
        cl.lock().unwrap().push((sub.clone(), evt.name.clone()));
    }));

    let bad = bus.on("e", |_| panic!("nope"));
    let _ = bus.emit("e", Value::Null);

    let got = captured.lock().unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].0.id(), bad.id());
    assert_eq!(got[0].1, "e");
}

#[test]
fn on_error_not_invoked_when_no_panic() {
    let bus = EventBus::new();
    let calls = Arc::new(Mutex::new(0u32));
    let cl = Arc::clone(&calls);
    bus.set_on_error(Box::new(move |_, _| *cl.lock().unwrap() += 1));

    bus.on("e", |_| { /* fine */ });
    let _ = bus.emit("e", Value::Null);
    assert_eq!(*calls.lock().unwrap(), 0);
}

#[test]
fn on_error_can_be_cleared() {
    let bus = EventBus::new();
    let calls = Arc::new(Mutex::new(0u32));
    let cl = Arc::clone(&calls);
    bus.set_on_error(Box::new(move |_, _| *cl.lock().unwrap() += 1));
    bus.clear_on_error();
    bus.on("e", |_| panic!("nope"));
    let _ = bus.emit("e", Value::Null);
    assert_eq!(*calls.lock().unwrap(), 0);
}

#[test]
fn on_error_fires_per_panicking_handler() {
    let bus = EventBus::new();
    let count = Arc::new(Mutex::new(0u32));
    let cl = Arc::clone(&count);
    bus.set_on_error(Box::new(move |_, _| *cl.lock().unwrap() += 1));

    bus.on("e", |_| panic!("a"));
    bus.on("e", |_| { /* ok */ });
    bus.on("e", |_| panic!("b"));

    let _ = bus.emit("e", Value::Null);
    assert_eq!(*count.lock().unwrap(), 2);
}

// ---------- panicking on_once is still removed ----------

#[test]
fn on_once_removed_even_if_handler_panics() {
    let bus = EventBus::new();
    let count = Arc::new(Mutex::new(0u32));
    let cl = Arc::clone(&count);
    bus.on_once("e", move |_| {
        *cl.lock().unwrap() += 1;
        panic!("once-and-done");
    });
    let _ = bus.emit("e", Value::Null);
    let _ = bus.emit("e", Value::Null);
    assert_eq!(*count.lock().unwrap(), 1);
    assert_eq!(bus.subscribers(Some("e")).len(), 0);
}

// ---------- thread safety ----------

#[test]
fn bus_is_shareable_across_threads() {
    let bus = Arc::new(EventBus::new());
    let counter = Arc::new(Mutex::new(0u32));
    let cl = Arc::clone(&counter);
    bus.on("e", move |_| *cl.lock().unwrap() += 1);

    let mut handles = Vec::new();
    for _ in 0..8 {
        let b = Arc::clone(&bus);
        handles.push(thread::spawn(move || {
            for _ in 0..50 {
                b.emit("e", Value::Null);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(*counter.lock().unwrap(), 8 * 50);
}

// ---------- Event shape ----------

#[test]
fn event_holds_name_and_payload_verbatim() {
    let bus = EventBus::new();
    let captured: Arc<Mutex<Option<Event>>> = Arc::new(Mutex::new(None));
    let cl = Arc::clone(&captured);
    bus.on("x", move |e| *cl.lock().unwrap() = Some(e.clone()));
    bus.emit("x", json!({"a": 1, "b": "two"}));
    let evt = captured.lock().unwrap().clone().unwrap();
    assert_eq!(evt.name, "x");
    assert_eq!(evt.payload, json!({"a": 1, "b": "two"}));
}
