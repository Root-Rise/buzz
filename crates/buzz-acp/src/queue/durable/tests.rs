use super::*;
use crate::bridge_state::BridgeState;
use nostr::{EventBuilder, Keys, Kind};

fn input(keys: &Keys, scope: &SessionScope, content: &str) -> QueuedEvent {
    QueuedEvent {
        channel_id: scope.channel_id(),
        scope: scope.clone(),
        event: EventBuilder::new(Kind::Custom(9), content)
            .sign_with_keys(keys)
            .unwrap(),
        received_at: Instant::now(),
        prompt_tag: "mention".into(),
    }
}

fn open(path: &std::path::Path, keys: &Keys) -> BridgeState {
    BridgeState::open(path, "ws://isolated.invalid", &keys.public_key().to_hex()).unwrap()
}

#[test]
fn restart_never_replays_terminal_or_uncertain_inputs_and_sibling_can_progress() {
    let keys = Keys::generate();
    let path = std::env::temp_dir()
        .join(format!("buzz-queue-{}", Uuid::new_v4()))
        .join("state.db");
    let state = open(&path, &keys);
    let channel_id = Uuid::new_v4();
    let blocked = SessionScope::Thread {
        channel_id,
        root_event_id: "a".repeat(64),
    };
    let sibling = SessionScope::Thread {
        channel_id,
        root_event_id: "b".repeat(64),
    };
    let mut queue = EventQueue::new(DedupMode::Queue).with_durable(Some(state.clone()));
    let mut previously_submitted = Vec::new();
    for status in [
        "completed",
        "failed",
        "cancelled",
        "interrupted",
        "uncertain",
    ] {
        let event = input(&keys, &blocked, status);
        let id = event.event.id.to_hex();
        assert!(queue.push(event.clone()));
        state
            .prepare_submission(std::slice::from_ref(&id), status, &json!({"text": status}))
            .unwrap();
        state.finish_batch(&[id], status).unwrap();
        previously_submitted.push(event);
    }
    let pending = input(&keys, &blocked, "waiting behind uncertain work");
    let independent = input(&keys, &sibling, "independent sibling");
    assert!(queue.push(pending.clone()));
    assert!(queue.push(independent.clone()));
    drop((queue, state));

    let state = open(&path, &keys);
    let mut queue = EventQueue::new(DedupMode::Queue).with_durable(Some(state.clone()));
    for old in &previously_submitted {
        assert!(
            !queue.push(old.clone()),
            "relay replay cannot re-admit existing work"
        );
    }
    let subscriptions = HashSet::from([channel_id]);
    queue.refill_durable(&subscriptions).unwrap();
    let batch = queue
        .flush_next()
        .expect("uncertain scope must not block sibling");
    assert_eq!(batch.scope, sibling);
    assert_eq!(
        batch.events.iter().map(|e| e.event.id).collect::<Vec<_>>(),
        [independent.event.id]
    );
    assert!(
        queue.flush_next().is_none(),
        "uncertain work fences only its own scope"
    );
    let independent_id = independent.event.id.to_hex();
    state
        .prepare_submission(
            std::slice::from_ref(&independent_id),
            "sibling",
            &json!({"text":"independent"}),
        )
        .unwrap();
    state.finish_batch(&[independent_id], "completed").unwrap();
    queue.mark_complete(&sibling);
    // A later authoritative completion releases the scope, without replaying its old input.
    let uncertain = previously_submitted.last().unwrap().event.id.to_hex();
    state.finish_batch(&[uncertain], "completed").unwrap();
    queue.refill_durable(&subscriptions).unwrap();
    let batch = queue
        .flush_next()
        .expect("queued work becomes eligible after reconciliation");
    assert_eq!(
        batch.events.iter().map(|e| e.event.id).collect::<Vec<_>>(),
        [pending.event.id]
    );
}

#[test]
fn capacity_overflow_is_durable_and_restart_refill_delivers_each_input_once() {
    let keys = Keys::generate();
    let path = std::env::temp_dir()
        .join(format!("buzz-queue-{}", Uuid::new_v4()))
        .join("state.db");
    let state = open(&path, &keys);
    let scope = SessionScope::Thread {
        channel_id: Uuid::new_v4(),
        root_event_id: "c".repeat(64),
    };
    let subscriptions = HashSet::from([scope.channel_id()]);
    let mut queue = EventQueue::new(DedupMode::Queue).with_durable(Some(state.clone()));
    let mut expected = HashSet::new();
    for index in 0..MAX_PENDING_PER_CHANNEL + 2 {
        let event = input(&keys, &scope, &format!("synthetic-{index}"));
        expected.insert(event.event.id.to_hex());
        assert!(queue.push(event));
    }
    assert_eq!(state.pending(&scope).unwrap().len(), expected.len());
    assert!(
        queue.queued_event_count(&scope) < expected.len(),
        "exercise actual cache overflow"
    );
    let mut delivered = HashSet::new();
    while delivered.len() < expected.len() {
        // Lose the whole memory cache between batches, as on a bridge restart.
        drop(queue);
        queue = EventQueue::new(DedupMode::Queue).with_durable(Some(open(&path, &keys)));
        queue.refill_durable(&subscriptions).unwrap();
        let batch = queue
            .flush_next()
            .expect("accepted overflow remains deliverable");
        let ids: Vec<String> = batch
            .events
            .iter()
            .map(|event| event.event.id.to_hex())
            .collect();
        assert!(
            ids.iter().all(|id| delivered.insert(id.clone())),
            "completed input was replayed"
        );
        state
            .prepare_submission(
                &ids,
                &format!("batch-{}", delivered.len()),
                &json!({"events":ids}),
            )
            .unwrap();
        state.mark_accepted_batch(&ids).unwrap();
        state.finish_batch(&ids, "completed").unwrap();
        queue.mark_complete(&scope);
    }
    assert_eq!(delivered, expected);
    queue.refill_durable(&subscriptions).unwrap();
    assert!(queue.flush_next().is_none());
    assert!(state.pending(&scope).unwrap().is_empty());
}
