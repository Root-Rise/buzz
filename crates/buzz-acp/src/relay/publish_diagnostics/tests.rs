use super::*;
use crate::relay::{self, BgState, RelayCommand};
use futures_util::{SinkExt, StreamExt};
use nostr::{Event, EventBuilder, Keys, Kind};
use serde_json::json;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

fn event(keys: &Keys, kind: u16) -> Event {
    EventBuilder::new(Kind::Custom(kind), "")
        .sign_with_keys(keys)
        .expect("sign test event")
}

#[tokio::test]
async fn actual_publish_and_ack_correlate_independent_categories() {
    let (mut client, mut server) = relay::tests::test_ws_pair().await;
    let keys = Keys::generate();
    let mut state = BgState::new();
    let typing = event(&keys, 20002);
    let observer = event(&keys, 24200);
    for ev in [&typing, &observer] {
        relay::execute_connected_command(
            &mut client,
            &mut state,
            "test",
            RelayCommand::PublishEvent {
                event: Box::new(ev.clone()),
            },
        )
        .await;
        let frame = relay::tests::next_test_frame(&mut server).await;
        assert_eq!(frame[0], "EVENT");
        assert_eq!(frame[1]["id"], ev.id.to_hex());
    }
    assert_eq!(
        state.typing_diagnostics.pending.map(|p| p.0),
        Some(typing.id)
    );
    assert_eq!(
        state.observer_diagnostics.pending.map(|p| p.0),
        Some(observer.id)
    );
    let unrelated = event(&keys, 1);
    let (events, _) = mpsc::channel(4);
    let (controls, _) = mpsc::channel(4);
    for (ev, accepted) in [(&unrelated, true), (&typing, false), (&observer, true)] {
        server
            .send(Message::Text(
                json!(["OK", ev.id.to_hex(), accepted, "test acknowledgement"])
                    .to_string()
                    .into(),
            ))
            .await
            .expect("send relay ACK");
        let frame = client.next().await.expect("socket open").expect("read ACK");
        relay::handle_ws_message(
            frame,
            &mut client,
            &events,
            &controls,
            &mut state,
            &keys,
            "ws://localhost",
            "test",
            None,
        )
        .await;
        if ev.id == unrelated.id {
            assert!(state.typing_diagnostics.pending.is_some());
            assert!(state.observer_diagnostics.pending.is_some());
        } else if ev.id == typing.id {
            assert!(state.typing_diagnostics.pending.is_none());
            assert!(state.observer_diagnostics.pending.is_some());
        }
    }
    assert!(state.observer_diagnostics.pending.is_none());
}

#[tokio::test]
async fn gated_events_expect_no_ack_until_actual_drain_and_delayed_sends_expire() {
    let (mut client, mut server) = relay::tests::test_ws_pair().await;
    let keys = Keys::generate();
    let mut state = BgState::new();
    let typing = event(&keys, 20002);
    let observer = event(&keys, 24200);
    state.set_rate_limit_gate(60);
    for ev in [&typing, &observer] {
        relay::execute_connected_command(
            &mut client,
            &mut state,
            "test",
            RelayCommand::PublishEvent {
                event: Box::new(ev.clone()),
            },
        )
        .await;
    }
    assert!(state.typing_diagnostics.pending.is_none());
    assert!(state.observer_diagnostics.pending.is_none());
    assert_eq!(state.gated_observer_pending.len(), 1);
    state.rate_limit_gate = None;
    assert_eq!(
        relay::drain_gated_observer_pending(&mut client, &mut state, 1).await,
        1
    );
    let frame = relay::tests::next_test_frame(&mut server).await;
    assert_eq!(
        frame[1]["id"],
        observer.id.to_hex(),
        "gated typing was not published before observer drain"
    );
    assert_eq!(
        state.observer_diagnostics.pending.map(|p| p.0),
        Some(observer.id)
    );

    tokio::time::pause();
    let mut delayed = PublishDiagnostics::new("typing");
    assert!(delayed.sample());
    tokio::time::advance(Duration::from_secs(10)).await;
    delayed.sent(typing.id);
    tokio::time::advance(Duration::from_secs(20)).await;
    assert!(
        !delayed.sample(),
        "an outstanding ACK cannot be overwritten by a new sample"
    );
    tokio::time::advance(Duration::from_secs(10)).await;
    delayed.expire();
    assert!(
        delayed.pending.is_none(),
        "missing ACK becomes unknown, not permanently pending"
    );
    assert!(delayed.sample());
    delayed.sent(observer.id);
    delayed.acknowledge(&typing.id.to_hex(), true, "late old ACK");
    assert_eq!(delayed.pending.map(|p| p.0), Some(observer.id));
}
