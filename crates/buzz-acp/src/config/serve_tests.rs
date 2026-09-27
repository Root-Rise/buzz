use super::*;
use clap::Parser;

fn serve_args() -> CliArgs {
    CliArgs::try_parse_from([
        "buzz-acp",
        "--private-key",
        "0000000000000000000000000000000000000000000000000000000000000001",
        "--serve-url",
        "ws://127.0.0.1:49123/api/ws",
        "--serve-profile",
        "isolated",
        "--bridge-state",
        "/tmp/buzz-config-test.db",
        "--permission-mode",
        "default",
        "--multiple-event-handling",
        "queue",
        "--no-base-prompt",
    ])
    .unwrap()
}

#[test]
fn serve_config_propagates_settings_without_exposing_credentials() {
    let mut args = serve_args();
    args.model = Some("synthetic-model".into());
    args.effort_level = Some("high".into());
    args.serve_token = Some("synthetic-secret".into());
    let config = Config::from_args(args).unwrap();
    let serve = config.serve.as_ref().unwrap();
    assert_eq!(serve.model, config.model);
    assert_eq!(serve.effort, config.effort_level);
    assert_eq!(serve.profile, "isolated");
    let summary = config.summary();
    assert!(summary.contains("hermes-serve"));
    assert!(!summary.contains("synthetic-secret"));
    assert!(!format!("{serve:?}").contains("synthetic-secret"));
    let mut native_args = serve_args();
    native_args.serve_credentials = Some("/tmp/isolated-native-auth.json".into());
    assert!(Config::from_args(native_args)
        .unwrap()
        .serve
        .unwrap()
        .credentials
        .is_some());
    let acp = CliArgs::try_parse_from([
        "buzz-acp",
        "--private-key",
        "0000000000000000000000000000000000000000000000000000000000000001",
        "--no-base-prompt",
    ])
    .unwrap();
    assert!(Config::from_args(acp).unwrap().serve.is_none());
}

#[test]
fn serve_config_rejects_unsupported_or_nondurable_execution() {
    let mutations: &[fn(&mut CliArgs)] = &[
        |args| args.bridge_state = None,
        |args| args.serve_url = Some("ws://127.0.0.1/api/ws?token=secret".into()),
        |args| args.mcp_command = "synthetic-mcp".into(),
        |args| args.multiple_event_handling = MultipleEventHandling::Steer,
        |args| args.permission_mode = PermissionMode::BypassPermissions,
        |args| args.heartbeat_interval = 10,
        |args| args.initial_message = Some("must not submit".into()),
        |args| args.max_turns_per_session = 2,
    ];
    for mutate in mutations {
        let mut args = serve_args();
        mutate(&mut args);
        assert!(Config::from_args(args).is_err());
    }
}
