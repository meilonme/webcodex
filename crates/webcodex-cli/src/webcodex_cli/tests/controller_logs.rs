use super::*;

#[test]
fn parses_log_follow_options_and_preserves_snapshot_default() {
    for flag in [None, Some("-f"), Some("--follow")] {
        let mut args = vec![
            "logs".into(),
            "--config".into(),
            "/tmp/controller.toml".into(),
            "--service-file".into(),
            "/tmp/controller.service".into(),
            "--lines".into(),
            "25".into(),
        ];
        if let Some(flag) = flag {
            args.push(flag.into());
        }
        assert_eq!(
            parse_controller_command(&args).unwrap(),
            ControllerCommand::Logs {
                config: PathBuf::from("/tmp/controller.toml"),
                service_file: PathBuf::from("/tmp/controller.service"),
                lines: 25,
                follow: flag.is_some(),
            }
        );
    }
    assert!(parse_controller_command(&["status".into(), "-f".into()]).is_err());
}

#[cfg(unix)]
fn runtime() -> ControllerRuntime {
    ControllerRuntime::new(toml::from_str(&render_default_config().unwrap()).unwrap())
}

#[cfg(unix)]
async fn request(runtime: &mut ControllerRuntime, value: Value) -> BufReader<UnixStream> {
    let (mut client, server) = UnixStream::pair().unwrap();
    write_log_response(&mut client, value).await.unwrap();
    handle_ipc(runtime, server).await.unwrap();
    BufReader::new(client)
}

#[cfg(unix)]
async fn response(reader: &mut BufReader<UnixStream>) -> Value {
    let mut line = String::new();
    assert!(reader.read_line(&mut line).await.unwrap() > 0);
    serde_json::from_str(&line).unwrap()
}

#[cfg(unix)]
#[tokio::test]
async fn follows_new_entries_without_duplicates_or_blocking_control_requests() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut runtime = runtime();
        log_line(&runtime.log, "runner", "old");
        log_line(&runtime.log, "runner", "repeat");
        let (client, server) = UnixStream::pair().unwrap();
        let (mut output, input) = tokio::io::duplex(4096);
        let follower =
            tokio::spawn(async move { follow_controller_logs(client, 1, &mut output).await });
        // Wait for the actual request rather than a scheduling delay.
        server.readable().await.unwrap();
        handle_ipc(&mut runtime, server).await.unwrap();
        let mut output = BufReader::new(input).lines();
        assert_eq!(
            output.next_line().await.unwrap().unwrap(),
            "[runner] repeat"
        );
        log_line(&runtime.log, "runner", "repeat");
        log_line(&runtime.log, "runner", "new");
        assert_eq!(
            output.next_line().await.unwrap().unwrap(),
            "[runner] repeat"
        );
        assert_eq!(output.next_line().await.unwrap().unwrap(), "[runner] new");

        let mut status = request(&mut runtime, json!({"method":"status"})).await;
        assert_eq!(
            response(&mut status).await["result"]["controller"],
            "running"
        );
        let mut snapshot = request(&mut runtime, json!({"method":"logs","lines":1})).await;
        assert_eq!(
            response(&mut snapshot).await["result"]["lines"],
            json!(["[runner] new"])
        );
        assert_eq!(
            snapshot.read_u8().await.unwrap_err().kind(),
            std::io::ErrorKind::UnexpectedEof
        );

        runtime.log_followers.abort_all();
        assert!(follower
            .await
            .unwrap()
            .unwrap_err()
            .contains("disconnected"));
    })
    .await
    .unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn bounds_followers_and_releases_disconnected_clients() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut runtime = runtime();
        let mut clients = Vec::new();
        for _ in 0..LOG_FOLLOWER_LIMIT {
            let mut client = request(&mut runtime, json!({"method":"logs","follow":true})).await;
            assert_eq!(response(&mut client).await["result"]["following"], true);
            clients.push(client);
        }
        let mut rejected = request(&mut runtime, json!({"method":"logs","follow":true})).await;
        assert_eq!(response(&mut rejected).await["ok"], false);
        drop(clients.pop());
        runtime.log_followers.join_next().await.unwrap().unwrap();
        let mut admitted = request(&mut runtime, json!({"method":"logs","follow":true})).await;
        assert_eq!(response(&mut admitted).await["result"]["following"], true);
    })
    .await
    .unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn reports_slow_consumers_instead_of_silently_losing_logs() {
    let (sender, receiver) = broadcast::channel(1);
    sender.send("first".to_string()).unwrap();
    sender.send("second".to_string()).unwrap();
    let mut output = Vec::new();
    stream_log_updates(&mut output, Vec::new(), receiver)
        .await
        .unwrap();
    let packets: Vec<Value> = std::str::from_utf8(&output)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(packets[0]["result"]["following"], true);
    assert_eq!(packets[1]["ok"], false);
    assert!(packets[1]["error"]
        .as_str()
        .unwrap()
        .contains("fell behind"));
}

#[cfg(unix)]
#[tokio::test]
async fn rejects_a_controller_that_only_returns_a_snapshot() {
    let (client, mut server) = UnixStream::pair().unwrap();
    let server = tokio::spawn(async move {
        let mut request = String::new();
        let mut reader = BufReader::new(&mut server);
        reader.read_line(&mut request).await.unwrap();
        write_log_response(&mut server, json!({"ok":true,"result":{"lines":[]}}))
            .await
            .unwrap();
    });
    let error = follow_controller_logs(client, 100, &mut Vec::new())
        .await
        .unwrap_err();
    assert!(error.contains("does not support log following"));
    server.await.unwrap();
}
