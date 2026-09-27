//! Native gateway auth: persist refreshed credentials, then mint one WS ticket.
use crate::acp::AcpError;
use serde::{Deserialize, Serialize};
use std::{
    io::Write,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;

/// Each bridge uses its own private file; all pool connections share this lock.
pub struct ServeCredentials {
    path: PathBuf,
    refresh: Mutex<()>,
}
#[derive(Deserialize, Serialize)]
struct NativeTokens {
    serve_url: String,
    access_token: String,
    refresh_token: String,
    expires_at: u64,
    provider: String,
}
fn auth_error(message: impl Into<String>) -> AcpError {
    AcpError::ServeUnavailable(format!("Serve native authentication: {}", message.into()))
}
impl ServeCredentials {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            refresh: Mutex::new(()),
        }
    }

    fn read(&self, target: &url::Url) -> Result<NativeTokens, AcpError> {
        let metadata = std::fs::symlink_metadata(&self.path)
            .map_err(|_| auth_error("cannot read credential file"))?;
        if !metadata.is_file() {
            return Err(auth_error("credential path must be a regular file"));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o077 != 0 {
                return Err(auth_error(
                    "credential file must not be group/world accessible (use 0600)",
                ));
            }
        }
        let raw =
            std::fs::read(&self.path).map_err(|_| auth_error("cannot read credential file"))?;
        let tokens: NativeTokens = serde_json::from_slice(&raw)
            .map_err(|_| auth_error("invalid credential file format"))?;
        if url::Url::parse(&tokens.serve_url).ok().as_ref() != Some(target) {
            return Err(auth_error("credential endpoint does not match --serve-url"));
        }
        if tokens.access_token.is_empty()
            || tokens.refresh_token.is_empty()
            || tokens.provider.is_empty()
        {
            return Err(auth_error(
                "credentials require access_token, refresh_token, and provider",
            ));
        }
        Ok(tokens)
    }
    fn persist(&self, tokens: &NativeTokens) -> Result<(), AcpError> {
        let parent = self
            .path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| std::path::Path::new("."));
        let temporary = parent.join(format!(".serve-credentials-{}.tmp", uuid::Uuid::new_v4()));
        let result = (|| -> std::io::Result<()> {
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(&temporary)?;
            file.write_all(&serde_json::to_vec(tokens)?)?;
            file.sync_all()?;
            std::fs::rename(&temporary, &self.path)?;
            #[cfg(unix)]
            std::fs::File::open(parent)?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(temporary);
        }
        result.map_err(|_| {
            auth_error("cannot persist rotated credentials; operator reconciliation required")
        })
    }
    async fn rotate(
        &self,
        client: &reqwest::Client,
        base: &str,
        tokens: &mut NativeTokens,
    ) -> Result<(), AcpError> {
        let response = client.post(format!("{base}/auth/native/refresh"))
            .json(&serde_json::json!({"refresh_token":tokens.refresh_token,"provider":tokens.provider}))
            .send().await.map_err(|_| auth_error("refresh transport failed; credentials retained"))?;
        if !response.status().is_success() {
            return Err(auth_error(format!(
                "refresh returned HTTP {}; credentials retained",
                response.status().as_u16()
            )));
        }
        let mut refreshed: serde_json::Value = response
            .json()
            .await
            .map_err(|_| auth_error("invalid refresh response"))?;
        refreshed["serve_url"] = serde_json::json!(tokens.serve_url);
        let refreshed: NativeTokens = serde_json::from_value(refreshed)
            .map_err(|_| auth_error("incomplete refresh response"))?;
        if refreshed.access_token.is_empty()
            || refreshed.refresh_token.is_empty()
            || refreshed.provider.is_empty()
        {
            return Err(auth_error("empty refreshed credentials"));
        }
        self.persist(&refreshed)?;
        *tokens = refreshed;
        Ok(())
    }
    /// The upgrade receives only a short-lived ticket, never a native bearer.
    pub async fn ticket(&self, target: &url::Url) -> Result<String, AcpError> {
        let _guard = self.refresh.lock().await;
        let mut base = target.clone();
        let scheme = if base.scheme() == "wss" {
            "https"
        } else {
            "http"
        };
        if scheme == "http"
            && !matches!(
                base.host_str(),
                Some("127.0.0.1" | "localhost" | "[::1]" | "::1")
            )
        {
            return Err(auth_error(
                "native credentials require TLS except on loopback",
            ));
        }
        base.set_scheme(scheme)
            .map_err(|_| auth_error("invalid endpoint scheme"))?;
        let prefix = base
            .path()
            .strip_suffix("/api/ws")
            .ok_or_else(|| auth_error("endpoint must end with /api/ws"))?
            .to_owned();
        base.set_path(&prefix);
        let base = base.as_str().trim_end_matches('/');
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(15))
            .build()
            .map_err(|_| auth_error("cannot initialize HTTP transport"))?;
        let mut tokens = self.read(target)?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| auth_error("invalid system clock"))?
            .as_secs();
        if tokens.expires_at <= now.saturating_add(60) {
            self.rotate(&client, base, &mut tokens).await?;
        }
        for attempt in 0..2 {
            let response = client
                .post(format!("{base}/api/auth/ws-ticket"))
                .bearer_auth(&tokens.access_token)
                .send()
                .await
                .map_err(|_| auth_error("ticket transport failed"))?;
            if response.status() == reqwest::StatusCode::UNAUTHORIZED && attempt == 0 {
                self.rotate(&client, base, &mut tokens).await?;
                continue;
            }
            if !response.status().is_success() {
                return Err(auth_error(format!(
                    "ticket returned HTTP {}",
                    response.status().as_u16()
                )));
            }
            let payload: serde_json::Value = response
                .json()
                .await
                .map_err(|_| auth_error("invalid ticket response"))?;
            return payload["ticket"]
                .as_str()
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .ok_or_else(|| auth_error("missing WebSocket ticket"));
        }
        Err(auth_error("ticket rejected after refresh"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hermes_serve::{ServeClient, ServeConfig};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn forbidden_ticket_does_not_refresh_or_fall_back_to_legacy_auth() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let url = format!("ws://{address}/api/ws");
        let path =
            std::env::temp_dir().join(format!("buzz-native-auth-{}.json", uuid::Uuid::new_v4()));
        let auth = Arc::new(ServeCredentials::new(path.clone()));
        auth.persist(&NativeTokens {
            serve_url: url.clone(),
            access_token: "synthetic-access".into(),
            refresh_token: "synthetic-refresh".into(),
            provider: "synthetic".into(),
            expires_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 3600,
        })
        .unwrap();
        let before = std::fs::read(&path).unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            drop(listener);
            let mut request = Vec::new();
            loop {
                let mut buffer = [0u8; 1024];
                let count = stream.read(&mut buffer).await.unwrap();
                assert!(count > 0);
                request.extend_from_slice(&buffer[..count]);
                if request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                    break;
                }
            }
            let request = String::from_utf8(request).unwrap();
            assert!(request.starts_with("POST /api/auth/ws-ticket "));
            assert!(request
                .to_ascii_lowercase()
                .contains("authorization: bearer synthetic-access"));
            assert!(!request.contains("synthetic-refresh"));
            stream
                .write_all(
                    b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
        });
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            ServeClient::connect(ServeConfig {
                url,
                profile: "isolated".into(),
                token: Some("must-not-fall-back".into()),
                credentials: Some(auth),
                model: None,
                effort: None,
            }),
        )
        .await
        .unwrap();
        let error = result
            .err()
            .expect("forbidden native credential must fail")
            .to_string();
        assert!(error.contains("HTTP 403"));
        assert!(!error.contains("synthetic-access") && !error.contains("synthetic-refresh"));
        assert_eq!(std::fs::read(&path).unwrap(), before);
        server.await.unwrap();
        std::fs::remove_file(path).unwrap();
    }
}
