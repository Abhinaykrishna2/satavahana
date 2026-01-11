
use hex;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{error, info, warn};

pub struct KiteAuth {
    pub api_key: String,
    pub access_token: String,
}

impl KiteAuth {
    pub fn new(api_key: String, access_token: String) -> Self {
        Self {
            api_key,
            access_token,
        }
    }

    pub fn auth_header(&self) -> String {
        format!("token {}:{}", self.api_key, self.access_token)
    }

    pub fn ws_url(&self) -> String {
        format!(
            "wss://ws.kite.trade/?api_key={}&access_token={}",
            self.api_key, self.access_token
        )
    }

    pub async fn run_login(
        api_key: &str,
        api_secret: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(cached) = load_cached_token() {
            info!("Reusing access token cached from earlier today (.kite_token)");
            return Ok(cached);
        }

        let login_url = format!(
            "https://kite.zerodha.com/connect/login?api_key={}&v=3",
            api_key
        );

        let listener = TcpListener::bind("127.0.0.1:8080").await.map_err(|e| {
            error!("Could not bind to 127.0.0.1:8080 — is another process using port 8080?");
            e
        })?;

        info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        info!("  ZERODHA OAUTH LOGIN");
        info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        info!("  Opening browser for Zerodha login...");
        info!("  If the browser does not open, visit:");
        info!("  {}", login_url);
        info!("  Waiting for callback on http://127.0.0.1:8080/callback");
        info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        open_browser(&login_url);

        let (mut stream, addr) = listener.accept().await?;
        info!("  Callback received from {}", addr);

        let mut buf = vec![0u8; 8192];
        let n = stream.read(&mut buf).await?;
        let raw_request = String::from_utf8_lossy(&buf[..n]);

        let request_token = match extract_request_token(&raw_request) {
            Ok(t) => t,
            Err(e) => {
                let body = format!(
                    "<html><body><h2>Error: {}</h2><p>Check the terminal for details.</p></body></html>",
                    e
                );
                let _ = send_http_response(&mut stream, 400, &body).await;
                return Err(e);
            }
        };

        let success_html = "\
            <html><head><title>Satavahana — Logged In</title></head>\
            <body style=\"font-family:sans-serif;text-align:center;margin-top:80px\">\
            <h2>Login successful!</h2>\
            <p>You can close this tab and return to the terminal.</p>\
            </body></html>";
        let _ = send_http_response(&mut stream, 200, success_html).await;

        info!("  Request token captured. Exchanging for access token...");

        let access_token = exchange_token(api_key, api_secret, &request_token).await?;

        save_cached_token(&access_token);
        info!("  Access token obtained and cached for today (.kite_token).");
        info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        Ok(access_token)
    }
}


fn extract_request_token(
    request: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let first_line = request.lines().next().unwrap_or("");
    let path = first_line.split_whitespace().nth(1).unwrap_or("");

    if let Some(query) = path.split_once('?').map(|(_, q)| q) {
        for pair in query.split('&') {
            if let Some((key, val)) = pair.split_once('=') {
                if key == "request_token" && !val.is_empty() {
                    return Ok(val.to_string());
                }
            }
        }
    }

    error!("  Raw callback request:\n{}", request);
    Err(format!(
        "request_token not found in callback. Did Zerodha return status=success? Path: {}",
        path
    )
    .into())
}

async fn exchange_token(
    api_key: &str,
    api_secret: &str,
    request_token: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let mut hasher = Sha256::new();
    hasher.update(api_key.as_bytes());
    hasher.update(request_token.as_bytes());
    hasher.update(api_secret.as_bytes());
    let checksum = hex::encode(hasher.finalize());

    let client = reqwest::Client::new();
    let body = format!(
        "api_key={}&request_token={}&checksum={}",
        api_key, request_token, checksum
    );

    let resp = client
        .post("https://api.kite.trade/session/token")
        .header("X-Kite-Version", "3")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await?;

    let status = resp.status();
    let json: serde_json::Value = resp.json().await?;

    if !status.is_success() {
        return Err(format!(
            "Kite token exchange failed ({}): {}",
            status,
            json.get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("unknown error")
        )
        .into());
    }

    json["data"]["access_token"]
        .as_str()
        .map(|s| s.to_string())
        .ok_or_else(|| "access_token missing from Kite response".into())
}

fn open_browser(url: &str) {
    let result = if cfg!(target_os = "macos") {
        std::process::Command::new("open").arg(url).spawn()
    } else if cfg!(target_os = "linux") {
        std::process::Command::new("xdg-open").arg(url).spawn()
    } else {
        std::process::Command::new("cmd")
            .args(["/C", "start", url])
            .spawn()
    };

    if result.is_err() {
        warn!("Could not open browser automatically. Please visit the URL above manually.");
    }
}

async fn send_http_response(
    stream: &mut tokio::net::TcpStream,
    status_code: u16,
    body: &str,
) -> Result<(), std::io::Error> {
    let status_text = if status_code == 200 { "OK" } else { "Bad Request" };
    let response = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        status_code, status_text, body.len(), body
    );
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await
}


fn today_ist() -> String {
    use chrono::TimeZone;
    let ist = chrono::FixedOffset::east_opt(5 * 3600 + 30 * 60).unwrap();
    ist.from_utc_datetime(&chrono::Utc::now().naive_utc())
        .format("%Y-%m-%d")
        .to_string()
}

fn load_cached_token() -> Option<String> {
    let content = std::fs::read_to_string(".kite_token").ok()?;
    let mut lines = content.lines();
    let date = lines.next()?;
    let token = lines.next()?;
    if date == today_ist() && !token.is_empty() {
        Some(token.to_string())
    } else {
        None
    }
}

fn save_cached_token(token: &str) {
    let content = format!("{}\n{}\n", today_ist(), token);
    if let Err(e) = std::fs::write(".kite_token", &content) {
        warn!("Could not write .kite_token cache: {}", e);
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_header() {
        let auth = KiteAuth::new("abc123".into(), "tok456".into());
        assert_eq!(auth.auth_header(), "token abc123:tok456");
    }

    #[test]
    fn test_ws_url() {
        let auth = KiteAuth::new("abc123".into(), "tok456".into());
        assert_eq!(
            auth.ws_url(),
            "wss://ws.kite.trade/?api_key=abc123&access_token=tok456"
        );
    }

    #[test]
    fn test_extract_request_token_ok() {
        let raw = "GET /callback?request_token=abc123xyz&status=success HTTP/1.1\r\nHost: 127.0.0.1:8080\r\n";
        let tok = extract_request_token(raw).unwrap();
        assert_eq!(tok, "abc123xyz");
    }

    #[test]
    fn test_extract_request_token_missing() {
        let raw = "GET /callback?status=success HTTP/1.1\r\n";
        assert!(extract_request_token(raw).is_err());
    }

    #[test]
    fn test_extract_request_token_order_independent() {
        let raw = "GET /callback?status=success&request_token=tok999 HTTP/1.1\r\n";
        let tok = extract_request_token(raw).unwrap();
        assert_eq!(tok, "tok999");
    }
}
