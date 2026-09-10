//! Минимальный HTTPS-клиент (HTTP/1.1 поверх rustls/ring).
//!
//! Нужен только для двух простых запросов к KuCoin REST, поэтому тяжёлые
//! HTTP-крейты (reqwest/hyper) не подключаем: это держит Docker-сборку без
//! C-инструментов (провайдер ring собирается обычным gcc, cmake не нужен).

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use rustls::ClientConfig;
use rustls::pki_types::ServerName;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

fn tls_config() -> Result<Arc<ClientConfig>> {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let cfg = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(Arc::new(cfg))
}

/// Разбивает `https://host/base` на (host, "/base").
fn split_base(api_base: &str) -> Result<(&str, String)> {
    let rest = api_base
        .strip_prefix("https://")
        .context("KCS_API_BASE должен начинаться с https://")?;
    match rest.split_once('/') {
        Some((host, path)) => Ok((host, format!("/{path}"))),
        None => Ok((rest, String::new())),
    }
}

/// Выполняет HTTPS GET-запрос и возвращает тело ответа.
pub async fn request(api_base: &str, path: &str) -> Result<Vec<u8>> {
    let (host, prefix) = split_base(api_base)?;
    let req_path = format!("{prefix}{path}");

    let connector = TlsConnector::from(tls_config()?);
    let server_name = ServerName::try_from(host.to_string()).context("некорректный host")?;
    let tcp = TcpStream::connect((host, 443))
        .await
        .context("TCP connect")?;
    let mut stream = connector
        .connect(server_name, tcp)
        .await
        .context("TLS handshake")?;

    let request = format!(
        "GET {req_path} HTTP/1.1\r\n\
         Host: {host}\r\n\
         User-Agent: kcs-monitor/0.1\r\n\
         Accept: application/json\r\n\
         Connection: close\r\n\r\n"
    );
    stream.write_all(request.as_bytes()).await?;
    stream.flush().await?;

    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(30), stream.read_to_end(&mut response))
        .await
        .context("HTTP read timeout")??;

    split_http_response(&response)
}

/// Отделяет заголовки от тела, декодирует chunked, проверяет статус-код.
fn split_http_response(raw: &[u8]) -> Result<Vec<u8>> {
    let header_end = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .context("HTTP-ответ без конца заголовков")?;
    let head = std::str::from_utf8(&raw[..header_end]).context("заголовки не UTF-8")?;
    let mut lines = head.lines();
    let status_line = lines.next().context("пустой ответ")?;
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .context("нет статус-кода")?;

    let body = &raw[header_end + 4..];
    if !(200..300).contains(&status) {
        let text = String::from_utf8_lossy(body);
        bail!(
            "HTTP {status}: {}",
            text.chars().take(300).collect::<String>()
        );
    }

    let chunked = head
        .lines()
        .any(|l| l.to_ascii_lowercase().starts_with("transfer-encoding:"));
    if chunked {
        decode_chunked(body)
    } else {
        Ok(body.to_vec())
    }
}

/// Декодирует тело в формате HTTP chunked.
fn decode_chunked(mut body: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    loop {
        // Строка размера чанка (hex), возможны расширения после ';'.
        let line_end = body
            .windows(2)
            .position(|w| w == b"\r\n")
            .context("chunked: нет конца строки размера")?;
        let size_line =
            std::str::from_utf8(&body[..line_end]).context("chunked: размер не UTF-8")?;
        let size_hex = size_line.split(';').next().unwrap_or("").trim();
        let size = usize::from_str_radix(size_hex, 16).context("chunked: неверный размер")?;
        body = &body[line_end + 2..];

        if size == 0 {
            // Пропускаем trailer-заголовки до пустой строки.
            while let Some(pos) = body.windows(2).position(|w| w == b"\r\n") {
                if pos == 0 {
                    break;
                }
                body = &body[pos + 2..];
            }
            return Ok(out);
        }
        if body.len() < size + 2 {
            bail!("chunked: тело короче заявленного");
        }
        out.extend_from_slice(&body[..size]);
        body = &body[size + 2..]; // CRLF после данных чанка
    }
}

/// Парсит тело как JSON; при неудаче показывает первые байты ответа.
fn parse_json(body: &[u8]) -> Result<serde_json::Value> {
    serde_json::from_slice(body).with_context(|| {
        let preview = body
            .iter()
            .take(160)
            .map(|b| format!("{b:02x}"))
            .collect::<Vec<_>>()
            .join(" ");
        format!("ответ не JSON ({} байт, начало: {preview})", body.len())
    })
}

/// GET-запрос, ответ парсится как JSON.
pub async fn get_json(api_base: &str, path: &str) -> Result<serde_json::Value> {
    let body = request(api_base, path).await?;
    parse_json(&body)
}
