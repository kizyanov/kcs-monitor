//! Минимальный HTTPS-клиент (HTTP/1.1 поверх rustls/ring) с общим на процесс
//! ограничителем скорости и обработкой 429.
//!
//! Нужен только для двух простых запросов к KuCoin REST, поэтому тяжёлые
//! HTTP-крейты (reqwest/hyper) не подключаем: это держит Docker-сборку без
//! C-инструментов (провайдер ring собирается обычным gcc, cmake не нужен).
//!
//! KuCoin ограничивает не число запросов, а их «вес»: публичный пул считается
//! по IP и для VIP0 равен 4000 weight / 30 с (≈133 вес/с). Вес конкретного
//! эндпоинта указан в документации (`/api/v1/market/candles` — 3,
//! `/api/v1/symbols` — 4). При превышении квоты биржа отвечает HTTP 429 с
//! кодом 429000 и заголовками `gw-ratelimit-*`, поэтому тормозим заранее
//! (token bucket) и дополнительно слушаем подсказки сервера.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use rustls::ClientConfig;
use rustls::pki_types::ServerName;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_rustls::TlsConnector;

/// Пауза при 429, если сервер не подсказал время ожидания
/// (серверная перегрузка отдаёт 429 без заголовков `gw-ratelimit-*`).
pub const DEFAULT_429_PAUSE: Duration = Duration::from_secs(5);
/// Максимум, на который мы готовы встать по подсказке сервера.
const MAX_PAUSE: Duration = Duration::from_secs(60);
/// Максимальная пауза «остатка квоты мало, ждём сброса окна».
const MAX_QUOTA_WAIT: Duration = Duration::from_secs(5);

/// Ошибка «биржа ответила 429»: несёт время ожидания, которое уже применено
/// ко всем задачам процесса через [`RateLimiter::pause`].
#[derive(Debug, Clone)]
pub struct RateLimited {
    /// Сколько процесс поставлен на паузу.
    pub retry_after: Duration,
    /// true — сервер сам назвал время (заголовки), false — взяли дефолт.
    pub hinted: bool,
}

impl std::fmt::Display for RateLimited {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "HTTP 429 (слишком много запросов), пауза {:.1}s{}",
            self.retry_after.as_secs_f64(),
            if self.hinted { "" } else { " (оценка)" }
        )
    }
}

impl std::error::Error for RateLimited {}

/// Состояние token bucket'а: накопленный вес и момент последнего пересчёта.
struct Bucket {
    tokens: f64,
    last: Instant,
}

/// Начисляет вес за прошедшее время и резервирует `weight`.
///
/// Резерв делается сразу, даже «в долг» (токены уходят в минус): так несколько
/// параллельных задач выстраиваются в очередь по фактическому темпу, а не
/// просыпаются одновременно и не бьют залпом.
fn reserve(bucket: &mut Bucket, rate: f64, capacity: f64, weight: f64, now: Instant) -> Duration {
    let elapsed = now.saturating_duration_since(bucket.last).as_secs_f64();
    bucket.tokens = (bucket.tokens + elapsed * rate).min(capacity);
    bucket.last = now;

    let deficit = weight - bucket.tokens;
    bucket.tokens -= weight;
    if deficit > 0.0 && rate > 0.0 {
        Duration::from_secs_f64(deficit / rate)
    } else {
        Duration::ZERO
    }
}

/// Общий на процесс ограничитель: не чаще `rate` единиц веса в секунду
/// (плюс короткий всплеск до `capacity`).
pub struct RateLimiter {
    rate: f64,
    capacity: f64,
    state: Mutex<State>,
}

struct State {
    bucket: Bucket,
    /// До этого момента запросы не отправляем (общая пауза после 429).
    pause_until: Option<Instant>,
}

impl RateLimiter {
    /// `weight_per_sec` — допустимый вес запросов в секунду.
    pub fn new(weight_per_sec: f64) -> Self {
        let rate = if weight_per_sec.is_finite() && weight_per_sec > 0.0 {
            weight_per_sec
        } else {
            1.0
        };
        // Всплеск — секундная норма: сглаживает старт, но не даёт залпов.
        let capacity = rate;
        Self {
            rate,
            capacity,
            state: Mutex::new(State {
                bucket: Bucket {
                    tokens: capacity,
                    last: Instant::now(),
                },
                pause_until: None,
            }),
        }
    }

    /// Ждёт своей очереди за `weight` единиц квоты.
    pub async fn acquire(&self, weight: f64) {
        let weight = if weight.is_finite() && weight > 0.0 {
            weight
        } else {
            0.0
        };
        loop {
            // 1. Общая пауза: ждём её окончания, квоту не резервируем.
            let pause = {
                let mut st = self.state.lock().await;
                let now = Instant::now();
                match st.pause_until {
                    Some(until) if until > now => Some(until - now),
                    Some(_) => {
                        // Вышли из паузы: время простоя в запас не зачисляем,
                        // иначе все задачи стартовали бы залпом.
                        st.pause_until = None;
                        st.bucket.last = now;
                        None
                    }
                    None => None,
                }
            };
            if let Some(wait) = pause {
                tokio::time::sleep(wait).await;
                continue; // паузу могли продлить, пока спали
            }

            // 2. Резервируем свой вес и, если нужно, добираем паузу по темпу.
            let wait = {
                let mut st = self.state.lock().await;
                reserve(
                    &mut st.bucket,
                    self.rate,
                    self.capacity,
                    weight,
                    Instant::now(),
                )
            };
            if !wait.is_zero() {
                tokio::time::sleep(wait).await;
            }
            return;
        }
    }

    /// Общая пауза для всех задач (после 429). Повторные вызовы не укорачивают
    /// уже установленную паузу.
    pub async fn pause(&self, wait: Duration) {
        let wait = wait.min(MAX_PAUSE);
        let mut st = self.state.lock().await;
        let until = Instant::now() + wait;
        if st.pause_until.is_none_or(|cur| cur < until) {
            st.pause_until = Some(until);
        }
    }

    /// Подсказка из заголовков успешного ответа: если остатка квоты почти нет,
    /// ждём сброса окна, не доводя до 429.
    pub async fn observe_quota(&self, remaining: f64, reset: Duration, weight: f64) {
        if remaining.is_finite() && remaining < weight * 2.0 {
            let wait = reset.min(MAX_QUOTA_WAIT);
            if !wait.is_zero() {
                self.pause(wait).await;
            }
        }
    }
}

/// Заголовки + тело ответа.
struct Response {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Response {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// Сколько ждать после 429: сначала `Retry-After`, затем `gw-ratelimit-reset`.
fn retry_after_hint(resp: &Response) -> Option<Duration> {
    if let Some(v) = resp.header("retry-after")
        && let Ok(secs) = v.trim().parse::<f64>()
        && secs.is_finite()
        && secs >= 0.0
    {
        return Some(Duration::from_secs_f64(secs).min(MAX_PAUSE));
    }
    if let Some(v) = resp.header("gw-ratelimit-reset")
        && let Ok(ms) = v.trim().parse::<u64>()
    {
        return Some(Duration::from_millis(ms).min(MAX_PAUSE));
    }
    None
}

/// Остаток квоты публичного пула и время до её сброса (из заголовков ответа).
fn quota_hint(resp: &Response) -> Option<(f64, Duration)> {
    let remaining = resp
        .header("gw-ratelimit-remaining")?
        .trim()
        .parse::<f64>()
        .ok()?;
    let reset_ms = resp
        .header("gw-ratelimit-reset")
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(0);
    Some((remaining, Duration::from_millis(reset_ms)))
}

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

/// HTTP-клиент к бирже: один на процесс, держит общий лимитер.
pub struct HttpClient {
    api_base: String,
    limiter: Arc<RateLimiter>,
    tls: Arc<ClientConfig>,
}

impl HttpClient {
    pub fn new(api_base: &str, limiter: Arc<RateLimiter>) -> Result<Self> {
        Ok(Self {
            api_base: api_base.to_string(),
            limiter,
            tls: tls_config()?,
        })
    }

    /// Выполняет HTTPS GET-запрос и возвращает тело ответа.
    async fn fetch(&self, path: &str) -> Result<Response> {
        let (host, prefix) = split_base(&self.api_base)?;
        let req_path = format!("{prefix}{path}");

        let connector = TlsConnector::from(self.tls.clone());
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

    /// GET-запрос, ответ парсится как JSON. Перед отправкой запрашивает у
    /// лимитера `weight` единиц квоты; 429 превращает в [`RateLimited`] и
    /// ставит на паузу все задачи процесса.
    pub async fn get_json(&self, path: &str, weight: f64) -> Result<serde_json::Value> {
        self.limiter.acquire(weight).await;
        let resp = self.fetch(path).await?;

        match resp.status {
            200..=299 => {}
            429 => {
                let hint = retry_after_hint(&resp);
                let pause = hint.unwrap_or(DEFAULT_429_PAUSE);
                self.limiter.pause(pause).await;
                return Err(RateLimited {
                    retry_after: pause,
                    hinted: hint.is_some(),
                }
                .into());
            }
            status => bail!("HTTP {status}: {}", body_preview(&resp.body)),
        }

        if let Some((remaining, reset)) = quota_hint(&resp) {
            self.limiter.observe_quota(remaining, reset, weight).await;
        }
        parse_json(&resp.body)
    }
}

/// Отделяет заголовки от тела, декодирует chunked.
fn split_http_response(raw: &[u8]) -> Result<Response> {
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

    let headers: Vec<(String, String)> = lines
        .filter_map(|line| {
            let (name, value) = line.split_once(':')?;
            Some((name.trim().to_string(), value.trim().to_string()))
        })
        .collect();

    let body = &raw[header_end + 4..];
    let chunked = headers
        .iter()
        .any(|(k, _)| k.eq_ignore_ascii_case("transfer-encoding"));
    let body = if chunked {
        decode_chunked(body)?
    } else {
        body.to_vec()
    };

    Ok(Response {
        status,
        headers,
        body,
    })
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

/// Первые байты ответа для сообщения об ошибке.
fn body_preview(body: &[u8]) -> String {
    String::from_utf8_lossy(body).chars().take(300).collect()
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

/// Небольшой джиттер (0..1000 мс) для разведения повторных попыток: источник —
/// время + счётчик, отдельный крейт генератора случайных чисел не нужен.
pub fn jitter_ms() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0x9E37_79B9_7F4A_7C15);
    let counter = COUNTER.fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::from(d.subsec_nanos()))
        .unwrap_or(0);
    let mut x = nanos ^ counter;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    (x.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 33) % 1000
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resp(status: u16, headers: &[(&str, &str)]) -> Response {
        Response {
            status,
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            body: Vec::new(),
        }
    }

    #[test]
    fn bucket_paces_by_weight() {
        let now = Instant::now();
        let rate = 10.0;
        let capacity = 10.0;
        let mut bucket = Bucket {
            tokens: capacity,
            last: now,
        };

        // Первые 10 единиц веса уходят мгновенно (всплеск = секундная норма).
        for i in 0..10 {
            let wait = reserve(&mut bucket, rate, capacity, 1.0, now);
            assert!(wait.is_zero(), "запрос {i} не должен ждать");
        }
        // 11-я — уже в долг: 1 токен при 10 в секунду = 100 мс.
        let wait = reserve(&mut bucket, rate, capacity, 1.0, now);
        assert!(
            (wait.as_secs_f64() - 0.1).abs() < 1e-6,
            "ожидали 100 мс, получили {wait:?}"
        );

        // За 200 мс накопилось 2 токена — снова можно без ожидания.
        let later = now + Duration::from_millis(200);
        let wait = reserve(&mut bucket, rate, capacity, 1.0, later);
        assert!(wait.is_zero(), "после паузы ждать не нужно: {wait:?}");
    }

    #[test]
    fn bucket_does_not_exceed_capacity() {
        let now = Instant::now();
        let mut bucket = Bucket {
            tokens: 0.0,
            last: now,
        };
        // Час простоя не должен давать право на залп больше секундной нормы.
        reserve(
            &mut bucket,
            10.0,
            10.0,
            0.0,
            now + Duration::from_secs(3600),
        );
        assert!(bucket.tokens <= 10.0);
    }

    #[test]
    fn retry_after_prefers_header_then_reset() {
        assert_eq!(
            retry_after_hint(&resp(429, &[("Retry-After", "3")])),
            Some(Duration::from_secs(3))
        );
        assert_eq!(
            retry_after_hint(&resp(429, &[("gw-ratelimit-reset", "1500")])),
            Some(Duration::from_millis(1500))
        );
        // Retry-After важнее.
        assert_eq!(
            retry_after_hint(&resp(
                429,
                &[("Retry-After", "1"), ("gw-ratelimit-reset", "9000")]
            )),
            Some(Duration::from_secs(1))
        );
        // Серверная перегрузка: заголовков нет — оценку берём свою.
        assert_eq!(retry_after_hint(&resp(429, &[])), None);
    }

    #[test]
    fn retry_after_is_capped() {
        assert_eq!(
            retry_after_hint(&resp(429, &[("Retry-After", "100000")])),
            Some(MAX_PAUSE)
        );
    }

    #[test]
    fn quota_hint_reads_headers() {
        let r = resp(
            200,
            &[
                ("gw-ratelimit-limit", "4000"),
                ("gw-ratelimit-remaining", "17"),
                ("gw-ratelimit-reset", "480"),
            ],
        );
        assert_eq!(
            quota_hint(&r),
            Some((17.0, Duration::from_millis(480))),
            "остаток и время сброса окна"
        );
        assert_eq!(quota_hint(&resp(200, &[])), None);
    }

    #[tokio::test]
    async fn limiter_survives_zero_rate() {
        // Некорректная настройка не должна превращаться в нулевой интервал.
        let limiter = RateLimiter::new(0.0);
        limiter.acquire(3.0).await;
    }

    #[tokio::test]
    async fn pause_holds_all_tasks() {
        let limiter = RateLimiter::new(1000.0);
        limiter.pause(Duration::from_millis(120)).await;
        let started = Instant::now();
        limiter.acquire(1.0).await;
        assert!(
            started.elapsed() >= Duration::from_millis(100),
            "запрос прошёл сквозь паузу: {:?}",
            started.elapsed()
        );
    }

    #[test]
    fn jitter_stays_in_range() {
        for _ in 0..100 {
            assert!(jitter_ms() < 1000);
        }
    }
}
