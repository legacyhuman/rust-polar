//! Polar AccessLink transport primitives.

pub mod activity;
pub mod credentials;
pub mod exercise;
pub mod oauth;
pub mod physiology;
pub mod profile;
pub mod recovery;
pub mod resources;
pub mod sync;

use crate::archive;
use bytes::{Buf, Bytes};
use futures_util::StreamExt;
use http_body_util::BodyExt;
use hyper::body::Body;
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::{client::legacy::Client, rt::TokioExecutor};
use std::io::{self, Read};

pub type AccessLinkClient =
    Client<HttpsConnector<HttpConnector>, http_body_util::Empty<bytes::Bytes>>;

#[derive(Clone, Debug, Default)]
pub struct AccessLinkResponseHeaders {
    pub retry_after: Option<String>,
    pub link: Option<String>,
    pub rate_limits: std::collections::BTreeMap<String, String>,
}

/// Build an HTTPS-only client with the explicitly selected RustCrypto
/// provider. Certificate validation remains enabled through bundled Mozilla
/// roots. No system TLS or C-backed crypto provider is selected.
pub fn http_client() -> Result<AccessLinkClient, rustls::Error> {
    let provider = rustls_rustcrypto::provider();
    let connector = HttpsConnectorBuilder::new()
        .with_provider_and_webpki_roots(provider)?
        .https_only()
        .enable_http1()
        .build();
    Ok(Client::builder(TokioExecutor::new()).build(connector))
}

/// Fetch and durably archive one AccessLink HTTP response without buffering it
/// in memory. The caller must pass only redacted request metadata. HTTP status
/// and error bodies are also archived so retry/auth diagnosis remains possible.
pub async fn fetch_and_archive(
    client: &AccessLinkClient,
    archive: archive::ArchiveStore,
    url: &str,
    access_token: &str,
    resource: &str,
    request_metadata: archive::RequestMetadata,
    fetched_at: &str,
) -> Result<
    (archive::ArchiveReceipt, AccessLinkResponseHeaders),
    Box<dyn std::error::Error + Send + Sync>,
> {
    fetch_and_archive_with_connector(
        client,
        archive,
        url,
        access_token,
        resource,
        request_metadata,
        fetched_at,
    )
    .await
}

/// Transport-generic response handling shared with tests. Production only
/// reaches this through [`fetch_and_archive`] and its HTTPS/WebPKI connector;
/// tests may use a loopback HTTP connector without changing production trust.
async fn fetch_and_archive_with_connector<C>(
    client: &Client<C, http_body_util::Empty<Bytes>>,
    archive: archive::ArchiveStore,
    url: &str,
    access_token: &str,
    resource: &str,
    request_metadata: archive::RequestMetadata,
    fetched_at: &str,
) -> Result<
    (archive::ArchiveReceipt, AccessLinkResponseHeaders),
    Box<dyn std::error::Error + Send + Sync>,
>
where
    C: hyper_util::client::legacy::connect::Connect + Clone + Send + Sync + 'static,
{
    let uri = url.parse::<hyper::Uri>()?;
    let request = hyper::Request::get(uri)
        .header(
            hyper::header::AUTHORIZATION,
            format!("Bearer {access_token}"),
        )
        .body(http_body_util::Empty::<Bytes>::new())?;
    let response = client.request(request).await?;
    archive_response(response, archive, resource, request_metadata, fetched_at).await
}

/// Archive a response through the production streaming path. Generic over the
/// Hyper body type so local HTTP tests can exercise this without weakening the
/// HTTPS-only production connector.
async fn archive_response<B>(
    response: hyper::Response<B>,
    archive: archive::ArchiveStore,
    resource: &str,
    request_metadata: archive::RequestMetadata,
    fetched_at: &str,
) -> Result<
    (archive::ArchiveReceipt, AccessLinkResponseHeaders),
    Box<dyn std::error::Error + Send + Sync>,
>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    let status = response.status().as_u16();
    let mut headers = AccessLinkResponseHeaders {
        retry_after: response
            .headers()
            .get(hyper::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned),
        link: response
            .headers()
            .get(hyper::header::LINK)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned),
        rate_limits: std::collections::BTreeMap::new(),
    };
    for name in [
        "x-rate-limit-limit",
        "x-rate-limit-remaining",
        "x-rate-limit-reset",
    ] {
        if let Some(value) = response
            .headers()
            .get(name)
            .and_then(|value| value.to_str().ok())
        {
            headers
                .rate_limits
                .insert(name.to_owned(), value.to_owned());
        }
    }
    let body = response.into_body();

    let (sender, receiver) = tokio::sync::mpsc::channel::<io::Result<Bytes>>(4);
    let source_archive = archive.clone();
    let resource = resource.to_owned();
    let fetched_at = fetched_at.to_owned();
    let writer = tokio::task::spawn_blocking(move || {
        source_archive.store_body(
            "polar",
            resource,
            request_metadata,
            status,
            fetched_at,
            BodyChannelReader {
                receiver,
                current: Bytes::new(),
            },
        )
    });

    let stream = body.into_data_stream();
    futures_util::pin_mut!(stream);
    let mut completed = true;
    while let Some(item) = stream.next().await {
        match item {
            Ok(bytes) => {
                // Bound message sizes as well as queue length; one oversized
                // HTTP frame cannot defeat the byte budget.
                for chunk in bytes.chunks(64 * 1024) {
                    sender
                        .send(Ok(Bytes::copy_from_slice(chunk)))
                        .await
                        .map_err(|_| io::Error::other("archive writer stopped"))?;
                }
            }
            Err(error) => {
                let _ = sender.send(Err(io::Error::other(error.to_string()))).await;
                completed = false;
                break;
            }
        }
    }
    if completed {
        // An explicit EOF marker distinguishes a complete HTTP body from a
        // cancelled async task that merely dropped its sender.
        sender
            .send(Ok(Bytes::new()))
            .await
            .map_err(|_| io::Error::other("archive writer stopped"))?;
    }
    drop(sender);
    Ok((writer.await??, headers))
}

struct BodyChannelReader {
    receiver: tokio::sync::mpsc::Receiver<io::Result<Bytes>>,
    current: Bytes,
}

impl Read for BodyChannelReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        loop {
            if !self.current.is_empty() {
                let count = out.len().min(self.current.len());
                out[..count].copy_from_slice(&self.current[..count]);
                self.current.advance(count);
                return Ok(count);
            }
            match self.receiver.blocking_recv() {
                Some(Ok(bytes)) if bytes.is_empty() => return Ok(0),
                Some(Ok(bytes)) => self.current = bytes,
                Some(Err(error)) => return Err(error),
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "HTTP body producer stopped before end-of-body marker",
                    ));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{convert::Infallible, io, path::PathBuf};

    use bytes::Bytes;
    use futures_util::StreamExt;
    use http_body_util::{Empty, StreamBody};
    use hyper::{Request, Response, body::Frame};
    use hyper_util::{
        client::legacy::Client,
        rt::{TokioExecutor, TokioIo},
    };

    type LocalClient = Client<super::HttpConnector, Empty<Bytes>>;

    fn local_client() -> LocalClient {
        let mut connector = super::HttpConnector::new();
        connector.enforce_http(true);
        Client::builder(TokioExecutor::new()).build(connector)
    }

    async fn start_http_server<B>(response: Response<B>) -> (String, tokio::task::JoinHandle<()>)
    where
        B: hyper::body::Body<Data = Bytes> + Send + 'static,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let response = std::sync::Arc::new(std::sync::Mutex::new(Some(response)));
            let service =
                hyper::service::service_fn(move |_request: Request<hyper::body::Incoming>| {
                    let response = response
                        .lock()
                        .unwrap()
                        .take()
                        .expect("one request per test server");
                    async move { Ok::<_, Infallible>(response) }
                });
            hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await
                .unwrap_or_else(|error| {
                    // A deliberately interrupted/cancelled response closes the
                    // connection with an error, which is expected in those tests.
                    let message = error.to_string();
                    if !message.contains("closed") && !message.contains("error") {
                        panic!("loopback HTTP server failed: {message}");
                    }
                });
        });
        (format!("http://{address}/v3/test"), task)
    }

    fn test_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    fn temp_archive(label: &str) -> (PathBuf, crate::archive::ArchiveStore) {
        let root = std::env::temp_dir().join(format!(
            "rust-polar-http-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let archive = crate::archive::ArchiveStore::new(root.clone()).with_min_free_bytes(0);
        (root, archive)
    }

    #[test]
    fn builds_https_client_with_explicit_rustcrypto_provider() {
        let _client = super::http_client().unwrap();
    }

    #[test]
    fn loopback_http_stream_archives_chunks_and_captures_response_headers() {
        test_runtime().block_on(async {
            let body = StreamBody::new(futures_util::stream::iter([
                Ok::<_, io::Error>(Frame::data(Bytes::from_static(b"first-"))),
                Ok(Frame::data(Bytes::from_static(b"second-"))),
                Ok(Frame::data(Bytes::from_static(b"third"))),
            ]));
            let response = Response::builder()
                .status(429)
                .header(hyper::header::RETRY_AFTER, "17")
                .header(
                    hyper::header::LINK,
                    "<https://api.polar.com/next>; rel=next",
                )
                .header("x-rate-limit-limit", "20")
                .header("x-rate-limit-remaining", "3")
                .header("x-rate-limit-reset", "1700000000")
                .body(body)
                .unwrap();
            let (url, server) = start_http_server(response).await;
            let (root, archive) = temp_archive("chunks");
            let (receipt, headers) = super::fetch_and_archive_with_connector(
                &local_client(),
                archive.clone(),
                &url,
                "test-token",
                "test-resource",
                Default::default(),
                "test-time",
            )
            .await
            .unwrap();
            server.await.unwrap();

            assert_eq!(receipt.status, 429);
            assert_eq!(headers.retry_after.as_deref(), Some("17"));
            assert_eq!(
                headers.link.as_deref(),
                Some("<https://api.polar.com/next>; rel=next")
            );
            assert_eq!(headers.rate_limits.get("x-rate-limit-limit").unwrap(), "20");
            assert_eq!(
                headers.rate_limits.get("x-rate-limit-remaining").unwrap(),
                "3"
            );
            assert_eq!(
                headers.rate_limits.get("x-rate-limit-reset").unwrap(),
                "1700000000"
            );
            let mut decoded = Vec::new();
            std::io::Read::read_to_end(&mut archive.read(&receipt).unwrap(), &mut decoded).unwrap();
            assert_eq!(decoded, b"first-second-third");
            let _ = std::fs::remove_dir_all(root);
        });
    }

    #[test]
    fn loopback_http_interrupted_body_fails_and_does_not_publish_receipt() {
        test_runtime().block_on(async {
            let body = StreamBody::new(futures_util::stream::iter([
                Ok::<_, io::Error>(Frame::data(Bytes::from_static(b"partial"))),
                Err(io::Error::other("simulated body transport failure")),
            ]));
            let (url, server) = start_http_server(Response::new(body)).await;
            let (root, archive) = temp_archive("interrupted");
            let result = super::fetch_and_archive_with_connector(
                &local_client(),
                archive,
                &url,
                "test-token",
                "test-resource",
                Default::default(),
                "test-time",
            )
            .await;
            assert!(result.is_err(), "truncated HTTP body must fail archive");
            let _ = server.await;
            let receipts = root.join("receipts");
            assert!(!receipts.exists() || std::fs::read_dir(receipts).unwrap().next().is_none());
            let temp = root.join("tmp");
            assert!(!temp.exists() || std::fs::read_dir(temp).unwrap().next().is_none());
            let _ = std::fs::remove_dir_all(root);
        });
    }

    #[test]
    fn cancelling_fetch_drops_stream_and_cleans_unpublished_archive() {
        test_runtime().block_on(async {
            let stream = futures_util::stream::iter([Ok::<_, io::Error>(Frame::data(
                Bytes::from_static(b"partial"),
            ))])
            .chain(futures_util::stream::once(async {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                Ok(Frame::data(Bytes::from_static(b"late")))
            }));
            let (url, server) = start_http_server(Response::new(StreamBody::new(stream))).await;
            let (root, archive) = temp_archive("cancelled");
            let client = local_client();
            let request = tokio::spawn(async move {
                super::fetch_and_archive_with_connector(
                    &client,
                    archive,
                    &url,
                    "test-token",
                    "test-resource",
                    Default::default(),
                    "test-time",
                )
                .await
            });
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            request.abort();
            let _ = request.await;
            let _ = server.await;

            // The archive writer is a blocking task. Cancellation closes its
            // channel, after which it removes the unpublished .part file; give
            // that independent cleanup task time to observe EOF.
            let temp = root.join("tmp");
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                loop {
                    let empty = !temp.exists()
                        || std::fs::read_dir(&temp)
                            .map(|mut entries| entries.next().is_none())
                            .unwrap_or(false);
                    if empty {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("archive writer removes its partial file after cancellation");

            let receipts = root.join("receipts");
            assert!(!receipts.exists() || std::fs::read_dir(receipts).unwrap().next().is_none());
            assert!(!temp.exists() || std::fs::read_dir(temp).unwrap().next().is_none());
            let _ = std::fs::remove_dir_all(root);
        });
    }

    #[test]
    fn bounded_async_body_pipe_archives_large_stream_without_collecting_it() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let root = std::env::temp_dir().join(format!("rust-polar-pipe-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&root);
            let archive = crate::archive::ArchiveStore::new(root.clone());
            let (sender, receiver) = tokio::sync::mpsc::channel(2);
            let writer_archive = archive.clone();
            let writer = tokio::task::spawn_blocking(move || {
                writer_archive.store_body(
                    "polar",
                    "test",
                    Default::default(),
                    200,
                    "t",
                    super::BodyChannelReader {
                        receiver,
                        current: bytes::Bytes::new(),
                    },
                )
            });
            let body = vec![0x5a; 300_000];
            for chunk in body.chunks(64 * 1024) {
                sender
                    .send(Ok(bytes::Bytes::copy_from_slice(chunk)))
                    .await
                    .unwrap();
            }
            sender.send(Ok(bytes::Bytes::new())).await.unwrap();
            drop(sender);
            let receipt = writer.await.unwrap().unwrap();
            let mut decoded = Vec::new();
            std::io::Read::read_to_end(&mut archive.read(&receipt).unwrap(), &mut decoded).unwrap();
            assert_eq!(decoded, body);
            let _ = std::fs::remove_dir_all(root);
        });
    }

    #[test]
    fn cancelled_body_pipe_is_not_mistaken_for_a_complete_response() {
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        drop(sender);
        let mut body = super::BodyChannelReader {
            receiver,
            current: bytes::Bytes::new(),
        };
        let mut buffer = [0u8; 16];
        assert_eq!(
            std::io::Read::read(&mut body, &mut buffer)
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::UnexpectedEof
        );
    }
}
