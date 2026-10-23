//! Polar OAuth authorization and AccessLink user registration.
//!
//! OAuth credentials and tokens are intentionally non-serializable and redact
//! themselves from `Debug`. An authorization-code exchange consumes its
//! attempt before the request is sent. Any transport failure after that point
//! is ambiguous and must be resolved by starting a new authorization flow;
//! callers must never replay the code automatically.

use std::fmt;
use std::io::Read;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::header::{AUTHORIZATION, CONTENT_TYPE};
use hyper::{Method, Request, Response, StatusCode};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::{client::legacy::Client, rt::TokioExecutor};
use serde::Deserialize;

const AUTHORIZATION_ENDPOINT: &str = "https://flow.polar.com/oauth2/authorization";
const TOKEN_ENDPOINT: &str = "https://polarremote.com/v2/oauth2/token";
const REGISTRATION_ENDPOINT: &str = "https://www.polaraccesslink.com/v3/users";
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;

pub type OAuthHttpClient = Client<HttpsConnector<HttpConnector>, Full<Bytes>>;

/// Client ID and secret from the Polar AccessLink application registration.
/// Its debug representation never reveals either value.
pub struct ClientCredentials {
    client_id: String,
    client_secret: String,
}

impl ClientCredentials {
    pub fn new(client_id: impl Into<String>, client_secret: impl Into<String>) -> Self {
        Self {
            client_id: client_id.into(),
            client_secret: client_secret.into(),
        }
    }

    pub fn client_id(&self) -> &str {
        &self.client_id
    }
}

impl fmt::Debug for ClientCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientCredentials")
            .field("client_id", &"[redacted]")
            .field("client_secret", &"[redacted]")
            .finish()
    }
}

/// One pending authorization attempt. Constructing it reads state from the
/// operating system CSPRNG; callback validation is one-use.
pub struct OAuthAttempt {
    state: String,
    redirect_uri: String,
    phase: AttemptPhase,
}

enum AttemptPhase {
    AwaitingCallback,
    CodeReady(AuthorizationCode),
    Consumed,
}

struct AuthorizationCode(String);

impl OAuthAttempt {
    pub fn new(redirect_uri: impl Into<String>) -> Result<Self, OAuthError> {
        let redirect_uri = redirect_uri.into();
        validate_redirect_uri(&redirect_uri)?;
        Ok(Self {
            state: random_state()?,
            redirect_uri,
            phase: AttemptPhase::AwaitingCallback,
        })
    }

    /// Build the provider authorization link. The caller should open this URL
    /// in a browser and deliver the resulting redirect to `accept_callback`.
    pub fn authorization_url(&self, client_id: &str) -> Result<String, OAuthError> {
        Ok(format!(
            "{AUTHORIZATION_ENDPOINT}?response_type=code&client_id={}&redirect_uri={}&state={}",
            form_encode(client_id),
            form_encode(&self.redirect_uri),
            form_encode(&self.state),
        ))
    }

    /// Validate exact callback origin/path, returned state and callback shape.
    /// On success the code is held privately for a single exchange attempt.
    pub fn accept_callback(&mut self, callback_url: &str) -> Result<(), OAuthError> {
        if !matches!(self.phase, AttemptPhase::AwaitingCallback) {
            return Err(OAuthError::AttemptAlreadyUsed);
        }
        if callback_url.contains('#') {
            return Err(OAuthError::RedirectMismatch);
        }
        let callback =
            hyper::Uri::try_from(callback_url).map_err(|_| OAuthError::InvalidCallback)?;
        let expected = hyper::Uri::try_from(self.redirect_uri.as_str())
            .map_err(|_| OAuthError::InvalidRedirectUri)?;
        if callback.scheme_str() != expected.scheme_str()
            || callback.authority() != expected.authority()
            || callback.path() != expected.path()
        {
            return Err(OAuthError::RedirectMismatch);
        }
        let params = parse_query(callback.query().unwrap_or_default())?;
        validate_redirect_query(expected.query(), &params)?;
        let returned_state = params.get("state").ok_or(OAuthError::MissingState)?;
        if !constant_time_eq(returned_state.as_bytes(), self.state.as_bytes()) {
            self.phase = AttemptPhase::Consumed;
            return Err(OAuthError::StateMismatch);
        }
        if let Some(error) = params.get("error") {
            self.phase = AttemptPhase::Consumed;
            return Err(OAuthError::ProviderDenied(error.clone()));
        }
        let code = params
            .get("code")
            .filter(|value| !value.is_empty())
            .ok_or(OAuthError::MissingCode)?
            .clone();
        self.phase = AttemptPhase::CodeReady(AuthorizationCode(code));
        Ok(())
    }

    /// Exchange the validated code exactly once. This method consumes the
    /// attempt before network I/O; a transport error is classified as
    /// ambiguous because the provider may already have redeemed the code.
    pub async fn exchange(
        mut self,
        client: &OAuthHttpClient,
        credentials: &ClientCredentials,
    ) -> Result<TokenResponse, OAuthError> {
        let code = match std::mem::replace(&mut self.phase, AttemptPhase::Consumed) {
            AttemptPhase::CodeReady(code) => code,
            _ => return Err(OAuthError::CodeNotValidated),
        };
        let request = token_request(credentials, &code.0, &self.redirect_uri)?;
        drop(code);
        let response = client
            .request(request)
            .await
            .map_err(|_| OAuthError::AmbiguousExchange)?;
        parse_token_response(response).await
    }
}

impl fmt::Debug for OAuthAttempt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OAuthAttempt")
            .field("state", &"[redacted]")
            .field("redirect_uri", &self.redirect_uri)
            .field(
                "phase",
                &match self.phase {
                    AttemptPhase::AwaitingCallback => "awaiting_callback",
                    AttemptPhase::CodeReady(_) => "code_ready",
                    AttemptPhase::Consumed => "consumed",
                },
            )
            .finish()
    }
}

/// Access token returned by Polar. Refresh tokens are optional because the
/// authorization response may omit them; callers must not assume they exist.
pub struct TokenResponse {
    pub access_token: String,
    pub token_type: Option<String>,
    pub expires_in: Option<u64>,
    pub refresh_token: Option<String>,
    pub scope: Option<String>,
}

impl fmt::Debug for TokenResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenResponse")
            .field("access_token", &"[redacted]")
            .field("token_type", &self.token_type)
            .field("expires_in", &self.expires_in)
            .field(
                "refresh_token",
                &self.refresh_token.as_ref().map(|_| "[redacted]"),
            )
            .field("scope", &self.scope)
            .finish()
    }
}

/// Build a TLS client that accepts the POST bodies required by OAuth and user
/// registration. Certificate validation uses the same bundled WebPKI roots as
/// the AccessLink GET client.
pub fn oauth_http_client() -> Result<OAuthHttpClient, rustls::Error> {
    let provider = rustls_rustcrypto::provider();
    let connector = HttpsConnectorBuilder::new()
        .with_provider_and_webpki_roots(provider)?
        .https_only()
        .enable_http1()
        .build();
    Ok(Client::builder(TokioExecutor::new()).build(connector))
}

/// Perform one read-only profile request to validate an existing AccessLink
/// Bearer token before saving it. The response body is deliberately dropped:
/// this check must not archive or print personal profile data.
pub async fn validate_access_token(
    client: &OAuthHttpClient,
    access_token: &str,
    user_id: &str,
) -> Result<(), OAuthError> {
    let profile = super::resources::user_profile(user_id);
    let request = Request::get(profile.url)
        .header(AUTHORIZATION, format!("Bearer {access_token}"))
        .body(Full::new(Bytes::new()))
        .map_err(|_| OAuthError::InvalidRequest)?;
    let response =
        tokio::time::timeout(std::time::Duration::from_secs(15), client.request(request))
            .await
            .map_err(|_| OAuthError::TokenCheckTimeout)?
            .map_err(|_| OAuthError::TokenCheckTransport)?;
    let status = response.status();
    if !status.is_success() {
        return Err(OAuthError::TokenRejected(status.as_u16()));
    }
    Ok(())
}

/// Register an AccessLink user after token exchange. This call is also
/// one-shot: transport failure is ambiguous, and the caller must inspect the
/// provider account state before attempting registration again.
pub async fn register_user(
    client: &OAuthHttpClient,
    access_token: &str,
    member_id: &str,
) -> Result<RegisteredUser, OAuthError> {
    let body = serde_json::to_vec(&serde_json::json!({ "member-id": member_id }))
        .map_err(|_| OAuthError::InvalidResponse)?;
    let request = Request::builder()
        .method(Method::POST)
        .uri(REGISTRATION_ENDPOINT)
        .header(AUTHORIZATION, format!("Bearer {access_token}"))
        .header(CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(body)))
        .map_err(|_| OAuthError::InvalidRequest)?;
    let response = client
        .request(request)
        .await
        .map_err(|_| OAuthError::AmbiguousRegistration)?;
    parse_registration_response(response).await
}

/// AccessLink's registered provider user identifier, kept as an open JSON
/// scalar so future response type changes are preserved.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegisteredUser {
    pub user_id: serde_json::Value,
}

#[derive(Deserialize)]
struct TokenWireResponse {
    access_token: String,
    #[serde(default)]
    token_type: Option<String>,
    #[serde(default)]
    expires_in: Option<u64>,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    scope: Option<String>,
}

#[derive(Deserialize)]
struct RegistrationWireResponse {
    #[serde(rename = "user-id")]
    user_id: serde_json::Value,
}

async fn parse_token_response(response: Response<Incoming>) -> Result<TokenResponse, OAuthError> {
    let status = response.status();
    let body = bounded_body(response.into_body()).await?;
    if !status.is_success() {
        return Err(response_error(status, &body));
    }
    let wire: TokenWireResponse =
        serde_json::from_slice(&body).map_err(|_| OAuthError::InvalidResponse)?;
    if wire.access_token.is_empty() {
        return Err(OAuthError::InvalidResponse);
    }
    Ok(TokenResponse {
        access_token: wire.access_token,
        token_type: wire.token_type,
        expires_in: wire.expires_in,
        refresh_token: wire.refresh_token,
        scope: wire.scope,
    })
}

async fn parse_registration_response(
    response: Response<Incoming>,
) -> Result<RegisteredUser, OAuthError> {
    let status = response.status();
    let body = bounded_body(response.into_body()).await?;
    if !status.is_success() {
        return Err(response_error(status, &body));
    }
    let wire: RegistrationWireResponse =
        serde_json::from_slice(&body).map_err(|_| OAuthError::InvalidResponse)?;
    if !wire.user_id.is_string() && !wire.user_id.is_number() {
        return Err(OAuthError::InvalidResponse);
    }
    Ok(RegisteredUser {
        user_id: wire.user_id,
    })
}

async fn bounded_body(mut body: Incoming) -> Result<Vec<u8>, OAuthError> {
    let mut bytes = Vec::with_capacity(1024);
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| OAuthError::AmbiguousResponse)?;
        if let Ok(data) = frame.into_data() {
            if bytes.len().saturating_add(data.len()) > MAX_RESPONSE_BYTES {
                return Err(OAuthError::ResponseTooLarge);
            }
            bytes.extend_from_slice(&data);
        }
    }
    Ok(bytes)
}

fn token_request(
    credentials: &ClientCredentials,
    code: &str,
    redirect_uri: &str,
) -> Result<Request<Full<Bytes>>, OAuthError> {
    let body = format!(
        "grant_type=authorization_code&code={}&redirect_uri={}",
        form_encode(code),
        form_encode(redirect_uri),
    );
    // Polar's token endpoint uses HTTP Basic client authentication. Base64 is
    // implemented locally to avoid adding a dependency for this small value.
    let basic = base64_encode(
        format!("{}:{}", credentials.client_id, credentials.client_secret).as_bytes(),
    );
    Request::builder()
        .method(Method::POST)
        .uri(TOKEN_ENDPOINT)
        .header(AUTHORIZATION, format!("Basic {basic}"))
        .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
        .body(Full::new(Bytes::from(body)))
        .map_err(|_| OAuthError::InvalidRequest)
}

fn validate_redirect_uri(uri: &str) -> Result<(), OAuthError> {
    if uri.contains('#') {
        return Err(OAuthError::InvalidRedirectUri);
    }
    let uri = hyper::Uri::try_from(uri).map_err(|_| OAuthError::InvalidRedirectUri)?;
    let Some(authority) = uri.authority() else {
        return Err(OAuthError::InvalidRedirectUri);
    };
    let valid_scheme = match uri.scheme_str() {
        Some("https") => true,
        Some("http") => matches!(authority.host(), "localhost" | "127.0.0.1" | "[::1]"),
        _ => false,
    };
    if !valid_scheme || uri.path().is_empty() {
        return Err(OAuthError::InvalidRedirectUri);
    }
    Ok(())
}

fn validate_redirect_query(
    expected: Option<&str>,
    received: &std::collections::BTreeMap<String, String>,
) -> Result<(), OAuthError> {
    let Some(expected) = expected else {
        return Ok(());
    };
    for (key, value) in parse_query(expected)? {
        if received.get(&key) != Some(&value) {
            return Err(OAuthError::RedirectMismatch);
        }
    }
    Ok(())
}

fn parse_query(query: &str) -> Result<std::collections::BTreeMap<String, String>, OAuthError> {
    let mut values = std::collections::BTreeMap::new();
    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = form_decode(key).ok_or(OAuthError::InvalidCallback)?;
        let value = form_decode(value).ok_or(OAuthError::InvalidCallback)?;
        if values.insert(key, value).is_some() {
            return Err(OAuthError::InvalidCallback);
        }
    }
    Ok(values)
}

fn form_encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(byte));
        } else if byte == b' ' {
            encoded.push('+');
        } else {
            use fmt::Write as _;
            let _ = write!(&mut encoded, "%{byte:02X}");
        }
    }
    encoded
}

fn form_decode(value: &str) -> Option<String> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut cursor = 0;
    while cursor < bytes.len() {
        match bytes[cursor] {
            b'+' => {
                decoded.push(b' ');
                cursor += 1;
            }
            b'%' if cursor + 2 < bytes.len() => {
                let high = (bytes[cursor + 1] as char).to_digit(16)? as u8;
                let low = (bytes[cursor + 2] as char).to_digit(16)? as u8;
                decoded.push((high << 4) | low);
                cursor += 3;
            }
            b'%' => return None,
            byte => {
                decoded.push(byte);
                cursor += 1;
            }
        }
    }
    String::from_utf8(decoded).ok()
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    let maximum = left.len().max(right.len());
    for index in 0..maximum {
        difference |= usize::from(
            left.get(index).copied().unwrap_or(0) ^ right.get(index).copied().unwrap_or(0),
        );
    }
    difference == 0
}

fn random_state() -> Result<String, OAuthError> {
    let mut random = [0u8; 32];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut source| source.read_exact(&mut random))
        .map_err(|_| OAuthError::RandomUnavailable)?;
    Ok(hex_encode(&random))
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[(byte >> 4) as usize]));
        output.push(char::from(HEX[(byte & 0x0f) as usize]));
    }
    output
}

fn base64_encode(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let first = chunk[0];
        let second = *chunk.get(1).unwrap_or(&0);
        let third = *chunk.get(2).unwrap_or(&0);
        output.push(char::from(TABLE[(first >> 2) as usize]));
        output.push(char::from(
            TABLE[(((first & 0x03) << 4) | (second >> 4)) as usize],
        ));
        output.push(if chunk.len() > 1 {
            char::from(TABLE[(((second & 0x0f) << 2) | (third >> 6)) as usize])
        } else {
            '='
        });
        output.push(if chunk.len() > 2 {
            char::from(TABLE[(third & 0x3f) as usize])
        } else {
            '='
        });
    }
    output
}

fn response_error(status: StatusCode, body: &[u8]) -> OAuthError {
    #[derive(Deserialize)]
    struct ProviderError {
        error: Option<String>,
    }
    let code = serde_json::from_slice::<ProviderError>(body)
        .ok()
        .and_then(|body| body.error)
        .unwrap_or_else(|| "provider_error".into());
    OAuthError::ProviderResponse {
        status: status.as_u16(),
        code,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OAuthError {
    InvalidRedirectUri,
    InvalidCallback,
    RedirectMismatch,
    MissingState,
    StateMismatch,
    MissingCode,
    ProviderDenied(String),
    AttemptAlreadyUsed,
    CodeNotValidated,
    InvalidRequest,
    InvalidResponse,
    ResponseTooLarge,
    RandomUnavailable,
    AmbiguousExchange,
    AmbiguousRegistration,
    AmbiguousResponse,
    ProviderResponse { status: u16, code: String },
    TokenRejected(u16),
    TokenCheckTimeout,
    TokenCheckTransport,
}

impl fmt::Display for OAuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRedirectUri => f.write_str("invalid OAuth redirect URI"),
            Self::InvalidCallback => f.write_str("invalid OAuth callback"),
            Self::RedirectMismatch => f.write_str("OAuth callback redirect does not match"),
            Self::MissingState => f.write_str("OAuth callback has no state"),
            Self::StateMismatch => f.write_str("OAuth callback state does not match"),
            Self::MissingCode => f.write_str("OAuth callback has no authorization code"),
            Self::ProviderDenied(code) => write!(f, "Polar authorization denied ({code})"),
            Self::AttemptAlreadyUsed => f.write_str("OAuth attempt has already been used"),
            Self::CodeNotValidated => f.write_str("OAuth callback has not been validated"),
            Self::InvalidRequest => f.write_str("could not build OAuth request"),
            Self::InvalidResponse => f.write_str("Polar returned an invalid OAuth response"),
            Self::ResponseTooLarge => f.write_str("Polar OAuth response exceeds size limit"),
            Self::RandomUnavailable => f.write_str("operating-system random source unavailable"),
            Self::AmbiguousExchange => f.write_str("OAuth code exchange outcome is ambiguous; do not retry the code"),
            Self::AmbiguousRegistration => f.write_str("AccessLink registration outcome is ambiguous; inspect provider state before retrying"),
            Self::AmbiguousResponse => f.write_str("Polar response body was interrupted"),
            Self::ProviderResponse { status, code } => {
                write!(f, "Polar rejected the OAuth request (HTTP {status}, {code})")
            }
            Self::TokenRejected(status) => {
                write!(f, "Polar rejected the AccessLink token (HTTP {status})")
            }
            Self::TokenCheckTimeout => f.write_str("timed out validating the AccessLink token"),
            Self::TokenCheckTransport => {
                f.write_str("could not contact Polar to validate the AccessLink token")
            }
        }
    }
}

impl std::error::Error for OAuthError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn attempt() -> OAuthAttempt {
        OAuthAttempt {
            state: "expected-state".into(),
            redirect_uri: "https://localhost/callback?tenant=polar".into(),
            phase: AttemptPhase::AwaitingCallback,
        }
    }

    #[test]
    fn builds_authorization_url_with_encoded_parameters() {
        let attempt = attempt();
        let url = attempt.authorization_url("client id").unwrap();
        assert!(url.contains("client_id=client+id"));
        assert!(url.contains("redirect_uri=https%3A%2F%2Flocalhost%2Fcallback%3Ftenant%3Dpolar"));
        assert!(url.ends_with("state=expected-state"));
    }

    #[test]
    fn accepts_callback_only_after_redirect_and_state_match() {
        let mut attempt = attempt();
        attempt
            .accept_callback(
                "https://localhost/callback?tenant=polar&code=abc%2F123&state=expected-state",
            )
            .unwrap();
        assert!(matches!(attempt.phase, AttemptPhase::CodeReady(_)));
        assert_eq!(
            attempt.accept_callback(
                "https://localhost/callback?tenant=polar&code=x&state=expected-state"
            ),
            Err(OAuthError::AttemptAlreadyUsed)
        );
    }

    #[test]
    fn rejects_mismatched_state_redirect_duplicates_and_fragments() {
        let mut wrong_state = attempt();
        assert_eq!(
            wrong_state
                .accept_callback("https://localhost/callback?tenant=polar&code=x&state=wrong"),
            Err(OAuthError::StateMismatch)
        );
        let mut wrong_redirect = attempt();
        assert_eq!(
            wrong_redirect.accept_callback(
                "https://evil.example/callback?tenant=polar&code=x&state=expected-state"
            ),
            Err(OAuthError::RedirectMismatch)
        );
        let mut duplicate = attempt();
        assert_eq!(
            duplicate.accept_callback(
                "https://localhost/callback?tenant=polar&code=x&code=y&state=expected-state"
            ),
            Err(OAuthError::InvalidCallback)
        );
        let mut fragment = attempt();
        assert_eq!(
            fragment.accept_callback(
                "https://localhost/callback?tenant=polar&code=x&state=expected-state#fragment"
            ),
            Err(OAuthError::RedirectMismatch)
        );
    }

    #[test]
    fn token_response_does_not_require_refresh_token() {
        let token: TokenWireResponse = serde_json::from_str(
            r#"{"access_token":"secret","token_type":"Bearer","expires_in":3600}"#,
        )
        .unwrap();
        assert_eq!(token.access_token, "secret");
        assert_eq!(token.refresh_token, None);
        assert_eq!(base64_encode(b"client:secret"), "Y2xpZW50OnNlY3JldA==");
    }

    #[test]
    fn registration_user_id_accepts_number_or_string() {
        let numeric: RegistrationWireResponse =
            serde_json::from_str(r#"{"user-id":12345}"#).unwrap();
        let textual: RegistrationWireResponse =
            serde_json::from_str(r#"{"user-id":"polar-user"}"#).unwrap();
        assert_eq!(numeric.user_id, serde_json::json!(12345));
        assert_eq!(textual.user_id, serde_json::json!("polar-user"));
    }

    #[test]
    fn callback_code_never_appears_in_attempt_debug() {
        let mut attempt = attempt();
        attempt
            .accept_callback(
                "https://localhost/callback?tenant=polar&code=very-secret&state=expected-state",
            )
            .unwrap();
        assert!(!format!("{attempt:?}").contains("very-secret"));
        assert!(!format!("{attempt:?}").contains("expected-state"));
    }
}
