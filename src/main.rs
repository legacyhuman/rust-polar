use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug, Parser)]
#[command(name = "rust-polar", version, about)]
struct Cli {
    /// Data directory (defaults to the platform-specific application data directory).
    #[arg(long, env = "RUST_POLAR_DATA_DIR")]
    data_dir: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Initialize the local archive.
    Init,
    /// Show local database and synchronization status.
    Status,
    /// Write a redb-consistent snapshot and its referenced archive files.
    Backup { destination: PathBuf },
    /// Restore a verified backup into a new data directory selected by --data-dir.
    Restore { source: PathBuf },
    /// Authorize this installation with Polar AccessLink.
    Auth {
        #[command(subcommand)]
        command: AuthCommand,
    },
    /// Run a one-shot synchronization cycle.
    Sync {
        #[command(subcommand)]
        command: SyncCommand,
    },
    /// Keep the database open and serve local read-only API clients.
    Serve {
        /// Unix socket path (defaults to DATA_DIR/run/rust-polar.sock).
        #[arg(long, env = "RUST_POLAR_SOCKET")]
        socket: Option<PathBuf>,
        /// Delay between scheduled Polar sync cycles.
        #[arg(
            long,
            env = "RUST_POLAR_SYNC_INTERVAL_SECONDS",
            default_value_t = 1800,
            value_parser = parse_sync_interval
        )]
        sync_interval_seconds: u64,
    },
    /// Print a bounded page of normalized local records as JSON Lines.
    Query {
        #[arg(long)]
        after: Option<String>,
        #[arg(long, default_value_t = 1000)]
        limit: usize,
    },
    /// Export one normalized metric over a half-open UTC time range.
    Export {
        #[arg(long)]
        metric: String,
        #[arg(long)]
        from: String,
        #[arg(long)]
        to: String,
        #[arg(long, default_value = "polar")]
        source: String,
        /// Defaults to the authorized Polar user id, or `local` when unavailable.
        #[arg(long)]
        user_id: Option<String>,
        /// Output format.
        #[arg(long, default_value = "jsonl", value_parser = ["jsonl", "csv"])]
        format: String,
        /// Rows fetched per page (maximum 1000).
        #[arg(long, default_value_t = 1000)]
        limit: usize,
    },
    /// Verify archived response files.
    Archive {
        #[command(subcommand)]
        command: ArchiveCommand,
    },
}

#[derive(Debug, Subcommand)]
enum AuthCommand {
    Polar {
        /// User-chosen AccessLink member identifier (not a Polar account ID).
        #[arg(long)]
        member_id: String,
    },
    /// Import an existing AccessLink token after a read-only profile check.
    Import {
        /// Polar-issued AccessLink user-id returned by registration.
        #[arg(long)]
        user_id: String,
    },
}

#[derive(Debug, Subcommand)]
enum SyncCommand {
    Polar {
        /// Inclusive start date (YYYY-MM-DD); must be supplied with --to.
        #[arg(long)]
        from: Option<String>,
        /// Inclusive end date (YYYY-MM-DD); must be supplied with --from.
        #[arg(long)]
        to: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
enum ArchiveCommand {
    Verify,
    Diagnose {
        /// Only report incomplete/orphan files at least this old.
        #[arg(long, default_value_t = 86_400)]
        older_than_seconds: u64,
    },
    Cleanup {
        /// Only consider incomplete/orphan files at least this old.
        #[arg(long, default_value_t = 86_400)]
        older_than_seconds: u64,
        /// Delete the candidates from the immediately preceding dry-run scan.
        #[arg(long)]
        apply: bool,
    },
    Replay {
        #[arg(long)]
        resource: Option<String>,
    },
}

struct LocalApiReply {
    status: u16,
    body: Vec<u8>,
}

fn parse_sync_interval(value: &str) -> Result<u64, String> {
    let seconds = value
        .parse::<u64>()
        .map_err(|error| format!("sync interval must be an integer number of seconds: {error}"))?;
    if !(30..=2_592_000).contains(&seconds) {
        return Err("sync interval must be between 30 seconds and 30 days".into());
    }
    Ok(seconds)
}

fn parse_utc_micros(value: &str) -> Result<i64, Box<dyn std::error::Error>> {
    let parsed =
        time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)?;
    i64::try_from(parsed.unix_timestamp_nanos() / 1_000)
        .map_err(|_| "timestamp is outside the supported UTC microsecond range".into())
}

fn parse_date(value: &str) -> Result<time::Date, Box<dyn std::error::Error>> {
    let format = time::format_description::parse_borrowed::<1>("[year]-[month]-[day]")?;
    time::Date::parse(value, &format).map_err(|_| "date must use YYYY-MM-DD".into())
}

struct ExportOptions<'a> {
    root: &'a std::path::Path,
    user_id: &'a str,
    source: &'a str,
    metric: &'a str,
    from: &'a str,
    to: &'a str,
    from_micros: i64,
    to_micros: i64,
    limit: usize,
    format: &'a str,
}

fn export_measurements(options: ExportOptions<'_>) -> Result<(), Box<dyn std::error::Error>> {
    use std::io::Write as _;

    let socket = socket_path(options.root);
    let mut stdout = std::io::BufWriter::new(std::io::stdout().lock());
    let mut cursor: Option<String> = None;
    let mut header_written = false;
    loop {
        let reply = unix_api_request(
            &socket,
            "GET",
            &series_page_path(
                options.user_id,
                options.source,
                options.metric,
                options.from,
                options.to,
                options.limit,
                cursor.as_deref(),
            ),
        )?;
        let rows = if let Some(reply) = reply {
            // The daemon owns redb. Request bounded JSON pages and close each
            // socket response before asking for the next one.
            if !(200..300).contains(&reply.status) {
                return Err(format!(
                    "series query failed (HTTP {}): {}",
                    reply.status,
                    String::from_utf8_lossy(&reply.body)
                )
                .into());
            }
            let page: serde_json::Value = serde_json::from_slice(&reply.body)?;
            cursor = page
                .get("next_cursor")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
            page.get("rows")
                .and_then(serde_json::Value::as_array)
                .cloned()
                .ok_or("series response is missing rows")?
        } else {
            let store = rust_polar::storage::Store::open(options.root)?;
            let page = store.measurement_page(rust_polar::storage::MeasurementQuery {
                user_id: options.user_id,
                source: options.source,
                metric: options.metric,
                context: None,
                from_utc_micros: options.from_micros,
                to_utc_micros: options.to_micros,
                limit: options.limit,
                after: cursor.as_deref(),
            })?;
            cursor = page.next_cursor;
            page.rows
                .iter()
                .map(serde_json::to_value)
                .collect::<Result<Vec<_>, _>>()?
        };

        if options.format == "csv" && !header_written {
            writeln!(
                stdout,
                "id,user_id,source,metric,value,unit,start_utc_micros,end_utc_micros,aggregation,context,quality"
            )?;
            header_written = true;
        }
        for row in &rows {
            if options.format == "jsonl" {
                serde_json::to_writer(&mut stdout, row)?;
                stdout.write_all(b"\n")?;
            } else {
                write_measurement_csv_row(&mut stdout, row)?;
            }
        }
        if cursor.is_none() {
            break;
        }
    }
    stdout.flush()?;
    Ok(())
}

fn series_page_path(
    user_id: &str,
    source: &str,
    metric: &str,
    from: &str,
    to: &str,
    limit: usize,
    cursor: Option<&str>,
) -> String {
    let mut path = format!(
        "/v1/series?user_id={}&source={}&metric={}&from={}&to={}&limit={limit}",
        percent_encode(user_id.as_bytes()),
        percent_encode(source.as_bytes()),
        percent_encode(metric.as_bytes()),
        percent_encode(from.as_bytes()),
        percent_encode(to.as_bytes()),
    );
    if let Some(cursor) = cursor {
        path.push_str("&cursor=");
        path.push_str(&percent_encode(cursor.as_bytes()));
    }
    path
}

fn write_measurement_csv_row(
    writer: &mut impl std::io::Write,
    row: &serde_json::Value,
) -> Result<(), std::io::Error> {
    let start = row
        .pointer("/time/start/Utc/unix_micros")
        .and_then(serde_json::Value::as_i64)
        .map(|value| value.to_string())
        .unwrap_or_default();
    let end = row
        .pointer("/time/end/Utc/unix_micros")
        .and_then(serde_json::Value::as_i64)
        .map(|value| value.to_string())
        .unwrap_or_default();
    let fields = [
        row.pointer("/id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        row.pointer("/provenance/user_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        row.pointer("/provenance/source")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        row.pointer("/metric")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        row.pointer("/value")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| {
                row.get("value")
                    .map(ToString::to_string)
                    .unwrap_or_default()
            }),
        row.get("unit").map(ToString::to_string).unwrap_or_default(),
        start,
        end,
        row.get("aggregation")
            .map(ToString::to_string)
            .unwrap_or_default(),
        row.get("context")
            .map(ToString::to_string)
            .unwrap_or_default(),
        row.pointer("/provenance/quality")
            .map(ToString::to_string)
            .unwrap_or_default(),
    ];
    for (index, field) in fields.iter().enumerate() {
        if index != 0 {
            writer.write_all(b",")?;
        }
        csv_field(writer, field)?;
    }
    writer.write_all(b"\n")
}

fn csv_field(writer: &mut impl std::io::Write, field: &str) -> Result<(), std::io::Error> {
    if field
        .chars()
        .any(|ch| matches!(ch, ',' | '"' | '\n' | '\r'))
    {
        writer.write_all(b"\"")?;
        for byte in field.bytes() {
            if byte == b'"' {
                writer.write_all(b"\"\"")?;
            } else {
                writer.write_all(&[byte])?;
            }
        }
        writer.write_all(b"\"")
    } else {
        writer.write_all(field.as_bytes())
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let root = cli.data_dir.unwrap_or_else(default_data_dir);
    match cli.command {
        Command::Init => {
            let _ = rust_polar::storage::Store::open(&root)?;
            println!("Initialized local archive at {}", root.display());
        }
        Command::Status => {
            let socket = socket_path(&root);
            if let Some(reply) = unix_api_request(&socket, "GET", "/v1/status")? {
                if !(200..300).contains(&reply.status) {
                    return Err(format!(
                        "status request failed (HTTP {}): {}",
                        reply.status,
                        String::from_utf8_lossy(&reply.body)
                    )
                    .into());
                }
                println!("{}", String::from_utf8_lossy(&reply.body));
            } else {
                let store = rust_polar::storage::Store::open(&root)?;
                let archive =
                    rust_polar::archive::ArchiveStore::new(store.archive_path().to_path_buf());
                let sidecar_count = archive.receipts()?.try_fold(0_u64, |count, receipt| {
                    receipt?;
                    Ok::<_, rust_polar::archive::ArchiveError>(count + 1)
                })?;
                println!("database: {}", store.database_path().display());
                println!("archive: {}", store.archive_path().display());
                println!("archive receipt sidecars: {sidecar_count}");
                println!(
                    "receipts registered in database: {}",
                    store.archive_count()?
                );
                println!("provider records: {}", store.record_count()?);
                println!("normalized measurements: {}", store.measurement_count()?);
                let credentials = rust_polar::polar::credentials::PolarCredentials::load(&root)?;
                println!(
                    "polar: {}",
                    if credentials.is_some() {
                        "authorized"
                    } else {
                        "not authorized"
                    }
                );
            }
        }
        Command::Backup { destination } => {
            let destination = if destination.is_absolute() {
                destination
            } else {
                std::env::current_dir()?.join(destination)
            };
            let socket = socket_path(&root);
            let path = format!(
                "/v1/admin/backup?destination={}",
                percent_encode(destination.to_string_lossy().as_bytes())
            );
            if let Some(reply) = unix_api_request(&socket, "POST", &path)? {
                if !(200..300).contains(&reply.status) {
                    return Err(format!(
                        "backup request failed (HTTP {}): {}",
                        reply.status,
                        String::from_utf8_lossy(&reply.body)
                    )
                    .into());
                }
                println!("{}", String::from_utf8_lossy(&reply.body));
            } else {
                let store = rust_polar::storage::Store::open(&root)?;
                store.backup_to(&destination)?;
                println!("backup created at {}", destination.display());
                println!("credentials were not copied; authorize Polar again after restore");
            }
        }
        Command::Restore { source } => {
            rust_polar::storage::Store::restore_backup(&source, &root)?;
            println!("backup restored to {}", root.display());
            println!("credentials were not restored; authorize Polar again");
        }
        Command::Auth { command } => match command {
            AuthCommand::Polar { member_id } => {
                let client_id = std::env::var("POLAR_CLIENT_ID")
                    .map_err(|_| "set POLAR_CLIENT_ID in the environment before authorizing")?;
                let client_secret = std::env::var("POLAR_CLIENT_SECRET")
                    .map_err(|_| "set POLAR_CLIENT_SECRET in the environment before authorizing")?;
                authorize_polar(&root, &member_id, client_id, client_secret)?;
            }
            AuthCommand::Import { user_id } => import_polar_token(&root, &user_id)?,
        },
        Command::Sync { command } => match command {
            SyncCommand::Polar { from, to } => {
                let range = match (from, to) {
                    (None, None) => None,
                    (Some(from), Some(to)) => Some((parse_date(&from)?, parse_date(&to)?)),
                    _ => return Err("--from and --to must be supplied together".into()),
                };
                if range.is_some_and(|(from, to)| to < from) {
                    return Err("--to must be on or after --from".into());
                }
                let mut path = "/v1/admin/sync".to_owned();
                if let Some((from, to)) = range {
                    path.push_str(&format!(
                        "?from={}&to={}",
                        percent_encode(
                            from.format(&time::format_description::well_known::Iso8601::DATE)?
                                .as_bytes()
                        ),
                        percent_encode(
                            to.format(&time::format_description::well_known::Iso8601::DATE)?
                                .as_bytes()
                        )
                    ));
                }
                if let Some(reply) = unix_api_request(&socket_path(&root), "POST", &path)? {
                    if !(200..300).contains(&reply.status) {
                        return Err(format!(
                            "sync request failed (HTTP {}): {}",
                            reply.status,
                            String::from_utf8_lossy(&reply.body)
                        )
                        .into());
                    }
                    println!("{}", String::from_utf8_lossy(&reply.body));
                    return Ok(());
                }
                let store = Arc::new(rust_polar::storage::Store::open(&root)?);
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .max_blocking_threads(2)
                    .enable_all()
                    .build()?;
                let status = runtime.block_on(async {
                    match range {
                        Some((from, to)) => {
                            rust_polar::polar::sync::run_cycle_range(store, &root, from, to).await
                        }
                        None => rust_polar::polar::sync::run_cycle(store, &root).await,
                    }
                })?;
                println!("{}", serde_json::to_string_pretty(&status)?);
            }
        },
        Command::Serve {
            socket,
            sync_interval_seconds,
        } => {
            if sync_interval_seconds == 0 {
                return Err("--sync-interval-seconds must be greater than zero".into());
            }
            let store = Arc::new(rust_polar::storage::Store::open(&root)?);
            let recovered = reconcile_archive_sidecars(&store)?;
            eprintln!("reconciled {recovered} archive receipt sidecars");
            let sync_interval = std::time::Duration::from_secs(sync_interval_seconds);
            rust_polar::scheduler::initialize(&store, sync_interval)?;
            let socket = socket.unwrap_or_else(|| socket_path(&root));
            let runtime = tokio::runtime::Builder::new_current_thread()
                .max_blocking_threads(2)
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
                let sync_lock = Arc::new(tokio::sync::Mutex::new(()));
                let mut server = tokio::spawn(rust_polar::api::serve_unix(
                    store.clone(),
                    socket.clone(),
                    shutdown_rx.clone(),
                    sync_lock.clone(),
                ));
                let mut scheduler = tokio::spawn(rust_polar::scheduler::run(
                    store,
                    sync_interval,
                    shutdown_rx,
                    sync_lock,
                ));
                tokio::select! {
                    result = &mut server => {
                        let _ = shutdown_tx.send(true);
                        scheduler.await??;
                        result??;
                    },
                    result = &mut scheduler => {
                        let _ = shutdown_tx.send(true);
                        server.await??;
                        result??;
                    },
                    _ = wait_for_shutdown_signal() => {
                        let _ = shutdown_tx.send(true);
                        server.await??;
                        scheduler.await??;
                    }
                }
                Ok::<_, Box<dyn std::error::Error>>(())
            })?;
        }
        Command::Query { after, limit } => {
            if !(1..=1000).contains(&limit) {
                return Err("--limit must be between 1 and 1000".into());
            }
            let socket = socket_path(&root);
            let mut path = format!("/v1/records?limit={limit}");
            if let Some(after) = after.as_deref() {
                path.push_str("&cursor=");
                path.push_str(&percent_encode(after.as_bytes()));
            }
            if let Some(reply) = unix_api_request(&socket, "GET", &path)? {
                if !(200..300).contains(&reply.status) {
                    return Err(format!(
                        "query failed (HTTP {}): {}",
                        reply.status,
                        String::from_utf8_lossy(&reply.body)
                    )
                    .into());
                }
                let page: serde_json::Value = serde_json::from_slice(&reply.body)?;
                if let Some(rows) = page.get("rows").and_then(serde_json::Value::as_array) {
                    for row in rows {
                        println!(
                            "{}",
                            serde_json::to_string(&serde_json::json!({
                                "key": row.get("id"),
                                "record": row.get("record")
                            }))?
                        );
                    }
                }
            } else {
                let store = rust_polar::storage::Store::open(&root)?;
                for (key, payload) in store.record_page(after.as_deref(), limit)? {
                    let key = serde_json::to_string(&key)?;
                    let payload: serde_json::Value = serde_json::from_slice(&payload)?;
                    println!("{{\"key\":{key},\"record\":{payload}}}");
                }
            }
        }
        Command::Export {
            metric,
            from,
            to,
            source,
            user_id,
            format,
            limit,
        } => {
            if metric.is_empty() || metric.len() > 256 || source.is_empty() || source.len() > 256 {
                return Err("metric and source must be 1 to 256 bytes".into());
            }
            if !(1..=1000).contains(&limit) {
                return Err("--limit must be between 1 and 1000".into());
            }
            let from_micros = parse_utc_micros(&from)?;
            let to_micros = parse_utc_micros(&to)?;
            if from_micros >= to_micros {
                return Err(
                    "--from must be earlier than --to; the range is half-open [from,to)".into(),
                );
            }
            let user_id = match user_id {
                Some(value) => value,
                None => rust_polar::polar::credentials::PolarCredentials::load(&root)?
                    .map(|credentials| credentials.user_id)
                    .unwrap_or_else(|| "polar-local".to_owned()),
            };
            if user_id.is_empty() || user_id.len() > 256 {
                return Err("user_id must be 1 to 256 bytes".into());
            }
            export_measurements(ExportOptions {
                root: &root,
                user_id: &user_id,
                source: &source,
                metric: &metric,
                from: &from,
                to: &to,
                from_micros,
                to_micros,
                limit,
                format: &format,
            })?;
        }
        Command::Archive { command } => match command {
            ArchiveCommand::Verify => {
                let socket = socket_path(&root);
                if let Some(reply) = unix_api_request(&socket, "POST", "/v1/admin/archive/verify")?
                {
                    if !(200..300).contains(&reply.status) {
                        return Err(format!(
                            "archive verify failed (HTTP {}): {}",
                            reply.status,
                            String::from_utf8_lossy(&reply.body)
                        )
                        .into());
                    }
                    let value: serde_json::Value = serde_json::from_slice(&reply.body)?;
                    println!("verified: {}", value["verified"]);
                    println!("failed: {}", value["failed"]);
                } else {
                    let store = rust_polar::storage::Store::open(&root)?;
                    let summary = rust_polar::archive::verify_all(store.archive_path())?;
                    println!("verified: {}", summary.verified);
                    println!("failed: {}", summary.failed);
                }
            }
            ArchiveCommand::Diagnose { older_than_seconds } => {
                let socket = socket_path(&root);
                let path =
                    format!("/v1/admin/archive/diagnose?older_than_seconds={older_than_seconds}");
                if let Some(reply) = unix_api_request(&socket, "POST", &path)? {
                    if !(200..300).contains(&reply.status) {
                        return Err(format!(
                            "archive diagnose failed (HTTP {}): {}",
                            reply.status,
                            String::from_utf8_lossy(&reply.body)
                        )
                        .into());
                    }
                    println!("{}", String::from_utf8_lossy(&reply.body));
                } else {
                    let store = rust_polar::storage::Store::open(&root)?;
                    let archive =
                        rust_polar::archive::ArchiveStore::new(store.archive_path().to_path_buf());
                    let summary = archive.diagnose(
                        std::time::Duration::from_secs(older_than_seconds),
                        |issue| {
                            println!(
                                "{:?}\t{}\t{} bytes",
                                issue.kind,
                                issue.path.display(),
                                issue.bytes
                            );
                        },
                    )?;
                    println!(
                        "stale parts: {} ({} bytes)",
                        summary.stale_parts, summary.stale_part_bytes
                    );
                    println!(
                        "orphan objects: {} ({} bytes)",
                        summary.orphan_objects, summary.orphan_object_bytes
                    );
                }
            }
            ArchiveCommand::Cleanup {
                older_than_seconds,
                apply,
            } => {
                let socket = socket_path(&root);
                let path = format!(
                    "/v1/admin/archive/cleanup?older_than_seconds={older_than_seconds}&apply={apply}"
                );
                if let Some(reply) = unix_api_request(&socket, "POST", &path)? {
                    if !(200..300).contains(&reply.status) {
                        return Err(format!(
                            "archive cleanup failed (HTTP {}): {}",
                            reply.status,
                            String::from_utf8_lossy(&reply.body)
                        )
                        .into());
                    }
                    println!("{}", String::from_utf8_lossy(&reply.body));
                } else {
                    let store = rust_polar::storage::Store::open(&root)?;
                    let archive =
                        rust_polar::archive::ArchiveStore::new(store.archive_path().to_path_buf());
                    let plan = archive
                        .preview_cleanup(std::time::Duration::from_secs(older_than_seconds))?;
                    let summary = plan.summary();
                    println!(
                        "dry run: {} stale parts ({} bytes), {} orphan objects ({} bytes)",
                        summary.stale_parts,
                        summary.stale_part_bytes,
                        summary.orphan_objects,
                        summary.orphan_object_bytes
                    );
                    if apply {
                        let removed = archive.apply_cleanup(&plan)?;
                        println!(
                            "removed: {} stale parts ({} bytes), {} orphan objects ({} bytes)",
                            removed.removed_parts,
                            removed.removed_part_bytes,
                            removed.removed_objects,
                            removed.removed_object_bytes
                        );
                    } else {
                        println!(
                            "no files deleted; rerun with --apply to delete this verified candidate set"
                        );
                    }
                }
            }
            ArchiveCommand::Replay { resource } => {
                let socket = socket_path(&root);
                let path = resource.as_ref().map_or_else(
                    || "/v1/admin/replay".to_owned(),
                    |resource| {
                        format!(
                            "/v1/admin/replay?resource={}",
                            percent_encode(resource.as_bytes())
                        )
                    },
                );
                if let Some(reply) = unix_api_request(&socket, "POST", &path)? {
                    if !(200..300).contains(&reply.status) {
                        return Err(format!(
                            "replay failed (HTTP {}): {}",
                            reply.status,
                            String::from_utf8_lossy(&reply.body)
                        )
                        .into());
                    }
                    println!("{}", String::from_utf8_lossy(&reply.body));
                } else {
                    let store = rust_polar::storage::Store::open(&root)?;
                    let credentials =
                        rust_polar::polar::credentials::PolarCredentials::load(&root)?;
                    let user_id = credentials
                        .as_ref()
                        .map_or("polar-local", |c| c.user_id.as_str());
                    let processed = rust_polar::ingest::replay_archive(
                        &store,
                        resource.as_deref(),
                        user_id,
                        None,
                    )
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                    println!("replayed supported receipts: {processed}");
                }
            }
        },
    }
    Ok(())
}

/// Repair the filesystem-to-redb publication gap after a crash. Sidecars are
/// streamed one at a time, so startup memory does not grow with archive size.
fn reconcile_archive_sidecars(
    store: &rust_polar::storage::Store,
) -> Result<u64, Box<dyn std::error::Error>> {
    let archive = rust_polar::archive::ArchiveStore::new(store.archive_path().to_path_buf());
    let mut reconciled = 0_u64;
    for receipt in archive.receipts()? {
        let receipt = receipt?;
        store.register_archive(&receipt.receipt_id, &receipt)?;
        reconciled = reconciled.saturating_add(1);
    }
    Ok(reconciled)
}

/// Run the interactive authorization flow without opening a browser or
/// placing client secrets in command-line arguments.
fn authorize_polar(
    root: &std::path::Path,
    member_id: &str,
    client_id: String,
    client_secret: String,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    const REDIRECT_URI: &str = "http://127.0.0.1:8765/callback";
    const CALLBACK_LIMIT: usize = 8192;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let listener = runtime.block_on(TcpListener::bind(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        8765,
    )))?;
    let mut attempt = rust_polar::polar::oauth::OAuthAttempt::new(REDIRECT_URI)?;
    let credentials =
        rust_polar::polar::oauth::ClientCredentials::new(client_id.clone(), client_secret.clone());
    let authorization_url = attempt.authorization_url(&client_id)?;
    println!("Open this URL and authorize the app:\n{authorization_url}");
    println!("Waiting up to 5 minutes for the Polar callback on 127.0.0.1:8765...");

    runtime.block_on(async {
        let (mut stream, peer) = tokio::time::timeout(
            std::time::Duration::from_secs(300),
            listener.accept(),
        )
        .await
        .map_err(|_| "timed out waiting for Polar OAuth callback")??;
        if !peer.ip().is_loopback() {
            return Err("OAuth callback connection did not come from loopback".into());
        }
        let mut request = Vec::with_capacity(1024);
        let mut buffer = [0u8; 1024];
        loop {
            let count = tokio::time::timeout(
                std::time::Duration::from_secs(15),
                stream.read(&mut buffer),
            )
            .await
            .map_err(|_| "timed out reading OAuth callback")??;
            if count == 0 {
                return Err("OAuth callback closed before sending an HTTP request".into());
            }
            if request.len().saturating_add(count) > CALLBACK_LIMIT {
                return Err("OAuth callback request exceeds 8 KiB".into());
            }
            request.extend_from_slice(&buffer[..count]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        let first_line = std::str::from_utf8(&request)
            .map_err(|_| "OAuth callback request is not valid UTF-8")?
            .split("\r\n")
            .next()
            .ok_or("OAuth callback request line is missing")?;
        let mut parts = first_line.split_ascii_whitespace();
        if parts.next() != Some("GET") {
            return Err("OAuth callback must use GET".into());
        }
        let target = parts.next().ok_or("OAuth callback target is missing")?;
        if !target.starts_with("/callback?") {
            return Err("OAuth callback path does not match".into());
        }
        let callback_url = format!("http://127.0.0.1:8765{target}");
        // Validate state and redirect before allowing the code to be exchanged.
        attempt.accept_callback(&callback_url)?;
        let response_body = b"Polar authorization received. You may close this tab.";
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
            response_body.len()
        );
        stream.write_all(response.as_bytes()).await?;
        stream.write_all(response_body).await?;
        let client = rust_polar::polar::oauth::oauth_http_client()?;
        let token = attempt.exchange(&client, &credentials).await?;
        let registered = rust_polar::polar::oauth::register_user(
            &client,
            &token.access_token,
            member_id,
        )
        .await?;
        let user_id = match registered.user_id {
            serde_json::Value::String(value) => value,
            value => value.to_string(),
        };
        let updated_at = time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)?;
        let stored = rust_polar::polar::credentials::PolarCredentials::new(
            rust_polar::polar::credentials::NewPolarCredentials {
                client_id: Some(client_id),
                client_secret: Some(client_secret),
                access_token: token.access_token,
                refresh_token: token.refresh_token,
                token_type: token.token_type,
                expires_in: token.expires_in,
                user_id,
                member_id: Some(member_id.to_owned()),
                updated_at,
            },
        );
        stored.save(root)?;
        println!("Polar authorization saved in the private credentials directory.");
        Ok::<(), Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}

fn import_polar_token(
    root: &std::path::Path,
    user_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if user_id.is_empty() || user_id.trim() != user_id {
        return Err("provide the exact Polar-issued AccessLink user-id".into());
    }
    let access_token = read_hidden("Polar AccessLink token: ")?;
    if access_token.is_empty() || access_token.bytes().any(|byte| byte.is_ascii_whitespace()) {
        return Err("the AccessLink token is empty or contains whitespace".into());
    }

    let client = rust_polar::polar::oauth::oauth_http_client()?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(rust_polar::polar::oauth::validate_access_token(
        &client,
        &access_token,
        user_id,
    ))?;

    let updated_at =
        time::OffsetDateTime::now_utc().format(&time::format_description::well_known::Rfc3339)?;
    let credentials = rust_polar::polar::credentials::PolarCredentials::new(
        rust_polar::polar::credentials::NewPolarCredentials {
            client_id: None,
            client_secret: None,
            access_token,
            refresh_token: None,
            token_type: Some("Bearer".to_owned()),
            expires_in: None,
            user_id: user_id.to_owned(),
            member_id: None,
            updated_at,
        },
    );
    credentials.save(root)?;
    println!("AccessLink token validated and saved in the private credentials directory.");
    println!("The token will not be printed; check its expiration before unattended sync.");
    Ok(())
}

/// Read a secret from a real terminal with echo disabled. Requiring a TTY
/// prevents callers from accidentally passing a token through a pipe or log.
#[cfg(unix)]
fn read_hidden(prompt: &str) -> Result<String, std::io::Error> {
    use std::io::{self, IsTerminal, Write};
    use std::os::fd::AsRawFd;

    let stdin = io::stdin();
    if !stdin.is_terminal() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "token import requires an interactive terminal",
        ));
    }
    let fd = stdin.as_raw_fd();
    let mut original = std::mem::MaybeUninit::<libc::termios>::uninit();
    // SAFETY: `fd` is stdin's open terminal descriptor and `original` is
    // writable storage for the termios structure requested by tcgetattr.
    if unsafe { libc::tcgetattr(fd, original.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: tcgetattr returned success and initialized the structure.
    let original = unsafe { original.assume_init() };
    let mut hidden = original;
    hidden.c_lflag &= !libc::ECHO;
    // SAFETY: `hidden` is a valid copy of the terminal's current settings.
    if unsafe { libc::tcsetattr(fd, libc::TCSAFLUSH, &hidden) } != 0 {
        return Err(std::io::Error::last_os_error());
    }

    struct RestoreEcho {
        fd: libc::c_int,
        settings: libc::termios,
    }
    impl Drop for RestoreEcho {
        fn drop(&mut self) {
            // SAFETY: the descriptor remains stdin for this process, and the
            // saved settings came from tcgetattr on that same descriptor.
            let _ = unsafe { libc::tcsetattr(self.fd, libc::TCSAFLUSH, &self.settings) };
        }
    }
    let restore = RestoreEcho {
        fd,
        settings: original,
    };

    eprint!("{prompt}");
    io::stderr().flush()?;
    let mut secret = String::new();
    let read_result = stdin.read_line(&mut secret);
    drop(restore);
    eprintln!();
    read_result?;
    while secret.ends_with(['\n', '\r']) {
        secret.pop();
    }
    Ok(secret)
}

#[cfg(not(unix))]
fn read_hidden(_prompt: &str) -> Result<String, std::io::Error> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "hidden token input is supported only on Unix terminals",
    ))
}

#[cfg(unix)]
fn unix_api_request(
    socket_path: &std::path::Path,
    method: &str,
    request_path: &str,
) -> Result<Option<LocalApiReply>, Box<dyn std::error::Error>> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    if !socket_path.exists() {
        return Ok(None);
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let response = runtime.block_on(async {
        let mut stream = match tokio::net::UnixStream::connect(socket_path).await {
            Ok(stream) => stream,
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
                ) =>
            {
                return Ok::<_, Box<dyn std::error::Error>>(None);
            }
            Err(error) => return Err(Box::new(error) as Box<dyn std::error::Error>),
        };
        let request = format!(
            "{method} {request_path} HTTP/1.1\r\nHost: rust-polar\r\nConnection: close\r\n\r\n"
        );
        stream.write_all(request.as_bytes()).await?;
        let mut response = Vec::new();
        let mut buffer = [0_u8; 8192];
        loop {
            let count = stream.read(&mut buffer).await?;
            if count == 0 {
                break;
            }
            if response.len().saturating_add(count) > 8 * 1024 * 1024 {
                return Err("local API response exceeds 8 MiB".into());
            }
            response.extend_from_slice(&buffer[..count]);
        }
        Ok(Some(response))
    })?;
    let Some(response) = response else {
        return Ok(None);
    };
    let boundary = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or("local API returned an invalid HTTP response")?;
    let header = std::str::from_utf8(&response[..boundary])?;
    let status = header
        .lines()
        .next()
        .and_then(|line| line.split_ascii_whitespace().nth(1))
        .ok_or("local API response has no status")?
        .parse::<u16>()?;
    Ok(Some(LocalApiReply {
        status,
        body: response[boundary + 4..].to_vec(),
    }))
}

#[cfg(not(unix))]
fn unix_api_request(
    _socket_path: &std::path::Path,
    _method: &str,
    _request_path: &str,
) -> Result<Option<LocalApiReply>, Box<dyn std::error::Error>> {
    Ok(None)
}

fn percent_encode(value: &[u8]) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value {
        if byte.is_ascii_alphanumeric() || matches!(*byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(*byte));
        } else {
            use std::fmt::Write as _;
            let _ = write!(encoded, "%{byte:02X}");
        }
    }
    encoded
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() -> Result<(), std::io::Error> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut terminate = signal(SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() -> Result<(), std::io::Error> {
    tokio::signal::ctrl_c().await
}

fn default_data_dir() -> PathBuf {
    if let Some(path) = std::env::var_os("RUST_POLAR_DATA_DIR") {
        return PathBuf::from(path);
    }
    #[cfg(target_os = "macos")]
    let base = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
        .join("Library/Application Support");
    #[cfg(not(target_os = "macos"))]
    let base = std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::var_os("HOME")
                .map(PathBuf::from)
                .unwrap_or_else(std::env::temp_dir)
                .join(".local/share")
        });
    base.join("rust-polar")
}

fn socket_path(data_dir: &std::path::Path) -> PathBuf {
    std::env::var_os("RUST_POLAR_SOCKET")
        .map(PathBuf::from)
        .unwrap_or_else(|| data_dir.join("run/rust-polar.sock"))
}

#[cfg(test)]
mod config_tests {
    use super::parse_sync_interval;

    #[test]
    fn sync_interval_accepts_documented_operating_range() {
        assert_eq!(parse_sync_interval("30").unwrap(), 30);
        assert_eq!(parse_sync_interval("1800").unwrap(), 1800);
        assert_eq!(parse_sync_interval("2592000").unwrap(), 2_592_000);
    }

    #[test]
    fn sync_interval_rejects_busy_and_unreasonably_long_schedules() {
        assert!(parse_sync_interval("0").is_err());
        assert!(parse_sync_interval("29").is_err());
        assert!(parse_sync_interval("2592001").is_err());
        assert!(parse_sync_interval("not-a-number").is_err());
    }
}
