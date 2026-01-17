//! FGP daemon for direct PostgreSQL database operations.
//!
//! Connects to any PostgreSQL database (not just Neon-managed).
//!
//! # Usage
//! ```bash
//! fgp-postgres start           # Start daemon in background
//! fgp-postgres start -f        # Start in foreground
//! fgp-postgres stop            # Stop daemon
//! fgp-postgres status          # Check daemon status
//! fgp-postgres query "SELECT 1" # Quick query (no daemon)
//! ```

mod client;
mod service;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use fgp_daemon::{cleanup_socket, FgpServer};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::process::Command;

use crate::client::ConnectionConfig;
use crate::service::PostgresService;

const DEFAULT_SOCKET: &str = "~/.fgp/services/postgres/daemon.sock";

/// Named connection configuration stored in config file.
#[derive(Debug, Deserialize, Serialize)]
struct NamedConnection {
    url: Option<String>,
    host: Option<String>,
    port: Option<u16>,
    user: Option<String>,
    password: Option<String>,
    database: Option<String>,
    ssl: Option<bool>,
}

/// Config file structure for named connections.
#[derive(Debug, Deserialize, Serialize, Default)]
struct ConnectionsConfig {
    #[serde(default)]
    connections: HashMap<String, NamedConnection>,
    default: Option<String>,
}

/// Resolve connection configuration from multiple sources.
/// Priority: explicit params → env vars → config file default
fn resolve_connection(name: Option<&str>) -> Result<ConnectionConfig> {
    // 1. Try DATABASE_URL env var first (standard convention)
    if let Ok(url) = std::env::var("DATABASE_URL") {
        return ConnectionConfig::from_url(&url);
    }

    // 2. Try libpq-style env vars
    if std::env::var("PGHOST").is_ok() {
        return ConnectionConfig::from_env();
    }

    // 3. Try config file
    let config_path = shellexpand::tilde("~/.fgp/auth/postgres/connections.json").to_string();
    if let Ok(config_str) = std::fs::read_to_string(&config_path) {
        let config: ConnectionsConfig = serde_json::from_str(&config_str)
            .context("Failed to parse connections.json")?;

        // Use named connection or default
        let conn_name = name.or(config.default.as_deref());
        if let Some(name) = conn_name {
            if let Some(conn) = config.connections.get(name) {
                if let Some(url) = &conn.url {
                    return ConnectionConfig::from_url(url);
                }
                return Ok(ConnectionConfig {
                    host: conn.host.clone().unwrap_or_else(|| "localhost".into()),
                    port: conn.port.unwrap_or(5432),
                    user: conn.user.clone().unwrap_or_else(|| "postgres".into()),
                    password: conn.password.clone(),
                    database: conn.database.clone().unwrap_or_else(|| "postgres".into()),
                    ssl: conn.ssl.unwrap_or(false),
                });
            }
        }
    }

    anyhow::bail!(
        "No database connection configured.\n\
         Set DATABASE_URL env var, or use PGHOST/PGUSER/etc., or create ~/.fgp/auth/postgres/connections.json"
    )
}

#[derive(Parser)]
#[command(name = "fgp-postgres")]
#[command(about = "FGP daemon for direct PostgreSQL database operations")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the FGP daemon
    Start {
        /// Socket path (default: ~/.fgp/services/postgres/daemon.sock)
        #[arg(short, long, default_value = DEFAULT_SOCKET)]
        socket: String,

        /// Run in foreground (don't daemonize)
        #[arg(short, long)]
        foreground: bool,

        /// Named connection from config file
        #[arg(short, long)]
        connection: Option<String>,
    },

    /// Stop the running daemon
    Stop {
        /// Socket path
        #[arg(short, long, default_value = DEFAULT_SOCKET)]
        socket: String,
    },

    /// Check daemon status
    Status {
        /// Socket path
        #[arg(short, long, default_value = DEFAULT_SOCKET)]
        socket: String,
    },

    /// Run a quick query without starting daemon
    Query {
        /// SQL query to execute
        sql: String,

        /// Named connection from config file
        #[arg(short, long)]
        connection: Option<String>,
    },

    /// List tables in the database
    Tables {
        /// Schema to list tables from (default: public)
        #[arg(short = 'S', long, default_value = "public")]
        schema: String,

        /// Named connection from config file
        #[arg(short, long)]
        connection: Option<String>,
    },

    /// List configured connections
    Connections,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Start {
            socket,
            foreground,
            connection,
        } => cmd_start(socket, foreground, connection),
        Commands::Stop { socket } => cmd_stop(socket),
        Commands::Status { socket } => cmd_status(socket),
        Commands::Query { sql, connection } => cmd_query(sql, connection),
        Commands::Tables { schema, connection } => cmd_tables(schema, connection),
        Commands::Connections => cmd_connections(),
    }
}

fn cmd_start(socket: String, foreground: bool, connection: Option<String>) -> Result<()> {
    let socket_path = shellexpand::tilde(&socket).to_string();

    // Create parent directory
    if let Some(parent) = Path::new(&socket_path).parent() {
        std::fs::create_dir_all(parent).context("Failed to create socket directory")?;
    }

    // Resolve connection BEFORE fork
    let config = resolve_connection(connection.as_deref())?;

    let pid_file = format!("{}.pid", socket_path);

    println!("Starting fgp-postgres daemon...");
    println!("Socket: {}", socket_path);
    println!(
        "Database: {}@{}:{}/{}",
        config.user, config.host, config.port, config.database
    );

    if foreground {
        tracing_subscriber::fmt()
            .with_env_filter("fgp_postgres=debug,fgp_daemon=debug")
            .init();

        let service = PostgresService::new(config).context("Failed to create PostgresService")?;
        let server =
            FgpServer::new(service, &socket_path).context("Failed to create FGP server")?;
        server.serve().context("Server error")?;
    } else {
        use daemonize::Daemonize;

        let daemonize = Daemonize::new()
            .pid_file(&pid_file)
            .working_directory("/tmp");

        match daemonize.start() {
            Ok(_) => {
                tracing_subscriber::fmt()
                    .with_env_filter("fgp_postgres=debug,fgp_daemon=debug")
                    .init();

                let service =
                    PostgresService::new(config).context("Failed to create PostgresService")?;
                let server =
                    FgpServer::new(service, &socket_path).context("Failed to create FGP server")?;
                server.serve().context("Server error")?;
            }
            Err(e) => {
                eprintln!("Failed to daemonize: {}", e);
                std::process::exit(1);
            }
        }
    }

    Ok(())
}

fn cmd_stop(socket: String) -> Result<()> {
    let socket_path = shellexpand::tilde(&socket).to_string();
    let pid_file = format!("{}.pid", socket_path);

    if Path::new(&socket_path).exists() {
        if let Ok(client) = fgp_daemon::FgpClient::new(&socket_path) {
            if let Ok(response) = client.stop() {
                if response.ok {
                    println!("Daemon stopped.");
                    return Ok(());
                }
            }
        }
    }

    // Read PID
    let pid_str = std::fs::read_to_string(&pid_file)
        .context("Failed to read PID file - daemon may not be running")?;
    let pid: i32 = pid_str.trim().parse().context("Invalid PID in file")?;

    if !pid_matches_process(pid, "fgp-postgres") {
        anyhow::bail!("Refusing to stop PID {}: unexpected process", pid);
    }

    println!("Stopping fgp-postgres daemon (PID: {})...", pid);

    unsafe {
        libc::kill(pid, libc::SIGTERM);
    }

    std::thread::sleep(std::time::Duration::from_millis(500));

    let _ = cleanup_socket(&socket_path, Some(Path::new(&pid_file)));
    let _ = std::fs::remove_file(&pid_file);

    println!("Daemon stopped.");

    Ok(())
}

fn pid_matches_process(pid: i32, expected_name: &str) -> bool {
    let output = Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "comm="])
        .output();

    match output {
        Ok(output) if output.status.success() => {
            let command = String::from_utf8_lossy(&output.stdout);
            command.trim().contains(expected_name)
        }
        _ => false,
    }
}

fn cmd_status(socket: String) -> Result<()> {
    let socket_path = shellexpand::tilde(&socket).to_string();

    if !Path::new(&socket_path).exists() {
        println!("Status: NOT RUNNING");
        println!("Socket {} does not exist", socket_path);
        return Ok(());
    }

    use std::io::{BufRead, BufReader, Write};
    use std::os::unix::net::UnixStream;

    match UnixStream::connect(&socket_path) {
        Ok(mut stream) => {
            let request = r#"{"id":"status","v":1,"method":"health","params":{}}"#;
            writeln!(stream, "{}", request)?;
            stream.flush()?;

            let mut reader = BufReader::new(stream);
            let mut response = String::new();
            reader.read_line(&mut response)?;

            println!("Status: RUNNING");
            println!("Socket: {}", socket_path);
            println!("Health: {}", response.trim());
        }
        Err(e) => {
            println!("Status: NOT RESPONDING");
            println!("Socket exists but connection failed: {}", e);
        }
    }

    Ok(())
}

fn cmd_query(sql: String, connection: Option<String>) -> Result<()> {
    let config = resolve_connection(connection.as_deref())?;

    // Create a temporary runtime for the one-shot query
    let rt = tokio::runtime::Runtime::new()?;
    let result = rt.block_on(async {
        let client = crate::client::PostgresClient::new(config).await?;
        client.query(&sql, &[]).await
    })?;

    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

fn cmd_tables(schema: String, connection: Option<String>) -> Result<()> {
    let config = resolve_connection(connection.as_deref())?;

    let rt = tokio::runtime::Runtime::new()?;
    let result = rt.block_on(async {
        let client = crate::client::PostgresClient::new(config).await?;
        client.list_tables(&schema).await
    })?;

    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

fn cmd_connections() -> Result<()> {
    let config_path = shellexpand::tilde("~/.fgp/auth/postgres/connections.json").to_string();

    match std::fs::read_to_string(&config_path) {
        Ok(config_str) => {
            let config: ConnectionsConfig = serde_json::from_str(&config_str)?;
            println!("Configured connections:");
            for (name, conn) in &config.connections {
                let default_marker = if config.default.as_deref() == Some(name) {
                    " (default)"
                } else {
                    ""
                };
                if let Some(url) = &conn.url {
                    // Mask password in URL
                    let masked = url::Url::parse(url)
                        .map(|mut u| {
                            if u.password().is_some() {
                                let _ = u.set_password(Some("***"));
                            }
                            u.to_string()
                        })
                        .unwrap_or_else(|_| url.clone());
                    println!("  {}{}: {}", name, default_marker, masked);
                } else {
                    println!(
                        "  {}{}: {}@{}:{}/{}",
                        name,
                        default_marker,
                        conn.user.as_deref().unwrap_or("postgres"),
                        conn.host.as_deref().unwrap_or("localhost"),
                        conn.port.unwrap_or(5432),
                        conn.database.as_deref().unwrap_or("postgres")
                    );
                }
            }
        }
        Err(_) => {
            println!("No connections configured.");
            println!(
                "Create {} or set DATABASE_URL env var.",
                config_path
            );
        }
    }

    // Also show env vars if set
    if let Ok(url) = std::env::var("DATABASE_URL") {
        let masked = url::Url::parse(&url)
            .map(|mut u| {
                if u.password().is_some() {
                    let _ = u.set_password(Some("***"));
                }
                u.to_string()
            })
            .unwrap_or_else(|_| url.clone());
        println!("\nEnvironment:");
        println!("  DATABASE_URL: {}", masked);
    }

    Ok(())
}
