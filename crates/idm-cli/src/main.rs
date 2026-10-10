//! # IDM CLI - Parallel Segmented File Downloader
//!
//! Thin CLI wrapper around `idm-core` providing console progress visualization,
//! interactive prompts, graceful Ctrl-C handling, and queue inspection.

mod progress;

use std::io::IsTerminal;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use idm_core::{AddDownloadOptions, DownloadEvent, DownloadManager, DownloadStatus};
use indicatif::HumanBytes;
use progress::setup_progress_bar;

#[derive(Debug, Clone)]
enum CliCommand {
    Download {
        url: String,
        output_path: Option<PathBuf>,
        connections: usize,
        chunk_size: u64,
        force: bool,
        limit_rate: Option<u64>,
    },
    List,
    Pause { id: String },
    Resume { id: String },
    Cancel { id: String },
    Help,
}

fn print_usage() {
    println!(
        r#"IDM - Asynchronous Parallel Segmented File Downloader

USAGE:
    idm [OPTIONS] <URL>
    idm <COMMAND>

COMMANDS:
    list                  List all downloads in the persistent queue
    pause <ID>            Pause an active or pending download
    resume <ID>           Resume a paused or failed download
    cancel <ID>           Cancel and stop a download
    help                  Show this help message

OPTIONS:
    -c, --connections <N>     Number of parallel connections per download (default: 8)
    -s, --chunk-size <BYTES>  Size of each segmented chunk in bytes (default: 4194304 / 4 MiB)
    -o, --output <PATH>       Target file destination path
    -f, --force               Overwrite destination file if it already exists
    -l, --limit-rate <BYTES>  Global transfer speed limit in bytes per second
    -h, --help                Show help information

EXAMPLES:
    idm https://example.com/largefile.iso
    idm -c 16 -s 1048576 https://example.com/largefile.iso
    idm --limit-rate 1048576 https://example.com/largefile.iso
    idm list
    idm resume <ID>
"#
    );
}

fn parse_cli_args() -> Result<CliCommand> {
    let args: Vec<String> = std::env::args().skip(1).collect();

    if args.is_empty() {
        return Ok(CliCommand::Help);
    }

    let first = &args[0];
    if first == "help" || first == "--help" || first == "-h" {
        return Ok(CliCommand::Help);
    } else if first == "list" {
        return Ok(CliCommand::List);
    } else if first == "pause" {
        let id = args.get(1).cloned().context("Missing download ID to pause")?;
        return Ok(CliCommand::Pause { id });
    } else if first == "resume" {
        let id = args.get(1).cloned().context("Missing download ID to resume")?;
        return Ok(CliCommand::Resume { id });
    } else if first == "cancel" {
        let id = args.get(1).cloned().context("Missing download ID to cancel")?;
        return Ok(CliCommand::Cancel { id });
    }

    // Parse download command with flags
    let mut url: Option<String> = None;
    let mut connections = 8;
    let mut chunk_size = 4 * 1024 * 1024;
    let mut output_path = None;
    let mut force = false;
    let mut limit_rate = None;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "-c" | "--connections" => {
                i += 1;
                connections = args.get(i).context("Missing value for --connections")?.parse()?;
            }
            "-s" | "--chunk-size" => {
                i += 1;
                chunk_size = args.get(i).context("Missing value for --chunk-size")?.parse()?;
            }
            "-o" | "--output" => {
                i += 1;
                output_path = Some(PathBuf::from(args.get(i).context("Missing value for --output")?));
            }
            "-f" | "--force" => {
                force = true;
            }
            "-l" | "--limit-rate" => {
                i += 1;
                let rate_str = args.get(i).context("Missing value for --limit-rate")?;
                limit_rate = Some(rate_str.parse()?);
            }
            "-h" | "--help" => return Ok(CliCommand::Help),
            arg if !arg.starts_with('-') => {
                url = Some(arg.to_string());
            }
            unknown => {
                bail!("Unknown argument '{}'. Run with --help for usage.", unknown);
            }
        }
        i += 1;
    }

    let url = url.context("No target URL specified. Run with --help for usage.")?;

    Ok(CliCommand::Download {
        url,
        output_path,
        connections,
        chunk_size,
        force,
        limit_rate,
    })
}

fn get_database_path() -> PathBuf {
    if let Ok(dir) = std::env::var("IDM_DATA_DIR") {
        PathBuf::from(dir).join("idm_queue.db")
    } else if let Some(home) = std::env::var_os("HOME") {
        PathBuf::from(home).join(".idm").join("idm_queue.db")
    } else {
        PathBuf::from("idm_queue.db")
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let command = match parse_cli_args() {
        Ok(cmd) => cmd,
        Err(e) => {
            eprintln!("Error: {}", e);
            print_usage();
            std::process::exit(1);
        }
    };

    let db_path = get_database_path();
    let manager = DownloadManager::new(&db_path, Some(4))?;

    match command {
        CliCommand::Help => {
            print_usage();
            Ok(())
        }
        CliCommand::List => {
            let downloads = manager.list()?;
            if downloads.is_empty() {
                println!("No downloads in queue.");
                return Ok(());
            }

            println!("{:<36} {:<12} {:<12} {:<12} {:<}", "ID", "STATUS", "DOWNLOADED", "TOTAL", "DESTINATION");
            println!("{}", "-".repeat(100));
            for d in downloads {
                let total_s = d.total_size.map(|s| HumanBytes(s).to_string()).unwrap_or_else(|| "Unknown".to_string());
                println!(
                    "{:<36} {:<12} {:<12} {:<12} {:<}",
                    d.id,
                    d.status.to_string(),
                    HumanBytes(d.downloaded_bytes).to_string(),
                    total_s,
                    d.destination_path.display()
                );
            }
            Ok(())
        }
        CliCommand::Pause { id } => {
            manager.pause(&id).await?;
            println!("Download '{}' paused.", id);
            Ok(())
        }
        CliCommand::Resume { id } => {
            manager.resume(&id).await?;
            println!("Download '{}' resumed.", id);
            Ok(())
        }
        CliCommand::Cancel { id } => {
            manager.cancel(&id).await?;
            println!("Download '{}' cancelled.", id);
            Ok(())
        }
        CliCommand::Download {
            url,
            output_path,
            connections,
            chunk_size,
            force,
            limit_rate,
        } => {
            // Check existing file prompt before adding
            if let Some(ref path) = output_path
                && path.exists()
            {
                if force {
                    println!("Notice: Overwriting existing destination file '{}' (--force specified).", path.display());
                } else if !cfg!(test) && std::io::stdin().is_terminal() {
                    use std::io::Write;
                    print!("File '{}' already exists. Overwrite? (y/N): ", path.display());
                    std::io::stdout().flush()?;
                    let mut input = String::new();
                    std::io::stdin().read_line(&mut input)?;
                    let trimmed = input.trim();
                    if !trimmed.eq_ignore_ascii_case("y") && !trimmed.eq_ignore_ascii_case("yes") {
                        bail!("Download aborted: File '{}' already exists.", path.display());
                    }
                } else {
                    bail!("Destination file '{}' already exists. Use --force to overwrite.", path.display());
                }
            }

            // Configure speed limiter if specified
            if let Some(rate) = limit_rate {
                manager.set_speed_limit(Some(rate)).await;
                println!("Configured global speed limit: {}/s", HumanBytes(rate));
            }

            let download_id = manager
                .add(AddDownloadOptions {
                    url: url.clone(),
                    destination_path: output_path,
                    chunk_size: Some(chunk_size),
                    connections: Some(connections),
                    force,
                })
                .await?;

            let mut rx = manager.subscribe(&download_id);

            // Double Ctrl-C handler
            let interrupted = Arc::new(AtomicBool::new(false));
            let interrupted_clone = Arc::clone(&interrupted);
            let mgr_clone = manager.clone();
            let dl_id_clone = download_id.clone();

            tokio::spawn(async move {
                if tokio::signal::ctrl_c().await.is_ok() {
                    interrupted_clone.store(true, Ordering::SeqCst);
                    eprintln!("\n Received Ctrl-C! Stopping workers and persisting state...");
                    mgr_clone.pause(&dl_id_clone).await.ok();

                    // Wait for potential second Ctrl-C to force-exit
                    if tokio::signal::ctrl_c().await.is_ok() {
                        eprintln!("\n Forcefully aborting download (second Ctrl-C received). Exiting.");
                        std::process::exit(130);
                    }
                }
            });

            let mut pb: Option<indicatif::ProgressBar> = None;

            while let Ok(event) = rx.recv().await {
                match event {
                    DownloadEvent::StateChanged { status, .. } => match status {
                        DownloadStatus::Downloading => {
                            if pb.is_none() {
                                println!("Starting download: {}", url);
                            }
                        }
                        DownloadStatus::Completed => {
                            if let Some(bar) = pb.take() {
                                bar.finish_and_clear();
                            }
                            if let Ok(Some(record)) = manager.get(&download_id) {
                                println!(
                                    "\n Download completed successfully: '{}' ({})",
                                    record.destination_path.display(),
                                    HumanBytes(record.downloaded_bytes)
                                );
                            } else {
                                println!("\n Download completed successfully.");
                            }
                            return Ok(());
                        }
                        DownloadStatus::Paused => {
                            if let Some(bar) = pb.take() {
                                bar.finish_and_clear();
                            }
                            println!("\n Download paused. You can resume this download at any time by rerunning the same command or `idm resume {}`.", download_id);
                            return Ok(());
                        }
                        DownloadStatus::Failed => {
                            if let Some(bar) = pb.take() {
                                bar.finish_and_clear();
                            }
                            bail!("Download failed.");
                        }
                        DownloadStatus::Cancelled => {
                            if let Some(bar) = pb.take() {
                                bar.finish_and_clear();
                            }
                            println!("\n Download cancelled.");
                            return Ok(());
                        }
                        DownloadStatus::Pending => {}
                    },
                    DownloadEvent::Progress {
                        downloaded_bytes,
                        total_bytes,
                        ..
                    } => {
                        let bar = match pb.as_ref() {
                            Some(bar) => bar,
                            None => {
                                let new_bar = setup_progress_bar(total_bytes)?;
                                pb = Some(new_bar);
                                pb.as_ref().unwrap()
                            }
                        };
                        bar.set_position(downloaded_bytes);
                    }
                    DownloadEvent::Error { message, .. } => {
                        eprintln!("\n Error: {}", message);
                    }
                }
            }

            Ok(())
        }
    }
}
