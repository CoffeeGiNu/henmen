// mod curation;
mod delegation;
mod termination;

use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;
use std::sync::Arc;

use crate::delegation::adapters::codex::CodexWorker;
use crate::delegation::adapters::session::SessionFiles;
use crate::delegation::application::{
    Mode, SessionId, WorkerRequest, WorkerResponse, delegate, resume,
};
use crate::delegation::ports::{SessionStore, Worker};
use crate::termination::Termination;

#[derive(Parser)]
#[command(name = "henmen", version)]
struct CommandLineInterface {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Delegate {
        task: String,
        #[command(flatten)]
        options: TurnOptions,
    },
    Resume {
        session_id: String,
        task: String,
        #[command(flatten)]
        options: TurnOptions,
    },
}

#[derive(clap::Args)]
struct TurnOptions {
    #[arg(long)]
    model: String,
    #[arg(long)]
    effort: String,
    #[arg(long, value_enum)]
    mode: CommandLineMode,
    #[arg(long)]
    cwd: Option<PathBuf>,
}

#[derive(Clone, Copy, ValueEnum)]
enum CommandLineMode {
    Inspect,
    Edit,
}

impl From<CommandLineMode> for Mode {
    fn from(mode: CommandLineMode) -> Self {
        match mode {
            CommandLineMode::Inspect => Mode::Inspect,
            CommandLineMode::Edit => Mode::Edit,
        }
    }
}

#[derive(serde::Serialize)]
struct CommandOutput {
    session_id: SessionId,
    response: WorkerResponse,
}

fn sessions_directory() -> anyhow::Result<PathBuf> {
    if let Some(state_home) = std::env::var_os("XDG_STATE_HOME") {
        return Ok(PathBuf::from(state_home).join("henmen").join("sessions"));
    }
    let home: std::ffi::OsString = std::env::var_os("HOME")
        .ok_or_else(|| anyhow::anyhow!("neither XDG_STATE_HOME nor HOME is set"))?;
    Ok(PathBuf::from(home)
        .join(".local")
        .join("state")
        .join("henmen")
        .join("sessions"))
}

fn build_request(task: String, options: TurnOptions) -> anyhow::Result<WorkerRequest> {
    let cwd: PathBuf = match options.cwd {
        Some(path) => std::fs::canonicalize(path)?,
        None => std::env::current_dir()?,
    };
    Ok(WorkerRequest {
        task,
        cwd,
        model: options.model,
        effort: options.effort,
        mode: options.mode.into(),
    })
}

fn install_termination_handler(termination: Arc<Termination>) -> anyhow::Result<()> {
    ctrlc::set_handler(move || {
        termination.request();
        if let Some(process_id) = termination.child_process_id() {
            let _ = nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(process_id as i32),
                nix::sys::signal::Signal::SIGTERM,
            );
        }
    })?;
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let command_line_interface: CommandLineInterface = CommandLineInterface::parse();

    let termination: Arc<Termination> = Arc::new(Termination::new());
    install_termination_handler(Arc::clone(&termination))?;

    let worker: CodexWorker = CodexWorker::new(termination);
    let store: SessionFiles = SessionFiles::new(sessions_directory()?);

    let (session_id, response): (SessionId, WorkerResponse) = match command_line_interface.command {
        Command::Delegate { task, options } => run_delegate(&worker, &store, task, options)?,
        Command::Resume {
            session_id,
            task,
            options,
        } => run_resume(&worker, &store, SessionId(session_id), task, options)?,
    };

    let output: CommandOutput = CommandOutput {
        session_id,
        response,
    };
    println!("{}", serde_json::to_string(&output)?);
    Ok(())
}

fn run_delegate(
    worker: &dyn Worker,
    store: &dyn SessionStore,
    task: String,
    options: TurnOptions,
) -> anyhow::Result<(SessionId, WorkerResponse)> {
    delegate(worker, store, &build_request(task, options)?)
}

fn run_resume(
    worker: &dyn Worker,
    store: &dyn SessionStore,
    session_id: SessionId,
    task: String,
    options: TurnOptions,
) -> anyhow::Result<(SessionId, WorkerResponse)> {
    resume(worker, store, &session_id, &build_request(task, options)?)
}
