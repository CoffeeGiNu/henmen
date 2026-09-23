// mod curation;
mod delegation;
mod termination;

use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::delegation::adapters::codex::CodexWorker;
use crate::delegation::adapters::log::RunLogFile;
use crate::delegation::adapters::session::SessionFiles;
use crate::delegation::application::{
    Mode, SessionId, TimedOut, WorkerRequest, WorkerResponse, delegate, models, resume,
};
use crate::delegation::ports::{RunLog, SessionStore, Worker};
use crate::termination::Termination;

#[derive(Parser)]
#[command(
    name = "henmen",
    version,
    about = "Delegate broad code investigation to a worker agent, so the calling agent's context stays small.",
    after_long_help = "Each command prints JSON on stdout, and nothing else; diagnostics go to stderr.\n`models` returns the current backend's available models and effort options.\n`delegate` and `resume` return:\n\n  {\"session_id\": \"01M2HZ...\",\n   \"response\": {\"status\": \"done\"|\"blocked\",\n                \"summary\": \"...\",\n                \"evidence\": [\"src/retry.rs:120-145\"],\n                \"changed_files\": [],\n                \"tests\": [],\n                \"open_questions\": []}}\n\nPass that session_id to `henmen resume` to continue the same thread. A read-only\nworker that could not do what it was asked reports \"blocked\" rather than failing.\n\nSessions are stored under $XDG_STATE_HOME/henmen/sessions/, or\n~/.local/state/henmen/sessions/ when that is unset."
)]
struct CommandLineInterface {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    #[command(about = "Print available models and effort options from the current backend as JSON")]
    Models,
    #[command(about = "Run one worker turn in a fresh thread")]
    Delegate {
        #[arg(help = "What the worker should do")]
        task: String,
        #[command(flatten)]
        options: TurnOptions,
    },
    #[command(about = "Run one more turn in an existing thread, keeping its context")]
    Resume {
        #[arg(help = "Session to continue, as printed by an earlier run")]
        session_id: SessionId,
        #[arg(help = "What the worker should do next")]
        task: String,
        #[command(flatten)]
        options: TurnOptions,
    },
}

#[derive(clap::Args)]
struct TurnOptions {
    #[arg(
        long,
        help = "Model slug for the worker. See `henmen models` for available choices"
    )]
    model: String,
    #[arg(long, help = "Reasoning effort. See `henmen models` for supported values")]
    effort: String,
    #[arg(
        long,
        value_enum,
        help = "inspect is read-only; edit may write inside --cwd"
    )]
    mode: CommandLineMode,
    #[arg(
        long,
        help = "Workspace root for the worker (default: the current directory)"
    )]
    cwd: Option<PathBuf>,
    #[arg(
        long,
        default_value_t = 30,
        help = "Hard deadline for one worker execution, in minutes"
    )]
    timeout_minutes: u64,
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

fn state_directory() -> anyhow::Result<PathBuf> {
    if let Some(state_home) = std::env::var_os("XDG_STATE_HOME") {
        return Ok(PathBuf::from(state_home).join("henmen"));
    }
    let home: std::ffi::OsString = std::env::var_os("HOME")
        .ok_or_else(|| anyhow::anyhow!("neither XDG_STATE_HOME nor HOME is set"))?;
    Ok(PathBuf::from(home)
        .join(".local")
        .join("state")
        .join("henmen"))
}

fn opt_in_run_log() -> anyhow::Result<Option<RunLogFile>> {
    if std::env::var_os("HENMEN_LOG").as_deref() != Some(std::ffi::OsStr::new("1")) {
        return Ok(None);
    }
    Ok(Some(RunLogFile::new(
        state_directory()?.join("logs").join("runs.jsonl"),
    )))
}

fn build_request(task: String, options: TurnOptions) -> anyhow::Result<WorkerRequest> {
    let cwd: PathBuf = match options.cwd {
        Some(path) => std::fs::canonicalize(path)?,
        None => std::env::current_dir()?,
    };
    let timeout: Duration = Duration::from_secs(options.timeout_minutes * 60);
    Ok(WorkerRequest {
        task,
        cwd,
        model: options.model,
        effort: options.effort,
        mode: options.mode.into(),
        timeout,
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
    if matches!(&command_line_interface.command, Command::Models) {
        println!("{}", models(&worker)?);
        return Ok(());
    }
    let store: SessionFiles = SessionFiles::new(state_directory()?.join("sessions"));
    let run_log: Option<RunLogFile> = opt_in_run_log()?;
    let run_log: Option<&dyn RunLog> = run_log
        .as_ref()
        .map(|run_log_file| run_log_file as &dyn RunLog);

    let turn_result: anyhow::Result<(SessionId, WorkerResponse)> =
        match command_line_interface.command {
            Command::Models => unreachable!(),
            Command::Delegate { task, options } => {
                run_delegate(&worker, &store, run_log, task, options)
            }
            Command::Resume {
                session_id,
                task,
                options,
            } => run_resume(&worker, &store, run_log, session_id, task, options),
        };
    let (session_id, response): (SessionId, WorkerResponse) = match turn_result {
        Ok(value) => value,
        Err(error) => {
            if error.downcast_ref::<TimedOut>().is_some() {
                eprintln!("{error:#}");
                std::process::exit(124);
            }
            return Err(error);
        }
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
    run_log: Option<&dyn RunLog>,
    task: String,
    options: TurnOptions,
) -> anyhow::Result<(SessionId, WorkerResponse)> {
    delegate(worker, store, run_log, &build_request(task, options)?)
}

fn run_resume(
    worker: &dyn Worker,
    store: &dyn SessionStore,
    run_log: Option<&dyn RunLog>,
    session_id: SessionId,
    task: String,
    options: TurnOptions,
) -> anyhow::Result<(SessionId, WorkerResponse)> {
    resume(
        worker,
        store,
        run_log,
        &session_id,
        &build_request(task, options)?,
    )
}
