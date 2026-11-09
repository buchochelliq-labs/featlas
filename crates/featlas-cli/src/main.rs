//! `fe-spec`: validation, explanation, planning, capability reporting,
//! bounded local fixtures, code generation, consumer reads and an MCP server.
//!
//! No command deploys, publishes, sends messages or creates cloud resources.
//! `generate` writes files only under the requested output directory.

mod commands;
mod mcp;

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::process::ExitCode;

#[derive(Parser)]
#[command(name = "fe-spec", version, about = "Feature engineering specification 0.2.0 toolchain (featlas)")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum OutFormat {
    Text,
    Json,
}

#[derive(Subcommand)]
enum Command {
    /// Parse, schema-validate and reference/type-check a project.
    Validate {
        project: PathBuf,
        #[arg(long, value_enum, default_value = "text")]
        format: OutFormat,
    },
    /// Explain a feature: identity, stages, lineage and requirements.
    Explain {
        project: PathBuf,
        #[arg(long)]
        feature: String,
        #[arg(long, value_enum, default_value = "text")]
        format: OutFormat,
    },
    /// Print the logical and logical-semantic identities (hashes).
    Hash { project: PathBuf },
    /// Build the target-neutral plan for a deployment, with adapter preflight.
    Plan {
        project: PathBuf,
        #[arg(long)]
        deployment: String,
        #[arg(long, value_enum, default_value = "json")]
        format: OutFormat,
    },
    /// Produce an AdapterCapabilityReport for a deployment.
    Capabilities {
        project: PathBuf,
        #[arg(long)]
        deployment: String,
        /// Directory receiving local acceptance evidence used by local adapters.
        #[arg(long)]
        evidence_dir: Option<PathBuf>,
        #[arg(long)]
        assessed_at: Option<String>,
    },
    /// Check a capability report's consistency against exact project bytes.
    CheckReport {
        report: PathBuf,
        #[arg(long)]
        project: PathBuf,
    },
    /// Run reference acceptance suites through the exact local evaluator.
    Test {
        project: PathBuf,
        #[arg(long)]
        suite: Option<String>,
        #[arg(long, value_enum, default_value = "text")]
        format: OutFormat,
    },
    /// Evaluate one acceptance case and show values, states and the stage trace.
    Evaluate {
        project: PathBuf,
        #[arg(long)]
        fixture: String,
        #[arg(long, value_enum, default_value = "json")]
        format: OutFormat,
    },
    /// Compute per-feature coverage states from source-status evidence (used by hosts).
    Coverage {
        project: PathBuf,
        #[arg(long)]
        deployment: String,
        #[arg(long)]
        feature_set: String,
        #[arg(long)]
        as_of: String,
        /// JSON array of source-status records.
        #[arg(long)]
        status: PathBuf,
    },
    /// Generate plan, capability report and adapter code into a directory (no execution).
    Generate {
        project: PathBuf,
        #[arg(long)]
        deployment: String,
        #[arg(long)]
        out: PathBuf,
    },
    /// Serve a target-neutral consumer request (local stores, or physical records supplied by a host).
    Consume {
        project: PathBuf,
        #[arg(long)]
        request: PathBuf,
        /// Physical records read by an external host for the bound target (JSON array).
        #[arg(long)]
        records: Option<PathBuf>,
    },
    /// Produce a store-migration plan between two deployments of the same project.
    MigrationPlan {
        project: PathBuf,
        #[arg(long)]
        from: String,
        #[arg(long)]
        to: String,
        #[arg(long, value_enum, default_value = "json")]
        format: OutFormat,
    },
    /// Build forward-horizon labels for a consumer from supplied rows (bounded, local).
    Label {
        project: PathBuf,
        #[arg(long)]
        consumer: String,
        /// JSON object: {"rows": [...], "source_rows": [...], "source_status": {...}}
        #[arg(long)]
        input: PathBuf,
    },
    /// Model Context Protocol server over stdio.
    Mcp {
        #[command(subcommand)]
        action: McpAction,
    },
}

#[derive(Subcommand)]
enum McpAction {
    /// Serve MCP over stdin/stdout (read-only tools; documents passed inline).
    Serve,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Validate { project, format } => commands::validate(&project, format),
        Command::Explain { project, feature, format } => commands::explain(&project, &feature, format),
        Command::Hash { project } => commands::hash(&project),
        Command::Plan { project, deployment, format } => commands::plan(&project, &deployment, format),
        Command::Capabilities { project, deployment, evidence_dir, assessed_at } => commands::capabilities(&project, &deployment, evidence_dir.as_deref(), assessed_at),
        Command::CheckReport { report, project } => commands::check_report(&report, &project),
        Command::Test { project, suite, format } => commands::test(&project, suite.as_deref(), format),
        Command::Evaluate { project, fixture, format } => commands::evaluate(&project, &fixture, format),
        Command::Coverage { project, deployment, feature_set, as_of, status } => commands::coverage(&project, &deployment, &feature_set, &as_of, &status),
        Command::Generate { project, deployment, out } => commands::generate(&project, &deployment, &out),
        Command::Consume { project, request, records } => commands::consume(&project, &request, records.as_deref()),
        Command::MigrationPlan { project, from, to, format } => commands::migration_plan(&project, &from, &to, format),
        Command::Label { project, consumer, input } => commands::label(&project, &consumer, &input),
        Command::Mcp { action: McpAction::Serve } => mcp::serve(),
    };
    match result {
        Ok(code) => code,
        Err(e) => {
            eprintln!("fe-spec: {e}");
            ExitCode::from(2)
        }
    }
}
