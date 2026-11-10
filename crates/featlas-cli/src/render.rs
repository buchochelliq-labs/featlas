//! Human-readable (`--format text`) output, rendered with rs-rich.
//!
//! Only presentation lives here: every value comes from the core unchanged. `--format json`
//! and the MCP server never pass through this module, so machine output stays byte-stable.
//! rs-rich follows upstream `rich`: colour only on a terminal, none when `NO_COLOR` is set,
//! and `COLUMNS` sets the width when output is piped.

use featlas_core::diag::{Diagnostic, Severity};
use rich::console::Overflow;
use rich::markup::escape;
use rich::{ColumnOptions, Console, Table, Text};

/// Soft wrap keeps one-line summaries on one line (scripts take `tail -1` of them); tables
/// still fit the console width.
pub fn console() -> Console {
    Console::builder().soft_wrap(true).build()
}

/// Print markup; callers escape every value taken from a project or the core.
pub fn line(c: &Console, markup: &str) {
    c.print_str(markup);
}

pub fn e(s: &str) -> String {
    escape(s)
}

pub fn verdict(passed: bool) -> &'static str {
    if passed {
        "[bold green]PASS[/]"
    } else {
        "[bold red]FAILED[/]"
    }
}

pub fn diagnostics(c: &Console, diags: &[Diagnostic]) {
    for d in diags {
        let (style, label) = match d.severity {
            Severity::Error => ("bold red", "error"),
            Severity::Warning => ("yellow", "warning"),
            _ => ("dim", "info"),
        };
        line(c, &format!("[{style}]{label:7}[/] {}", e(&d.to_string())));
    }
}

/// A table whose cells are plain values (escaped here) with optional per-cell markup.
pub struct Grid {
    table: Table,
}

impl Grid {
    pub fn new(title: &str, headers: &[&str]) -> Self {
        let mut table = Table::new().title(e(title));
        for h in headers {
            // Identifiers and reasons are evidence: fold them onto more lines, never elide them.
            let opts = ColumnOptions {
                overflow: Overflow::Fold,
                ..Default::default()
            };
            table.add_column_with(Text::new(*h), opts);
        }
        Grid { table }
    }

    /// `cells` are already markup; use [`e`] for raw values.
    pub fn row(&mut self, cells: &[String]) {
        let refs: Vec<&str> = cells.iter().map(String::as_str).collect();
        self.table.add_row(&refs);
    }

    pub fn print(&self, c: &Console) {
        c.print(&self.table);
    }
}

pub fn json(c: &Console, value: &serde_json::Value) {
    let text = serde_json::to_string_pretty(value).unwrap();
    if c.print_json(&text, &Default::default()).is_err() {
        println!("{text}");
    }
}
