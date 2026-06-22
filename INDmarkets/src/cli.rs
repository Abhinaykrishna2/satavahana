//! Command-line parsing for the live engine.
//!
//! Paper trading is the strict default. The `--live` flag is the *only* way to
//! route real capital to the Zerodha order API; omitting it simulates fills
//! locally against the live tape.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunMode {
    /// Simulate fills locally against the live order book — no capital at risk.
    Paper,
    /// Route real orders to the Zerodha Kite API.
    Live,
}

impl RunMode {
    pub fn is_live(self) -> bool {
        matches!(self, RunMode::Live)
    }

    pub fn label(self) -> &'static str {
        match self {
            RunMode::Paper => "PAPER",
            RunMode::Live => "LIVE",
        }
    }
}

#[derive(Debug, Clone)]
pub struct LiveArgs {
    pub mode: RunMode,
    /// Whether to run the new microstructure engine (Strategy 1 + 2) at all.
    /// `--live` or `--micro` turns it on; `config.microstructure.enabled` can
    /// also enable it independently of the CLI.
    pub microstructure: bool,
}

impl Default for LiveArgs {
    fn default() -> Self {
        LiveArgs {
            mode: RunMode::Paper,
            microstructure: false,
        }
    }
}

/// Parse run flags from an argument iterator (excluding argv[0] is fine; argv[0]
/// is just ignored as it won't match any flag).
///
/// Flags:
///   --live              run the microstructure engine and route REAL orders
///   --paper             explicit paper mode (default)
///   --micro / --microstructure   run the microstructure engine in paper mode
pub fn parse_args<I, S>(args: I) -> LiveArgs
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut live = false;
    let mut paper = false;
    let mut micro = false;

    for a in args {
        match a.as_ref() {
            "--live" => live = true,
            "--paper" => paper = true,
            "--micro" | "--microstructure" => micro = true,
            _ => {}
        }
    }

    // --paper always wins over --live as an explicit safety override.
    let mode = if live && !paper {
        RunMode::Live
    } else {
        RunMode::Paper
    };

    LiveArgs {
        mode,
        microstructure: live || micro,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_paper_no_engine() {
        let a = parse_args(Vec::<String>::new());
        assert_eq!(a.mode, RunMode::Paper);
        assert!(!a.microstructure);
    }

    #[test]
    fn live_flag_enables_live_and_engine() {
        let a = parse_args(["satavahana", "--live"]);
        assert_eq!(a.mode, RunMode::Live);
        assert!(a.microstructure);
    }

    #[test]
    fn micro_flag_runs_engine_in_paper() {
        let a = parse_args(["--micro"]);
        assert_eq!(a.mode, RunMode::Paper);
        assert!(a.microstructure);
    }

    #[test]
    fn explicit_paper_overrides_live() {
        let a = parse_args(["--live", "--paper"]);
        assert_eq!(a.mode, RunMode::Paper);
        // engine still runs, just simulated
        assert!(a.microstructure);
    }
}
