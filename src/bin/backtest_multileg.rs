//! Multi-leg premium-selling backtest — replays `*_option_selling_ticks.csv` through the same
//! logic as live `multileg` (regime pick → one trade/day → manage exits).

use satavahana::config::{default_config_path, Config};
use satavahana::multileg_replay::{replay_many, MultilegDayResult};

use std::env;
use std::path::{Path, PathBuf};

fn print_usage(bin: &str) {
    eprintln!(
        "Usage: {bin} [--all] [--capital N] [selling_csv_files...]\n\
         \n\
         Replays *_option_selling_ticks.csv (bid/ask + greeks required).\n\
         Picks the best structure per day via opening-regime indicators (matches live).\n\
         One multi-leg trade/day; 0/1-DTE open, far-DTE confirmed-sideways gate matches live.\n\
         \n\
         Examples:\n\
           {bin} ../data/2026-06-23_option_selling_ticks.csv\n\
           {bin} --all\n\
           {bin} --capital 15000 --all"
    );
}

fn selling_files_in_data() -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = std::fs::read_dir("../data")
        .ok()
        .into_iter()
        .flatten()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| (n.ends_with("_option_selling_ticks.csv")
                          || n.ends_with("_option_selling_ticks.csv.gz"))
                         && !n.contains("_trim"))
                .unwrap_or(false)
        })
        .collect();
    paths.sort();
    paths
}

fn format_day(r: &MultilegDayResult) -> String {
    if !r.traded {
        return format!(
            "  {}: {}",
            r.day,
            r.skip_reason.as_deref().unwrap_or("-")
        );
    }
    format!(
        "  {}  {:?} x{}lot  ER {:.2}  [{}]  net ₹{:+.2}",
        r.day,
        r.structure.unwrap(),
        r.lots,
        r.er.unwrap_or(0.0),
        r.exit_reason,
        r.net_pnl
    )
}

fn main() {
    let bin = env::args().next().unwrap_or_else(|| "backtest_multileg".into());
    let mut args: Vec<String> = env::args().skip(1).collect();
    let mut all = false;
    let mut capital: Option<f64> = None;
    let mut paths: Vec<PathBuf> = Vec::new();

    while let Some(a) = args.first().cloned() {
        match a.as_str() {
            "-h" | "--help" => {
                print_usage(&bin);
                return;
            }
            "--all" => {
                all = true;
                args.remove(0);
            }
            "--capital" => {
                args.remove(0);
                capital = Some(
                    args.first()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or_else(|| {
                            eprintln!("--capital requires a number");
                            std::process::exit(1);
                        }),
                );
                args.remove(0);
            }
            s if s.starts_with("--capital=") => {
                capital = Some(
                    s.split('=')
                        .nth(1)
                        .and_then(|v| v.parse().ok())
                        .unwrap_or_else(|| {
                            eprintln!("invalid --capital");
                            std::process::exit(1);
                        }),
                );
                args.remove(0);
            }
            _ => {
                paths.push(PathBuf::from(args.remove(0)));
            }
        }
    }

    if all {
        paths = selling_files_in_data();
    }
    if paths.is_empty() {
        print_usage(&bin);
        std::process::exit(1);
    }

    let start_cap = capital.unwrap_or_else(|| {
        Config::load(default_config_path())
            .map(|c| c.options_engine.initial_capital)
            .unwrap_or(15_000.0)
    }).max(0.0);

    let path_refs: Vec<&Path> = paths.iter().map(|p| p.as_path()).collect();
    let summary = replay_many(&path_refs, start_cap).unwrap_or_else(|e| {
        eprintln!("replay failed: {e}");
        std::process::exit(1);
    });

    println!("==============================================================");
    println!("SATAVAHANA — MULTI-LEG BACKTEST (Rust / live-parity replay)");
    println!("==============================================================");
    println!("Starting capital    : ₹{start_cap:.2}");
    println!("Days replayed       : {}", summary.days.len());
    println!("Regime picker       : Condor | Tight | Fly | WideFly (best score)");
    println!("Trades/day cap      : 1 (0/1-DTE open; far-DTE confirmed-sideways gate)\n");

    for d in &summary.days {
        println!("{}", format_day(d));
    }

    let traded: Vec<_> = summary.days.iter().filter(|d| d.traded).collect();
    let wins = traded.iter().filter(|d| d.net_pnl > 0.0).count();
    let losses = traded.iter().filter(|d| d.net_pnl < 0.0).count();
    let total = summary.end_capital - summary.start_capital;

    println!();
    if traded.is_empty() {
        println!("    no trading days");
    } else {
        println!(
            "    days {}/{} traded | win-rate {:.0}% ({}W/{}L) | total ₹{:+.2} ({:+.2}%) | final ₹{:.2}",
            traded.len(),
            summary.days.len(),
            wins as f64 / traded.len() as f64 * 100.0,
            wins,
            losses,
            total,
            total / summary.start_capital * 100.0,
            summary.end_capital
        );
    }
    println!("==============================================================");
}
