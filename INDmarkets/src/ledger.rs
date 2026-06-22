
use chrono::{DateTime, FixedOffset, Utc};
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use tracing::{info, warn};


fn ist_offset() -> FixedOffset {
    FixedOffset::east_opt(5 * 3600 + 30 * 60).expect("valid IST offset")
}

fn now_ist() -> DateTime<FixedOffset> { Utc::now().with_timezone(&ist_offset()) }

fn now_str() -> String {
    now_ist().format("%Y-%m-%d %H:%M:%S").to_string()
}

fn date_str() -> String {
    now_ist().format("%Y-%m-%d").to_string()
}

fn csv_esc(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

fn open_csv(path: &Path, header: &str) -> std::io::Result<BufWriter<File>> {
    let is_new = !path.exists() || path.metadata().map(|m| m.len() == 0).unwrap_or(true);
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    let mut writer = BufWriter::new(file);
    if is_new {
        writeln!(writer, "{}", header)?;
        writer.flush()?;
    }
    Ok(writer)
}


#[derive(Debug, Clone, Default)]
pub struct EntryContext {
    pub signal_id: u64,
    pub confidence: f64,
    pub strategy: String,
    pub session: String,
    pub regime: String,
    pub spot: f64,
    pub atm_iv_pct: f64,
    pub iv_rank: f64,
    pub pcr_oi: f64,
    pub pcr_vol: f64,
    pub max_pain: f64,
    pub net_gex: f64,
    pub days_to_expiry: f64,
    pub risk_reward: f64,
    pub breakeven_pct: f64,
    pub signal_reasons: String,
}

const OPTIONS_SIGNALS_HEADER: &str =
    "signal_id,datetime,underlying,option_type,strike,expiry,\
     strategy,confidence,session,regime,\
     spot,atm_iv_pct,iv_rank,pcr_oi,pcr_vol,max_pain,net_gex,days_to_expiry,\
     entry_price,lots,lot_size,capital_deployed,target_price,stop_price,\
     risk_reward,breakeven_pct,\
     signal_reasons";

const OPTIONS_TRADES_HEADER: &str =
    "trade_id,signal_id,datetime_entry,datetime_exit,\
     underlying,option_type,strike,expiry,\
     strategy,confidence,session_entry,regime_entry,\
     spot_at_entry,atm_iv_pct,iv_rank,pcr_oi,pcr_vol,max_pain,net_gex,days_to_expiry,\
     entry_price,exit_price,lots,lot_size,capital_deployed,\
     target_price,stop_price,exit_reason,\
     gross_pnl,transaction_costs,net_pnl,net_pnl_pct,capital_after,\
     holding_duration_mins,outcome,\
     signal_reasons";

pub struct OptionsJournal {
    signals_writer: BufWriter<File>,
    trades_writer:  BufWriter<File>,
    summary_path:   PathBuf,
    pub total_signals: u64,
    pub total_trades:  u64,
    pub wins:   u32,
    pub losses: u32,
    pub total_gross_pnl: f64,
    pub total_costs:     f64,
    pub total_net_pnl:   f64,
    pub initial_capital: f64,
    pub final_capital:   f64,
    pub strategy_stats: std::collections::HashMap<String, (u32, u32, u32, f64)>,
    pub winning_pcr:     Vec<f64>,
    pub winning_iv_rank: Vec<f64>,
    pub winning_session: Vec<String>,
}

impl OptionsJournal {
    /// `replay_date`: pass `Some("YYYY-MM-DD")` during backtests so log files are named
    /// after the data date rather than today. Pass `None` for live trading.
    pub fn new(log_dir: &str, initial_capital: f64, replay_date: Option<&str>) -> std::io::Result<Self> {
        fs::create_dir_all(log_dir)?;
        let d = replay_date.map(str::to_string).unwrap_or_else(date_str);

        let sig_path     = Path::new(log_dir).join(format!("{}_options_signals.csv", d));
        let trades_path  = Path::new(log_dir).join(format!("{}_options_trades.csv", d));
        let summary_path = Path::new(log_dir).join(format!("{}_session_summary.txt", d));

        info!("Options signals log : {}", sig_path.display());
        info!("Options trades log  : {}", trades_path.display());
        info!("Session summary     : {}", summary_path.display());

        Ok(Self {
            signals_writer: open_csv(&sig_path, OPTIONS_SIGNALS_HEADER)?,
            trades_writer:  open_csv(&trades_path, OPTIONS_TRADES_HEADER)?,
            summary_path,
            total_signals: 0,
            total_trades:  0,
            wins:   0,
            losses: 0,
            total_gross_pnl: 0.0,
            total_costs:     0.0,
            total_net_pnl:   0.0,
            initial_capital,
            final_capital: initial_capital,
            strategy_stats: std::collections::HashMap::new(),
            winning_pcr:     Vec::new(),
            winning_iv_rank: Vec::new(),
            winning_session: Vec::new(),
        })
    }

    pub fn log_signal(
        &mut self,
        signal_id: u64,
        signal_datetime: &str,
        underlying: &str,
        option_type: &str,
        strike: f64,
        expiry: &str,
        ctx: &EntryContext,
        entry_price: f64,
        lots: u32,
        lot_size: u32,
        capital_deployed: f64,
        target_price: f64,
        stop_price: f64,
    ) {
        self.total_signals += 1;
        let row = format!(
            "{},{},{},{},{},{},{},{:.1},{},{},\
             {:.2},{:.2},{:.1},{:.3},{:.3},{:.0},{:.4e},{:.2},\
             {:.2},{},{},{:.2},{:.2},{:.2},\
             {:.2},{:.2},\
             {}",
            signal_id, signal_datetime,
            underlying, option_type, strike, expiry,
            csv_esc(&ctx.strategy), ctx.confidence, csv_esc(&ctx.session), csv_esc(&ctx.regime),
            ctx.spot, ctx.atm_iv_pct, ctx.iv_rank, ctx.pcr_oi, ctx.pcr_vol,
            ctx.max_pain, ctx.net_gex, ctx.days_to_expiry,
            entry_price, lots, lot_size, capital_deployed, target_price, stop_price,
            ctx.risk_reward, ctx.breakeven_pct,
            csv_esc(&ctx.signal_reasons),
        );
        if let Err(e) = writeln!(self.signals_writer, "{}", row) {
            warn!("Ledger: failed to write options signal: {}", e);
        }
        let _ = self.signals_writer.flush();
    }

    pub fn log_trade(
        &mut self,
        trade_id: u64,
        entry_ctx: &EntryContext,
        underlying: &str,
        option_type: &str,
        strike: f64,
        expiry: &str,
        entry_datetime: &str,
        exit_datetime: &str,
        entry_price: f64,
        exit_price: f64,
        lots: u32,
        lot_size: u32,
        capital_deployed: f64,
        target_price: f64,
        stop_price: f64,
        exit_reason: &str,
        gross_pnl: f64,
        transaction_costs: f64,
        net_pnl: f64,
        net_pnl_pct: f64,
        capital_after: f64,
        holding_mins: f64,
    ) {
        self.total_trades += 1;
        let outcome = if net_pnl > 0.0 { "WIN" } else { "LOSS" };

        if net_pnl > 0.0 {
            self.wins += 1;
            self.winning_pcr.push(entry_ctx.pcr_oi);
            self.winning_iv_rank.push(entry_ctx.iv_rank);
            self.winning_session.push(entry_ctx.session.clone());
        } else {
            self.losses += 1;
        }
        self.total_gross_pnl += gross_pnl;
        self.total_costs     += transaction_costs;
        self.total_net_pnl   += net_pnl;
        self.final_capital    = capital_after;

        let strat = entry_ctx.strategy.clone();
        let entry = self.strategy_stats.entry(strat).or_insert((0, 0, 0, 0.0));
        entry.0 += 1;
        if net_pnl > 0.0 { entry.1 += 1; } else { entry.2 += 1; }
        entry.3 += net_pnl;

        let row = format!(
            "{},{},{},{},\
             {},{},{},{},\
             {},{:.1},{},{},\
             {:.2},{:.2},{:.1},{:.3},{:.3},{:.0},{:.4e},{:.2},\
             {:.2},{:.2},{},{},{:.2},\
             {:.2},{:.2},{},\
             {:.2},{:.2},{:.2},{:.2},{:.2},\
             {:.1},{},\
             {}",
            trade_id, entry_ctx.signal_id, entry_datetime, exit_datetime,
            underlying, option_type, strike, expiry,
            csv_esc(&entry_ctx.strategy), entry_ctx.confidence,
            csv_esc(&entry_ctx.session), csv_esc(&entry_ctx.regime),
            entry_ctx.spot, entry_ctx.atm_iv_pct, entry_ctx.iv_rank,
            entry_ctx.pcr_oi, entry_ctx.pcr_vol, entry_ctx.max_pain,
            entry_ctx.net_gex, entry_ctx.days_to_expiry,
            entry_price, exit_price, lots, lot_size, capital_deployed,
            target_price, stop_price, csv_esc(exit_reason),
            gross_pnl, transaction_costs, net_pnl, net_pnl_pct, capital_after,
            holding_mins, outcome,
            csv_esc(&entry_ctx.signal_reasons),
        );
        if let Err(e) = writeln!(self.trades_writer, "{}", row) {
            warn!("Ledger: failed to write options trade: {}", e);
        }
        let _ = self.trades_writer.flush();

        let emoji = if net_pnl >= 0.0 { "✅" } else { "❌" };
        info!("{} LEDGER TRADE #{} | {} {} {:.0} | {:} | ₹{:+.2} ({:.1}%) | Capital: ₹{:.2}",
            emoji, trade_id, underlying, option_type, strike,
            exit_reason, net_pnl, net_pnl_pct, capital_after);
    }

    pub fn write_session_summary(&self, open_position_count: usize) {
        let mut lines: Vec<String> = Vec::new();
        let total_trades = self.wins + self.losses;
        let win_rate = if total_trades > 0 {
            self.wins as f64 / total_trades as f64 * 100.0
        } else { 0.0 };
        let capital_change = self.final_capital - self.initial_capital;
        let capital_change_pct = (capital_change / self.initial_capital) * 100.0;
        let progress_to_target = (self.final_capital / 50_000.0) * 100.0;

        lines.push(format!("╔═══════════════════════════════════════════════════════════╗"));
        lines.push(format!("║  SESSION SUMMARY — {}                       ║", date_str()));
        lines.push(format!("╚═══════════════════════════════════════════════════════════╝"));
        lines.push(String::new());
        lines.push(format!("CAPITAL"));
        lines.push(format!("  Starting : ₹{:.2}", self.initial_capital));
        lines.push(format!("  Ending   : ₹{:.2}", self.final_capital));
        lines.push(format!("  Day P&L  : ₹{:+.2}  ({:+.1}%)", capital_change, capital_change_pct));
        lines.push(format!("  Progress to ₹50,000 : {:.1}%  ({} trades remaining approx)",
            progress_to_target,
            if self.final_capital < 50_000.0 {
                let needed = 50_000.0 / self.final_capital;
                format!("~{:.0} more 30%-gain trades", needed.log(1.3).ceil())
            } else {
                "TARGET REACHED! 🎯".to_string()
            }
        ));
        lines.push(String::new());

        lines.push(format!("OPTIONS SIGNALS & TRADES"));
        lines.push(format!("  Signals generated  : {}", self.total_signals));
        lines.push(format!("  Positions tracked  : {}", self.total_trades));
        lines.push(format!("  Open (unclosed)    : {}", open_position_count));
        lines.push(format!("  Wins               : {} / {} ({:.1}% win rate)",
            self.wins, total_trades, win_rate));
        lines.push(format!("  Gross P&L          : ₹{:+.2}", self.total_gross_pnl));
        lines.push(format!("  Transaction costs  : ₹{:.2}", self.total_costs));
        lines.push(format!("  Net P&L            : ₹{:+.2}", self.total_net_pnl));
        lines.push(String::new());

        if !self.strategy_stats.is_empty() {
            lines.push(format!("STRATEGY BREAKDOWN"));
            lines.push(format!("  {:<30} {:>7} {:>5}/{:<5} {:>12}", "Strategy", "Signals", "W", "L", "Net P&L"));
            lines.push(format!("  {}", "─".repeat(65)));
            let mut strats: Vec<_> = self.strategy_stats.iter().collect();
            strats.sort_by(|a, b| b.1.3.partial_cmp(&a.1.3).unwrap());
            for (name, (signals, wins, losses, pnl)) in &strats {
                let rate = if wins + losses > 0 {
                    format!("{:.0}%", *wins as f64 / (*wins + *losses) as f64 * 100.0)
                } else { "—".to_string() };
                lines.push(format!("  {:<30} {:>7} {:>3}/{:<3} {:>6}  ₹{:>+10.2}",
                    name, signals, wins, losses, rate, pnl));
            }
            lines.push(String::new());
        }

        if !self.winning_pcr.is_empty() {
            let avg_winning_pcr = self.winning_pcr.iter().sum::<f64>() / self.winning_pcr.len() as f64;
            let avg_winning_iv  = self.winning_iv_rank.iter().sum::<f64>() / self.winning_iv_rank.len() as f64;
            lines.push(format!("WINNING TRADE CONDITIONS (improvement hints)"));
            lines.push(format!("  Avg PCR at winning entry  : {:.2}  (trades entered when PCR was near this value won)", avg_winning_pcr));
            lines.push(format!("  Avg IV Rank at winning entry: {:.1}%  (IV was cheap — confirms buy-low-IV edge)", avg_winning_iv));
            if !self.winning_session.is_empty() {
                let mut session_count: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
                for s in &self.winning_session {
                    *session_count.entry(s.as_str()).or_insert(0) += 1;
                }
                let best_session = session_count.iter().max_by_key(|e| e.1).map(|(k, _)| *k).unwrap_or("—");
                lines.push(format!("  Best winning session       : {}  (consider focusing entries here)", best_session));
            }
            lines.push(String::new());
        }

        lines.push(format!("IMPROVEMENT ACTIONS"));
        if win_rate < 50.0 && total_trades >= 3 {
            lines.push(format!("  ⚠ Win rate below 50% — consider raising min_confidence from current value by +5 points"));
        }
        if win_rate > 70.0 && total_trades >= 5 {
            lines.push(format!("  ✓ Win rate > 70% — confidence threshold is well-calibrated"));
        }
        if self.total_net_pnl < 0.0 {
            lines.push(format!("  ⚠ Net day loss — review which strategies lost; tighten stops for those"));
        }
        lines.push(format!("  → Open options_trades.csv in Excel; pivot by `strategy` and `outcome`"));
        lines.push(format!("  → Filter wins: note the iv_rank, pcr_oi, session columns"));
        lines.push(format!("  → Strategies with < 40% win rate: consider disabling or raising min_confidence"));
        lines.push(String::new());
        lines.push(format!("Log files written to logs/{}*", date_str()));

        match fs::write(&self.summary_path, lines.join("\n")) {
            Ok(_) => info!("Session summary written → {}", self.summary_path.display()),
            Err(e) => warn!("Failed to write session summary: {}", e),
        }

        for line in &lines {
            info!("{}", line);
        }
    }
}


const EQUITY_ALERTS_HEADER: &str =
    "alert_id,datetime,symbol,direction,\
     nse_price,bse_price,gross_spread,net_spread,spread_pct,\
     vol_nse,vol_bse,est_pnl_per_share,\
     nse_bid,nse_ask,bse_bid,bse_ask";

pub struct EquityJournal {
    writer: BufWriter<File>,
    pub total_alerts: u64,
    pub best_net_spread: f64,
    pub total_est_pnl: f64,
    pub symbol_stats: std::collections::HashMap<String, (u64, f64)>,
}

impl EquityJournal {
    pub fn new(log_dir: &str) -> std::io::Result<Self> {
        fs::create_dir_all(log_dir)?;
        let d = date_str();
        let path = Path::new(log_dir).join(format!("{}_equity_alerts.csv", d));
        info!("Equity alerts log   : {}", path.display());
        Ok(Self {
            writer: open_csv(&path, EQUITY_ALERTS_HEADER)?,
            total_alerts: 0,
            best_net_spread: 0.0,
            total_est_pnl: 0.0,
            symbol_stats: std::collections::HashMap::new(),
        })
    }

    pub fn log_alert(
        &mut self,
        symbol: &str,
        direction: &str,
        nse_price: f64,
        bse_price: f64,
        gross_spread: f64,
        net_spread: f64,
        spread_pct: f64,
        vol_nse: u32,
        vol_bse: u32,
        est_pnl_per_share: f64,
        nse_bid: f64,
        nse_ask: f64,
        bse_bid: f64,
        bse_ask: f64,
    ) {
        self.total_alerts += 1;
        if net_spread > self.best_net_spread { self.best_net_spread = net_spread; }
        self.total_est_pnl += est_pnl_per_share;
        let entry = self.symbol_stats.entry(symbol.to_string()).or_insert((0, 0.0));
        entry.0 += 1;
        entry.1 += net_spread;

        let row = format!(
            "{},{},{},{},\
             {:.4},{:.4},{:.4},{:.4},{:.6},\
             {},{},{:.4},\
             {:.4},{:.4},{:.4},{:.4}",
            self.total_alerts, now_str(),
            symbol, csv_esc(direction),
            nse_price, bse_price, gross_spread, net_spread, spread_pct,
            vol_nse, vol_bse, est_pnl_per_share,
            nse_bid, nse_ask, bse_bid, bse_ask,
        );
        if let Err(e) = writeln!(self.writer, "{}", row) {
            warn!("Ledger: failed to write equity alert: {}", e);
        }
        if self.total_alerts % 10 == 0 {
            let _ = self.writer.flush();
        }
    }

    pub fn write_equity_summary(&self) {
        if self.total_alerts == 0 { return; }
        info!("─── Equity Spread Summary ───────────────────────────────────");
        info!("  Total alerts this session : {}", self.total_alerts);
        info!("  Best net spread           : ₹{:.4}", self.best_net_spread);

        if !self.symbol_stats.is_empty() {
            let mut syms: Vec<_> = self.symbol_stats.iter().collect();
            syms.sort_by(|a, b| b.1.0.cmp(&a.1.0));
            info!("  Top symbols by alert count:");
            for (sym, (count, total_spread)) in syms.iter().take(5) {
                let avg = total_spread / *count as f64;
                info!("    {:10} — {} alerts | avg net spread ₹{:.4}", sym, count, avg);
            }
        }
    }

    pub fn flush(&mut self) {
        let _ = self.writer.flush();
    }
}


#[allow(dead_code)]
pub fn print_combined_shutdown_summary(
    options: &OptionsJournal,
    equity: &EquityJournal,
    open_positions: usize,
) {
    info!("════════════════════════════════════════════════════════════");
    info!("  SATAVAHANA — SESSION COMPLETE");
    info!("════════════════════════════════════════════════════════════");
    equity.write_equity_summary();
    options.write_session_summary(open_positions);
    info!("════════════════════════════════════════════════════════════");
}
