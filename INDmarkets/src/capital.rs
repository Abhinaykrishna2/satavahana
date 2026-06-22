//! Hourly capital synchronization.
//!
//! Per the spec, the engine polls the live Zerodha margin/cash balance every hour
//! starting at 09:14 IST and continuing until market close (15:30 IST). The polled
//! balance feeds Kelly/per-trade sizing via [`RiskManager::update_capital`].
//!
//! All scheduling is anchored to IST instants derived from absolute UTC, so the
//! sleep durations are correct regardless of the host's local timezone (e.g. EST).

use crate::execution::fetch_live_available_funds;
use crate::risk::{ist_now, ist_offset};

use std::time::Duration;

use chrono::{DateTime, FixedOffset, TimeZone};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{info, warn};

/// NSE market close (IST). No capital polls are scheduled at/after this.
const CLOSE_HOUR: u32 = 15;
const CLOSE_MINUTE: u32 = 30;

/// Compute the next capital-poll instant strictly after `now`.
///
/// Poll schedule (IST): the anchor `start_hour:start_minute`, then `:poll_minute`
/// past every subsequent hour up to the last one that still precedes market close.
/// Returns `None` once the final poll of the day has passed.
pub fn next_poll_instant(
    now: DateTime<FixedOffset>,
    start_hour: u32,
    start_minute: u32,
    poll_minute: u32,
) -> Option<DateTime<FixedOffset>> {
    let date = now.date_naive();
    let mk = |h: u32, m: u32| -> Option<DateTime<FixedOffset>> {
        date.and_hms_opt(h, m, 0)
            .and_then(|ndt| ist_offset().from_local_datetime(&ndt).single())
    };

    // Last hour at which a `:poll_minute` poll still lands before close.
    let last_poll_hour = if poll_minute < CLOSE_MINUTE {
        CLOSE_HOUR
    } else {
        CLOSE_HOUR.saturating_sub(1)
    };

    let mut candidates: Vec<DateTime<FixedOffset>> = Vec::new();
    if let Some(t) = mk(start_hour, start_minute) {
        candidates.push(t);
    }
    let mut h = start_hour + 1;
    while h <= last_poll_hour {
        if let Some(t) = mk(h, poll_minute) {
            candidates.push(t);
        }
        h += 1;
    }

    candidates.into_iter().filter(|t| *t > now).min()
}

/// Spawn the hourly capital-sync task. It sleeps to each scheduled IST instant,
/// polls live margins, and sends the funds to the position-manager task (the
/// single owner of `RiskManager`), which applies them to sizing.
pub fn spawn_capital_sync(
    api_key: String,
    access_token: String,
    tx: UnboundedSender<f64>,
    start_hhmm: String,
    poll_minute: u32,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let (start_hour, start_minute) = crate::risk::parse_hhmm(&start_hhmm).unwrap_or((9, 14));
        info!(
            "💰 Capital sync scheduled: first poll {:02}:{:02} IST, then :{:02} hourly until {:02}:{:02} IST",
            start_hour, start_minute, poll_minute, CLOSE_HOUR, CLOSE_MINUTE
        );

        loop {
            let now = ist_now();
            let Some(next) = next_poll_instant(now, start_hour, start_minute, poll_minute) else {
                info!("💰 Capital sync: final poll of the day passed; sync task ending.");
                break;
            };

            let wait = (next - now).to_std().unwrap_or(Duration::ZERO);
            info!(
                "💰 Next capital poll at {} IST (in {}s)",
                next.format("%H:%M:%S"),
                wait.as_secs()
            );
            tokio::time::sleep(wait).await;

            match fetch_live_available_funds(&api_key, &access_token).await {
                Ok(funds) => {
                    info!("💰 Capital sync @ {} IST: live margins ₹{:.2}", ist_now().format("%H:%M:%S"), funds);
                    if tx.send(funds).is_err() {
                        info!("💰 Capital sync: manager channel closed; ending.");
                        break;
                    }
                }
                Err(e) => {
                    warn!("💰 Capital sync failed ({}); keeping previous capital", e);
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Timelike;

    fn ist(h: u32, m: u32, s: u32) -> DateTime<FixedOffset> {
        ist_offset().with_ymd_and_hms(2026, 6, 16, h, m, s).single().unwrap()
    }

    #[test]
    fn first_poll_is_the_anchor_when_before_open() {
        let next = next_poll_instant(ist(9, 10, 0), 9, 14, 14).unwrap();
        assert_eq!((next.hour(), next.minute()), (9, 14));
    }

    #[test]
    fn polls_hourly_on_the_poll_minute() {
        // Just after 09:14 -> next is 10:14.
        let next = next_poll_instant(ist(9, 20, 0), 9, 14, 14).unwrap();
        assert_eq!((next.hour(), next.minute()), (10, 14));
        // Just after 13:14 -> next is 14:14.
        let next = next_poll_instant(ist(13, 30, 0), 9, 14, 14).unwrap();
        assert_eq!((next.hour(), next.minute()), (14, 14));
    }

    #[test]
    fn last_poll_is_before_close_then_none() {
        // 15:14 is the final poll (poll_minute 14 < close 30).
        let next = next_poll_instant(ist(15, 0, 0), 9, 14, 14).unwrap();
        assert_eq!((next.hour(), next.minute()), (15, 14));
        // After 15:14 there are no more polls today.
        assert!(next_poll_instant(ist(15, 15, 0), 9, 14, 14).is_none());
        assert!(next_poll_instant(ist(16, 0, 0), 9, 14, 14).is_none());
    }

    #[test]
    fn exact_anchor_instant_is_not_repeated() {
        // At exactly 09:14:00 the anchor is not "strictly after", so we move on
        // to 10:14 — preventing a double poll right after firing.
        let next = next_poll_instant(ist(9, 14, 0), 9, 14, 14).unwrap();
        assert_eq!((next.hour(), next.minute()), (10, 14));
    }
}
