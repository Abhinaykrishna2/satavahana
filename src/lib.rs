/// Open a CSV reader, transparently decompressing `.gz` files so backtests can read the
/// gzipped tick archives directly (no manual gunzip). Plain `.csv` paths are unaffected.
pub fn open_csv(path: &std::path::Path) -> std::io::Result<csv::Reader<Box<dyn std::io::Read>>> {
    let file = std::fs::File::open(path)?;
    let reader: Box<dyn std::io::Read> = if path.extension().map_or(false, |e| e == "gz") {
        Box::new(flate2::read::GzDecoder::new(file))
    } else {
        Box::new(file)
    };
    Ok(csv::Reader::from_reader(reader))
}

pub mod backtest;
pub mod capital;
pub mod cli;
pub mod config;
pub mod costs;
pub mod execution;
pub mod greeks;
pub mod ledger;
pub mod microbook;
pub mod models;
pub mod multileg;
pub mod multileg_replay;
pub mod oms;
pub mod portfolio;
pub mod options_engine;
pub mod quant_engine;
pub mod risk;
pub mod store;
pub mod strategy;
pub mod technicals;
pub mod websocket;
