import os
from datetime import datetime

import ccxt
import pandas as pd
import pandas_ta as ta


SYMBOLS = ["BTC/USDT", "ETH/USDT"]
TIMEFRAME = "4h"
LIMIT = 500
OUTPUT_PATH = "/root/my_data/market_report.txt"
SMA_LENGTHS = [5, 13, 55, 144, 233]


def fetch_ohlcv(exchange: ccxt.Exchange, symbol: str) -> pd.DataFrame:
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=LIMIT)
    df = pd.DataFrame(
        ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"]
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    return df


def add_sma_indicators(df: pd.DataFrame) -> pd.DataFrame:
    for length in SMA_LENGTHS:
        df[f"SMA_{length}"] = ta.sma(df["close"], length=length)
    return df


def analyze_symbol(df: pd.DataFrame, symbol: str) -> str:
    latest = df.iloc[-1]
    price = latest["close"]
    sma144 = latest["SMA_144"]
    sma233 = latest["SMA_233"]
    sma13 = latest["SMA_13"]
    sma55 = latest["SMA_55"]

    if price > sma144 and price > sma233:
        trend = "牛市结构（价格位于 SMA 144/233 上方）"
    elif price < sma144 and price < sma233:
        trend = "熊市结构（价格位于 SMA 144/233 下方）"
    else:
        trend = "震荡结构（价格在 SMA 144/233 附近徘徊）"

    if sma13 > sma55:
        short_signal = "金叉（SMA 13 上穿 SMA 55）"
    elif sma13 < sma55:
        short_signal = "死叉（SMA 13 下穿 SMA 55）"
    else:
        short_signal = "粘合（SMA 13 与 SMA 55 基本重合）"

    return (
        f"{symbol}：\n"
        f"- 最新收盘价：{price:.2f}\n"
        f"- 大趋势：{trend}\n"
        f"- 短期信号：{short_signal}\n"
    )


def build_report() -> str:
    exchange = ccxt.binance({"enableRateLimit": True})
    sections = []

    for symbol in SYMBOLS:
        df = fetch_ohlcv(exchange, symbol)
        df = add_sma_indicators(df)
        sections.append(analyze_symbol(df, symbol))

    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    header = f"加密市场 4H 均线扫描报告（{timestamp}）\n"
    return header + "\n".join(sections)


def write_report(report: str, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as file:
        file.write(report)


if __name__ == "__main__":
    report_content = build_report()
    write_report(report_content, OUTPUT_PATH)
