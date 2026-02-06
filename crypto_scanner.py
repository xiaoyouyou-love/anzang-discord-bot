import os
from datetime import datetime

import ccxt
import pandas as pd


SYMBOLS = ["BTC/USDT", "ETH/USDT"]
MACRO_TIMEFRAME = "4h"
MACRO_LIMIT = 500
MICRO_TIMEFRAME = "1m"
MICRO_LIMIT = 2000
OUTPUT_PATH = "/root/my_data/market_report.txt"
MACRO_SMA_LENGTHS = [55, 144, 233]


def fetch_ohlcv(
    exchange: ccxt.Exchange, symbol: str, timeframe: str, limit: int
) -> pd.DataFrame:
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(
        ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"]
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    df = df.sort_values("timestamp").set_index("timestamp")
    return df


def add_sma(df: pd.DataFrame, lengths: list[int]) -> pd.DataFrame:
    result = df.copy()
    for length in lengths:
        result[f"SMA_{length}"] = result["close"].rolling(window=length).mean()
    return result


def analyze_macro_trend(macro_df: pd.DataFrame) -> tuple[str, str]:
    macro_df = add_sma(macro_df, MACRO_SMA_LENGTHS)
    latest = macro_df.iloc[-1]
    price = latest["close"]
    sma144 = latest["SMA_144"]
    sma233 = latest["SMA_233"]

    if price > sma144 and price > sma233:
        return "bull", "🌊 大趋势(4H): 🐂 牛市 (价格在 SMA 144/233 上方)"
    if price < sma144 and price < sma233:
        return "bear", "🌊 大趋势(4H): 🐻 熊市 (价格在 SMA 144/233 下方)"
    return "range", "🌊 大趋势(4H): ⚖️ 震荡 (价格在 SMA 144/233 附近)"


def resample_ohlcv(micro_df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    rule = f"{minutes}min"
    sampled = (
        micro_df.resample(rule)
        .agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            }
        )
        .dropna(subset=["open", "high", "low", "close"])
    )
    return sampled


def find_best_fit_timeframe(micro_df: pd.DataFrame) -> dict | None:
    best: dict | None = None

    for minutes in range(1, 61):
        sampled = resample_ohlcv(micro_df, minutes)
        sampled["SMA_5"] = sampled["close"].rolling(window=5).mean()
        sampled["SMA_13"] = sampled["close"].rolling(window=13).mean()

        valid = sampled.dropna(subset=["SMA_5", "SMA_13"])
        if len(valid) < 2:
            continue

        latest = valid.iloc[-1]
        prev = valid.iloc[-2]
        gap = abs(latest["SMA_5"] - latest["SMA_13"])

        candidate = {
            "minutes": minutes,
            "gap": gap,
            "latest_sma5": latest["SMA_5"],
            "latest_sma13": latest["SMA_13"],
            "prev_sma5": prev["SMA_5"],
            "prev_sma13": prev["SMA_13"],
        }

        if best is None or candidate["gap"] < best["gap"]:
            best = candidate

    return best


def describe_kiss(best_fit: dict, trend_key: str) -> tuple[str, str]:
    sma5 = best_fit["latest_sma5"]
    sma13 = best_fit["latest_sma13"]
    prev_gap = abs(best_fit["prev_sma5"] - best_fit["prev_sma13"])
    gap = best_fit["gap"]

    approaching = gap <= prev_gap
    if approaching:
        emoji = "🟢"
        vibe = "欲拒还迎"
    else:
        emoji = "🟡"
        vibe = "轻微背离"

    if sma5 >= sma13:
        relation = "SMA5回踩SMA13未破"
    else:
        relation = "SMA5反抽SMA13未破"

    if trend_key == "bull":
        suggestion = "这是一个完美的第3/5段做多切入点！"
    elif trend_key == "bear":
        suggestion = "这是一个完美的第3/5段做空切入点！"
    else:
        suggestion = "趋势未明，建议轻仓等待确认后再介入。"

    status = f"{emoji} {vibe} (Gap={gap:.4f}, {relation})"
    return status, suggestion


def analyze_symbol(exchange: ccxt.Exchange, symbol: str) -> str:
    macro_df = fetch_ohlcv(exchange, symbol, timeframe=MACRO_TIMEFRAME, limit=MACRO_LIMIT)
    trend_key, trend_line = analyze_macro_trend(macro_df)

    micro_df = fetch_ohlcv(exchange, symbol, timeframe=MICRO_TIMEFRAME, limit=MICRO_LIMIT)
    best_fit = find_best_fit_timeframe(micro_df)

    if best_fit is None:
        micro_block = (
            "🎯 最佳相切点: 数据不足\n"
            "   - 状态: ⚠️ 无法计算（部分周期下 SMA13 数据不足）\n"
            "   - 建议: 等待更多 1m K 线后重试。"
        )
    else:
        status, suggestion = describe_kiss(best_fit, trend_key)
        micro_block = (
            f"🎯 最佳相切点: 在 [{best_fit['minutes']}分钟] 级别\n"
            f"   - 状态: {status}\n"
            f"   - 建议: {suggestion}"
        )

    return f"[{symbol}]\n{trend_line}\n{micro_block}\n"


def build_report() -> str:
    exchange = ccxt.binance({"enableRateLimit": True})
    sections = [analyze_symbol(exchange, symbol) for symbol in SYMBOLS]
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    header = f"加密市场宏观+微观扫描报告（{timestamp}）\n"
    return header + "\n".join(sections)


def write_report(report: str, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as file:
        file.write(report)


if __name__ == "__main__":
    report_content = build_report()
    write_report(report_content, OUTPUT_PATH)
