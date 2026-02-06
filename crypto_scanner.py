import os
from datetime import datetime

import ccxt
import pandas as pd


SYMBOLS = ["BTC/USDT", "ETH/USDT"]
MACRO_TIMEFRAME = "4h"
MACRO_LIMIT = 500
MICRO_TIMEFRAME = "1m"
MICRO_LIMIT = 1500
MICRO_SCAN_MINUTES = range(15, 91)
OUTPUT_PATH = "/root/my_data/market_report.txt"


def fetch_ohlcv(exchange: ccxt.Exchange, symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
    raw = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(raw, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    return df.sort_values("timestamp").set_index("timestamp")


def add_sma(df: pd.DataFrame, length: int, source: str = "close") -> pd.Series:
    return df[source].rolling(window=length, min_periods=length).mean()


def macro_trend_label(price: float, sma144: float, sma233: float) -> tuple[str, str]:
    if price < sma144 and price < sma233:
        return "bear", "🔴 熊市 (SMA 144/233 压制中)"
    if price > sma144 and price > sma233:
        return "bull", "🟢 牛市 (SMA 144/233 支撑中)"
    return "range", "🟡 震荡 (SMA 144/233 缠绕区)"


def analyze_macro(df_4h: pd.DataFrame) -> dict:
    result = df_4h.copy()
    result["SMA_55"] = add_sma(result, 55)
    result["SMA_144"] = add_sma(result, 144)
    result["SMA_233"] = add_sma(result, 233)

    valid = result.dropna(subset=["SMA_144", "SMA_233"])
    if valid.empty:
        return {
            "trend_key": "range",
            "trend_text": "⚠️ 数据不足 (4H K线不足以计算 SMA 144/233)",
            "latest": None,
        }

    latest = valid.iloc[-1]
    trend_key, trend_text = macro_trend_label(latest["close"], latest["SMA_144"], latest["SMA_233"])
    return {"trend_key": trend_key, "trend_text": trend_text, "latest": latest}


def resample_ohlcv(df_1m: pd.DataFrame, minutes: int) -> pd.DataFrame:
    rule = f"{minutes}min"
    sampled = (
        df_1m.resample(rule)
        .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
        .dropna(subset=["open", "high", "low", "close"])
    )
    return sampled


def find_best_kiss(df_1m: pd.DataFrame, trend_key: str) -> dict | None:
    best = None

    for minutes in MICRO_SCAN_MINUTES:
        sampled = resample_ohlcv(df_1m, minutes)
        if len(sampled) < 13:
            continue

        sampled["SMA_5"] = add_sma(sampled, 5)
        sampled["SMA_13"] = add_sma(sampled, 13)

        valid = sampled.dropna(subset=["SMA_5", "SMA_13"])
        if valid.empty:
            continue

        latest = valid.iloc[-1]

        if trend_key == "bear" and not (latest["SMA_5"] < latest["SMA_13"]):
            continue
        if trend_key == "bull" and not (latest["SMA_5"] > latest["SMA_13"]):
            continue

        gap = abs(latest["SMA_5"] - latest["SMA_13"])
        candidate = {
            "minutes": minutes,
            "gap": gap,
            "sma5": latest["SMA_5"],
            "sma13": latest["SMA_13"],
        }

        if best is None or candidate["gap"] < best["gap"]:
            best = candidate

    return best


def format_kiss_status(best_kiss: dict, trend_key: str) -> tuple[str, str]:
    gap = best_kiss["gap"]

    if gap <= 2:
        kiss_text = f"🟢 完美相切 (Gap={gap:.4f}, 拒绝死叉)"
    elif gap <= 8:
        kiss_text = f"🟡 临近相切 (Gap={gap:.4f}, 趋势保持)"
    else:
        kiss_text = f"🟠 偏离相切 (Gap={gap:.4f}, 需继续等待)"

    if trend_key == "bull":
        suggestion = "这是第3/5段的潜在做多切入位置。"
    elif trend_key == "bear":
        suggestion = "这是第3/5段的潜在做空切入位置。"
    else:
        suggestion = "宏观震荡期，建议降低仓位并等待进一步确认。"

    return kiss_text, suggestion


def analyze_symbol(exchange: ccxt.Exchange, symbol: str) -> str:
    macro_df = fetch_ohlcv(exchange, symbol, MACRO_TIMEFRAME, MACRO_LIMIT)
    macro = analyze_macro(macro_df)

    micro_df = fetch_ohlcv(exchange, symbol, MICRO_TIMEFRAME, MICRO_LIMIT)
    best_kiss = find_best_kiss(micro_df, macro["trend_key"])

    lines = [
        f"币种: {symbol}",
        f"🌊 宏观趋势 (4H): {macro['trend_text']}",
        "-----------------------------------------",
        "🔬 微观相切扫描 (15m - 90m):",
    ]

    if best_kiss is None:
        lines.extend(
            [
                "   🏆 最佳相切点: 无有效周期",
                "   📏 状态: ⚠️ 由于周期过大或趋势过滤，无可用 SMA5/SMA13 相切结果",
                "   💡 建议: 等待更多 1m 数据或趋势重新排列后再扫描。",
            ]
        )
    else:
        status, suggestion = format_kiss_status(best_kiss, macro["trend_key"])
        lines.extend(
            [
                f"   🏆 最佳相切点: [{best_kiss['minutes']}分钟] 级别",
                f"   📏 状态: {status}",
                f"   💡 建议: {suggestion}",
            ]
        )

    lines.append("-----------------------------------------")
    return "\n".join(lines)


def build_report() -> str:
    exchange = ccxt.binance({"enableRateLimit": True})
    sections = [analyze_symbol(exchange, symbol) for symbol in SYMBOLS]
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    header = f"=== 🛡️ 林总均线系统扫描 ({ts} UTC时间) ==="
    return f"{header}\n" + "\n".join(sections) + "\n"


def write_report(path: str, content: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


if __name__ == "__main__":
    report = build_report()
    write_report(OUTPUT_PATH, report)
