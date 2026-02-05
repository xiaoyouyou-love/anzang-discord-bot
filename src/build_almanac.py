import argparse
import json
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import requests
from bs4 import BeautifulSoup

USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"


@dataclass
class AlmanacSource:
    name: str
    url_template: str


SOURCES = {
    "huangli123": AlmanacSource(
        name="huangli123",
        url_template="https://www.huangli123.net/huangli/{year}-{month}-{day}.html",
    ),
}


class FetchError(RuntimeError):
    pass


def _normalize_lines(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    lines = []
    for raw in soup.get_text("\n").splitlines():
        cleaned = re.sub(r"\s+", " ", raw).strip()
        if cleaned:
            lines.append(cleaned)
    return lines


def _find_line_value(lines: List[str], label: str) -> Optional[str]:
    label_colon = f"{label}："
    label_ascii_colon = f"{label}:"
    for idx, line in enumerate(lines):
        if line == label and idx + 1 < len(lines):
            return lines[idx + 1].strip()
        if line.startswith(label_colon):
            return line[len(label_colon) :].strip()
        if line.startswith(label_ascii_colon):
            return line[len(label_ascii_colon) :].strip()
        if line.startswith(label):
            remainder = line[len(label) :].lstrip(" ：:")
            if remainder:
                return remainder
        if label_colon in line:
            return line.split(label_colon, 1)[1].strip()
        if label_ascii_colon in line:
            return line.split(label_ascii_colon, 1)[1].strip()
    return None


def _split_items(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    parts = [p.strip() for p in re.split(r"[\s、]+", raw) if p.strip()]
    return parts


def _extract_kongwang(text: str) -> Dict[str, Optional[str]]:
    result = {"year": None, "month": None, "day": None}
    for key, label in [("year", "年空亡"), ("month", "月空亡"), ("day", "日空亡")]:
        match = re.search(rf"{label}[:： ]*([^\s]+)", text)
        if match:
            result[key] = match.group(1).strip()
    return result


def _extract_sansha(text: str, label: str) -> Dict[str, Optional[str]]:
    result = {"year": None, "month": None, "day": None}
    for key, prefix in [("year", "年"), ("month", "月"), ("day", "日")]:
        match = re.search(rf"{prefix}{label}[:： ]*([^\s]+)", text)
        if match:
            result[key] = match.group(1).strip()
    return result


def _extract_chong(raw: Optional[str]) -> Dict[str, Optional[str]]:
    if not raw:
        return {"text": None, "animal": None, "branch": None}
    animal = None
    branch = None
    match = re.search(r"冲([^\(煞]+)", raw)
    if match:
        animal = match.group(1).strip()
    branch_match = re.search(r"\(([^\)]+)\)", raw)
    if branch_match:
        branch = branch_match.group(1).strip()
    return {"text": raw, "animal": animal, "branch": branch}


def _extract_xingxiu(raw: Optional[str]) -> Dict[str, Optional[str]]:
    if not raw:
        return {"name": None, "jixiong": None}
    match = re.search(r"([^\(]+)\(([^\)]+)\)", raw)
    if match:
        return {"name": match.group(1).strip(), "jixiong": match.group(2).strip()}
    return {"name": raw.strip(), "jixiong": None}


def parse_almanac_html(html: str, target_date: date, url: str) -> Dict[str, object]:
    lines = _normalize_lines(html)
    text_blob = " ".join(lines)

    yi_raw = _find_line_value(lines, "宜")
    ji_raw = _find_line_value(lines, "忌")

    zhishen_raw = _find_line_value(lines, "值神")
    shier_shen = _find_line_value(lines, "十二神")

    xingxiu_raw = _find_line_value(lines, "星宿")
    chong_raw = _find_line_value(lines, "冲煞") or _find_line_value(lines, "冲")

    kongwang = _extract_kongwang(text_blob)
    sansha = _extract_sansha(text_blob, "三煞")
    qisha = _extract_sansha(text_blob, "七煞")

    jishen_raw = (
        _find_line_value(lines, "吉神宜趋")
        or _find_line_value(lines, "吉神")
        or _find_line_value(lines, "吉神宜趋：")
    )
    xiongsha_raw = _find_line_value(lines, "凶煞宜忌") or _find_line_value(lines, "凶煞")

    zhishen = None
    huangdao = False
    if zhishen_raw:
        huangdao = "黄道" in zhishen_raw
        zhishen = re.sub(r"\(.*?\)", "", zhishen_raw).strip()

    xingxiu_info = _extract_xingxiu(xingxiu_raw)
    chong_info = _extract_chong(chong_raw)

    data = {
        "date": target_date.strftime("%Y-%m-%d"),
        "yi": _split_items(yi_raw),
        "ji": _split_items(ji_raw),
        "zhishen": zhishen,
        "huangdao": huangdao,
        "shier_shen": shier_shen,
        "xingxiu": xingxiu_info["name"],
        "xingxiu_jixiong": xingxiu_info["jixiong"],
        "chong": chong_info,
        "kongwang": kongwang,
        "jishen": _split_items(jishen_raw),
        "xiongsha": _split_items(xiongsha_raw),
        "sansha": sansha,
        "qisha": qisha,
        "url": url,
    }
    return data


def _date_range(start: date, end: date) -> Iterable[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _get_cache_path(cache_dir: Path, target_date: date) -> Path:
    return cache_dir / f"{target_date.year}-{target_date.month}-{target_date.day}.html"


def fetch_html(url: str, cache_path: Path, session: requests.Session) -> str:
    if cache_path.exists():
        return cache_path.read_text(encoding="utf-8")

    last_error: Optional[Exception] = None
    for attempt in range(3):
        try:
            response = session.get(url, timeout=15)
            if response.status_code == 200:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                cache_path.write_text(response.text, encoding="utf-8")
                return response.text
            last_error = FetchError(f"Unexpected status code {response.status_code} for {url}")
        except Exception as exc:  # noqa: BLE001 - network failures should retry
            last_error = exc
        if attempt < 2:
            time.sleep(1.0)
    raise FetchError(str(last_error))


def fetch_almanac_for_date(
    target_date: date,
    source: AlmanacSource,
    cache_dir: Path,
    session: requests.Session,
) -> Dict[str, object]:
    url = source.url_template.format(
        year=target_date.year, month=target_date.month, day=target_date.day
    )
    cache_path = _get_cache_path(cache_dir, target_date)
    html = fetch_html(url, cache_path, session)
    return parse_almanac_html(html, target_date, url)


def build_almanac(
    start: date,
    end: date,
    output_path: Path,
    source_name: str,
    cache_dir: Optional[Path] = None,
) -> None:
    if source_name not in SOURCES:
        raise ValueError(f"Unsupported source: {source_name}")
    source = SOURCES[source_name]
    if cache_dir is None:
        cache_dir = Path("data") / "cache" / source_name

    output_path.parent.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    with output_path.open("w", encoding="utf-8") as outfile:
        for day in _date_range(start, end):
            record = fetch_almanac_for_date(day, source, cache_dir, session)
            outfile.write(json.dumps(record, ensure_ascii=False) + "\n")
            time.sleep(0.7)


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build almanac JSONL dataset.")
    parser.add_argument("--start", required=True, type=parse_date)
    parser.add_argument("--end", required=True, type=parse_date)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--source", default="huangli123")
    args = parser.parse_args()

    build_almanac(args.start, args.end, args.out, args.source)


if __name__ == "__main__":
    main()
