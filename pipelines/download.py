"""G空間情報センターから登記所備付地図の zip を取る。

法務省の地図は都道府県ごとの組織（moj-13tokyo など）に、1 市区町村（登記所）1 データセットで
載っている。過年度は moj-13tokyo-2024-2025 のような別の組織に移る。1 データセットに
前年と当年の 2 つの zip があり、ファイル名の末尾が年（13212-0128-2026.zip）。新しい年だけ取る。
ダウンロードにログインは要らない（S3 の署名付き URL へ転送される）。

出力:
- data/moj/zip/<ファイル名>.zip
- data/moj/sources.ndjson（1 行 = 1 zip。出典の表示に使うデータセットのページ URL を持つ）

データソース: 法務省 登記所備付地図データ（G空間情報センター）
https://front.geospatial.jp/
"""

import json
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlencode

from shared.fetch import download, fetch, write_atomic

logger = logging.getLogger("pipelines")

CKAN = "https://www.geospatial.jp/ckan"
USER_AGENT = "dataset-moj (+https://github.com/queria-io/dataset-moj)"
ZIP_DIR = Path("data/moj/zip")
SOURCES = Path("data/moj/sources.ndjson")
# 当年の組織だけを拾う（moj-13tokyo は拾い、moj-13tokyo-2024-2025 は拾わない）
ORGANIZATION = re.compile(r"moj-(\d{2})[a-z]+")
ZIP_NAME = re.compile(r"(\d{5})-(\d{4})-(\d{4})\.zip")
# 取り込む都道府県コード。全国（約 100GB）は大きさと時間を測ってから広げる
PREFECTURES = os.environ.get("MOJ_PREFECTURES", "13").split(",")
PARALLEL_DOWNLOADS = 6


def ckan(action: str, **params) -> dict:
    body = fetch(f"{CKAN}/api/3/action/{action}?{urlencode(params)}", user_agent=USER_AGENT, timeout=120)
    return json.loads(body)["result"]


def organizations() -> list[str]:
    return [
        name
        for name in ckan("organization_list", limit=1000)
        if (m := ORGANIZATION.fullmatch(name)) and m.group(1) in PREFECTURES
    ]


def latest_zip(package: dict) -> dict | None:
    """データセットの zip のうち、いちばん新しい年のもの。"""
    zips = [(m, r) for r in package["resources"] if (m := ZIP_NAME.search(r["url"]))]
    if not zips:
        return None
    match, resource = max(zips, key=lambda z: z[0].group(3))
    return {
        "file": match.group(0),
        "city_code": match.group(1),
        "registry_code": match.group(2),
        "year": int(match.group(3)),
        "title": package["title"],
        "page_url": f"{CKAN}/dataset/{package['name']}",
        "zip_url": resource["url"],
        "size": int(resource["size"]) if resource.get("size") else None,
    }


def sources() -> list[dict]:
    packages = [
        p
        for org in organizations()
        for p in ckan("package_search", fq=f"organization:{org}", rows=1000)["results"]
    ]
    return sorted((s for p in packages if (s := latest_zip(p))), key=lambda s: s["file"])


def fetch_zip(source: dict) -> None:
    path = ZIP_DIR / source["file"]
    if path.exists() and path.stat().st_size == source["size"]:
        return
    download(source["zip_url"], path, user_agent=USER_AGENT, timeout=300)
    logger.info("  %s (%s)", source["file"], source["title"])


def download_moj() -> list[dict]:
    found = sources()
    if not found:
        raise RuntimeError(f"no zip found for prefectures {PREFECTURES}")
    logger.info("  %d zip, %.2f GB", len(found), sum(s["size"] or 0 for s in found) / 1e9)
    ZIP_DIR.mkdir(parents=True, exist_ok=True)
    with ThreadPoolExecutor(PARALLEL_DOWNLOADS) as pool:
        list(pool.map(fetch_zip, found))
    write_atomic(SOURCES, "".join(json.dumps(s, ensure_ascii=False) + "\n" for s in found).encode())
    return found
