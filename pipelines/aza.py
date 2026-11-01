"""筆の形を大字・丁目・小字ごとに合わせ、市区町村ごとの Parquet にする。

全国の筆を 1 つの集約で合わせるとメモリに乗らないので、市区町村ごとに合わせる。
形の無い筆（任意座標系）と、図面の外の土地を指す筆（地番に「地区外」「別図」）は使わない。
コードは登記所ごとに振られていることがあるので、コードと名前の組でまとめる。

出力: data/moj/aza/<市区町村コード>.parquet
"""

import logging
from multiprocessing import Pool
from pathlib import Path

import duckdb

from shared.fetch import replacing

logger = logging.getLogger("pipelines")

AZA_DIR = Path("data/moj/aza")

SQL = """
COPY (
    SELECT
        city_code, oaza_code, chome_code, koaza_code, oaza, chome, koaza,
        count(*) AS parcels,
        ST_AsWKB(ST_CollectionExtract(ST_MakeValid(ST_Union_Agg(ST_MakeValid(ST_GeomFromWKB(geometry)))), 3))
            AS geometry
    FROM read_parquet({files})
    WHERE geometry IS NOT NULL
        AND oaza IS NOT NULL
        AND chiban NOT LIKE '%地区外%'
        AND chiban NOT LIKE '%別図%'
    GROUP BY ALL
    ORDER BY ALL
) TO '{output}' (FORMAT parquet, COMPRESSION zstd)
"""


def union_city(job: tuple[str, list[Path]]) -> str:
    city_code, files = job
    output = AZA_DIR / f"{city_code}.parquet"
    with replacing(output) as partial, duckdb.connect() as con:
        con.execute("INSTALL spatial; LOAD spatial; SET threads = 1")
        con.execute(SQL.format(files=[str(f) for f in files], output=partial))
    return city_code


def stale(output: Path, files: list[Path]) -> bool:
    return not output.exists() or output.stat().st_mtime < max(f.stat().st_mtime for f in files)


def union_all(parcel_dir: Path) -> None:
    """筆の Parquet より古いか無い市区町村だけ作り直す。筆の無くなった市区町村は消す。"""
    by_city: dict[str, list[Path]] = {}
    for f in sorted(parcel_dir.glob("*.parquet")):
        by_city.setdefault(f.name[:5], []).append(f)
    AZA_DIR.mkdir(parents=True, exist_ok=True)
    for gone in {p for p in AZA_DIR.glob("*.parquet") if p.stem not in by_city}:
        gone.unlink()
    todo = [(c, fs) for c, fs in by_city.items() if stale(AZA_DIR / f"{c}.parquet", fs)]
    logger.info("  合わせるのは %d 市区町村", len(todo))
    with Pool() as pool:
        for done, city in enumerate(pool.imap_unordered(union_city, todo), 1):
            logger.info("  [%d/%d] %s", done, len(todo), city)
