"""法務省 登記所備付地図データパイプライン。

1. list: G空間情報センターから取り込む zip の一覧を取る
2. download / convert: zip を取って 1 筆 1 行の Parquet にし、zip は消す
3. dbt: dbt ビルド

全国の zip は約 100GB あるので、取ったそばから変換して消す。Parquet が既にある zip は
取らない（ファイル名に年が入るので、原典が更新されると新しい名前になる）。
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import Pool
from pathlib import Path

from dbt.cli.main import dbtRunner

from pipelines.convert import convert
from pipelines.download import fetch_zip, list_moj

# dbt を読み込むとルートロガーが先に設定されるので、force で上書きする
logging.basicConfig(level=logging.INFO, format="%(message)s", force=True)
logger = logging.getLogger("pipelines")

PARCEL_DIR = Path("data/moj/parcel")
PARALLEL_DOWNLOADS = 6


def parquet_path(file: str) -> Path:
    return PARCEL_DIR / f"{Path(file).stem}.parquet"


def convert_all(sources: list[dict]) -> None:
    """Parquet の無い zip を取って変換する。今回の一覧に無い Parquet は消す。"""
    wanted = {parquet_path(s["file"]) for s in sources}
    for stale in set(PARCEL_DIR.glob("*.parquet")) - wanted:
        stale.unlink()
    todo = [s for s in sources if not parquet_path(s["file"]).exists()]
    logger.info("  変換するのは %d 本", len(todo))
    with Pool() as pool, ThreadPoolExecutor(PARALLEL_DOWNLOADS) as downloads:
        futures = {downloads.submit(fetch_zip, s): s for s in todo}
        for done, future in enumerate(as_completed(futures), 1):
            source, zip_path = futures[future], future.result()
            rows = convert(zip_path, parquet_path(source["file"]), pool)
            zip_path.unlink()
            logger.info("  [%d/%d] %s %s: %d 筆", done, len(todo), source["file"], source["title"], rows)


def dbt_build():
    dbt = dbtRunner()
    for command in (["deps"], ["build"], ["docs", "generate"]):
        result = dbt.invoke(command)
        if not result.success:
            raise SystemExit(f"dbt {' '.join(command)} failed")


def main():
    logger.info("1/3: list (登記所備付地図の zip)")
    sources = list_moj()

    logger.info("2/3: download / convert (地図 XML → Parquet)")
    convert_all(sources)

    logger.info("3/3: dbt build")
    dbt_build()


if __name__ == "__main__":
    main()
