"""法務省 登記所備付地図データパイプライン。

1. download: G空間情報センターから市区町村ごとの zip を取る
2. convert: 地図 XML を 1 筆 1 行の Parquet にする
3. dbt: dbt ビルド
"""

import logging
from multiprocessing import Pool
from pathlib import Path

from dbt.cli.main import dbtRunner

from pipelines.convert import convert
from pipelines.download import ZIP_DIR, download_moj

# dbt を読み込むとルートロガーが先に設定されるので、force で上書きする
logging.basicConfig(level=logging.INFO, format="%(message)s", force=True)
logger = logging.getLogger("pipelines")

PARCEL_DIR = Path("data/moj/parcel")


def parquet_path(file: str) -> Path:
    return PARCEL_DIR / f"{Path(file).stem}.parquet"


def convert_all(sources: list[dict]) -> None:
    """zip より新しい Parquet があれば飛ばす。今回の zip に無い Parquet は消す。"""
    wanted = {parquet_path(s["file"]) for s in sources}
    for stale in set(PARCEL_DIR.glob("*.parquet")) - wanted:
        stale.unlink()
    with Pool() as pool:
        for source in sources:
            zip_path, output = ZIP_DIR / source["file"], parquet_path(source["file"])
            if output.exists() and output.stat().st_mtime >= zip_path.stat().st_mtime:
                continue
            logger.info("  %s: %d 筆", source["file"], convert(zip_path, output, pool))


def dbt_build():
    dbt = dbtRunner()
    for command in (["deps"], ["build"], ["docs", "generate"]):
        result = dbt.invoke(command)
        if not result.success:
            raise SystemExit(f"dbt {' '.join(command)} failed")


def main():
    logger.info("1/3: download (登記所備付地図の zip)")
    sources = download_moj()

    logger.info("2/3: convert (地図 XML → Parquet)")
    convert_all(sources)

    logger.info("3/3: dbt build")
    dbt_build()


if __name__ == "__main__":
    main()
