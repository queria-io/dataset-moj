"""地図 XML の zip を 1 筆 1 行の Parquet にする。

市区町村ごとの zip の中に地図ごとの zip（または xml）が入っている。search-list.csv は
読まない。XML は mojxml で読み、公共座標の筆だけ形を経緯度（JGD2011）に直して持つ。
任意座標の筆は図面の上の座標しか無く、地図に重ねられないので形を持たない。
地区外・別図の筆も残す（地番にそう書いてある）。
筆界未定地の構成筆は mojxml が読まないので、lxml で読み直してリストで持つ。

出力: data/moj/parcel/<市区町村コード>.parquet
"""

import io
import zipfile
from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
from lxml import etree
import pyarrow.parquet as pq
from mojxml.parse import ParseOptions, parse_raw
from shapely.geometry import shape

OPTIONS = ParseOptions(include_arbitrary_crs=True, include_chikugai=True)
ARBITRARY_CRS = "任意座標系"

# 原典の項目名 → 列名
FIELDS = {
    "筆ID": "fude_id",
    "地図名": "map_name",
    "市区町村コード": "city_code",
    "市区町村名": "city_name",
    "大字コード": "oaza_code",
    "丁目コード": "chome_code",
    "小字コード": "koaza_code",
    "予備コード": "yobi_code",
    "大字名": "oaza",
    "丁目名": "chome",
    "小字名": "koaza",
    "予備名": "yobi",
    "地番": "chiban",
    "座標系": "crs",
    "測地系判別": "datum",
    "精度区分": "accuracy",
    "座標値種別": "coord_type",
}

# 構成筆の項目名 → フィールド名
MEMBER_FIELDS = {
    "大字コード": "oaza_code",
    "丁目コード": "chome_code",
    "小字コード": "koaza_code",
    "予備コード": "yobi_code",
    "大字名": "oaza",
    "丁目名": "chome",
    "小字名": "koaza",
    "予備名": "yobi",
    "地番": "chiban",
}
NS = "{http://www.moj.go.jp/MINJI/tizuxml}"
MEMBER = pa.struct([(name, pa.string()) for name in MEMBER_FIELDS.values()])

SCHEMA = pa.schema(
    [("file", pa.string())]
    + [(name, pa.string()) for name in FIELDS.values()]
    + [
        ("undetermined_members", pa.list_(MEMBER)),
        ("geometry", pa.binary()),
        ("lng", pa.float64()),
        ("lat", pa.float64()),
    ]
)


def xml_members(data: bytes, name: str) -> Iterator[tuple[str, bytes]]:
    """zip を入れ子まで開いて (ファイル名, XML) を返す。"""
    if name.endswith(".xml"):
        yield name, data
        return
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        for member in z.namelist():
            if member.endswith((".zip", ".xml")):
                yield from xml_members(z.read(member), member)


def undetermined_members(content: bytes) -> dict[str, list[dict]]:
    """筆 ID → 筆界未定の構成筆。構成筆を持たない筆は入らない。"""
    root = etree.fromstring(content)
    return {
        fude.get("id"): members
        for fude in root.iterfind(f"{NS}主題属性/{NS}筆")
        if (
            members := [
                {column: member.findtext(f"{NS}{key}") for key, column in MEMBER_FIELDS.items()}
                for member in fude.iterfind(f"{NS}筆界未定構成筆")
            ]
        )
    }


def to_row(file: str, feature: dict, members: dict[str, list[dict]]) -> dict:
    props = feature["properties"]
    public = props["座標系"] != ARBITRARY_CRS and feature["geometry"] is not None
    geom = shape(feature["geometry"]) if public else None
    point = geom.representative_point() if geom is not None else None
    return {
        "file": file,
        **{column: props.get(key) for key, column in FIELDS.items()},
        "undetermined_members": members.get(props["筆ID"]),
        "geometry": geom.wkb if geom is not None else None,
        "lng": point.x if point is not None else None,
        "lat": point.y if point is not None else None,
    }


def parse_member(member: tuple[str, bytes]) -> list[dict]:
    file, content = member
    members = undetermined_members(content)
    return [to_row(file, f, members) for f in parse_raw(content, OPTIONS)]


def convert(zip_path: Path, output: Path) -> int:
    """市区町村の zip を Parquet にして行数を返す。1 プロセスで読む（並列は zip 単位で呼び出し側がする）。"""
    members = xml_members(zip_path.read_bytes(), zip_path.name)
    rows = [row for member in members for row in parse_member(member)]
    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    output.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, output, compression="zstd")
    return table.num_rows
