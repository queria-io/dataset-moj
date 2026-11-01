# dataset-moj

法務省の登記所備付地図データ（地図 XML）を Queria のカタログ（[data.queria.io](https://data.queria.io/)）へ取り込むデータセットです。

## データ出典

法務省が [G空間情報センター](https://front.geospatial.jp/) で公開している「登記所備付地図データ」（[法務省の案内](https://www.moj.go.jp/MINJI/minji05_00494.html)）。市区町村（登記所）ごとの zip に、図面ごとの地図 XML が入っています。

使うときは、登記所備付地図データ利用規約にしたがって出典（「登記所備付地図データ○○市」（法務省）とデータセットのページ URL）と、加工したものであることを書いてください。市区町村ごとの名前と URL は `chizu.source` にあります。他の情報と照合して特定の個人を識別する使い方は規約で禁止されています。

## 収録テーブル

| テーブル | 内容 |
| --- | --- |
| `chizu.parcel` | 1 行が 1 筆。市区町村・大字・丁目・小字・地番、座標系、精度区分。公共座標の筆だけ形と代表点 |
| `chizu.source` | 取り込んだ zip。出典に書くデータセット名とページ URL |

- 全国を収録しています
- 任意座標系の図面の筆は形を持ちません。東京都では 9 割の筆が任意座標系です
- 筆を示すキーは `(file, fude_id)` です。地番は図面をまたぐと重なることがあります

```sql
-- 日野市の筆を座標系ごとに数える
SELECT crs, count(*) AS parcels, count(geometry) AS with_shape
FROM moj.chizu.parcel
WHERE city_code = '13212'
GROUP BY crs;
```

## 変換のしかた

地図 XML は [mojxml](https://github.com/MIERUNE/mojxml-py) で読み、公共座標を経緯度（JGD2011）に直します。筆界未定地のもとの筆（筆界未定構成筆）は mojxml が読まないので、lxml で読み直してリストで持ちます。自己交差する形は `ST_MakeValid` で直します。

各データセットには前年と当年の zip があり、新しい年のものだけを取ります。全国で zip が約 100GB あるので、取ったそばから Parquet にして zip は消します。Parquet が既にある zip は取り直しません。都道府県を絞るときは環境変数 `MOJ_PREFECTURES`（都道府県コードのカンマ区切り）を使います。

## ビルド

```bash
git submodule update --init
uv sync
scripts/build.sh                        # queria sync -- python main.py
MOJ_PREFECTURES=13,14 scripts/build.sh  # 東京都と神奈川県だけ
```

原典は年 1 回（春）更新されます。Sync は手動実行だけです。
