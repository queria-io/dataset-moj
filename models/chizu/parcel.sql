{# 登記所備付地図の筆。1 行が 1 筆。pipelines/convert.py が zip ごとに書いた Parquet を束ねる。
   地域で引くので市区町村・大字・丁目・小字の順に並べる。
   原典の形には自己交差するものが 0.3% ほどあるので st_makevalid を通す。
   全国で 2.4 億筆・17.5GB あり、既定の並列のまま R2 へ書くと、Parquet を読むファイルと
   マルチパートアップロードの接続で、開けるファイル数の上限（macOS の既定 256）を使い切る。
   名前解決もできなくなり「Could not resolve hostname」で落ちるので、並列を絞る。 #}

{{ config(pre_hook=[
    "SET threads = 4",
    "SET s3_uploader_thread_limit = 8",
]) }}

select
    city_code,
    {{ local_government_code('city_code') }} as lg_code,
    city_name,
    oaza_code,
    chome_code,
    koaza_code,
    yobi_code,
    oaza,
    chome,
    koaza,
    yobi,
    chiban,
    crs,
    datum,
    accuracy,
    coord_type,
    undetermined_members,
    st_makevalid(st_geomfromwkb(geometry)) as geometry,
    lng,
    lat,
    map_name,
    file,
    fude_id
from read_parquet('data/moj/parcel/*.parquet')
order by city_code, oaza_code, chome_code, koaza_code, yobi_code, chiban, file, fude_id
