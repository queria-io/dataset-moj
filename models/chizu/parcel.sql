{# 登記所備付地図の筆。1 行が 1 筆。pipelines/convert.py が zip ごとに書いた Parquet を束ねる。
   地域で引くので市区町村・大字・丁目・小字の順に並べる。
   原典の形には自己交差するものが 0.3% ほどあるので st_makevalid を通す。 #}

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
