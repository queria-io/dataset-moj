{# 大字・丁目・小字ごとの形。1 行が 1 つの大字・丁目・小字。pipelines/aza.py が市区町村ごとに
   筆の形を合わせて書いた Parquet を束ねる。 #}

select
    city_code,
    {{ local_government_code('city_code') }} as lg_code,
    oaza_code,
    chome_code,
    koaza_code,
    oaza,
    chome,
    koaza,
    parcels,
    st_geomfromwkb(geometry) as geometry
from read_parquet('data/moj/aza/*.parquet')
order by city_code, oaza_code, chome_code, koaza_code, oaza, chome, koaza
