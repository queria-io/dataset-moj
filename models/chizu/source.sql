{# 取り込んだ zip。1 行が 1 zip（市区町村の 1 登記所）。出典の表示に使うページ URL を持つ。
   pipelines/download.py が data/moj/sources.ndjson に書く。 #}

select
    file,
    city_code,
    {{ local_government_code('city_code') }} as lg_code,
    registry_code,
    year,
    title,
    page_url,
    zip_url,
    size
from read_json(
    'data/moj/sources.ndjson',
    format = 'newline_delimited',
    columns = {
        file: 'VARCHAR',
        city_code: 'VARCHAR',
        registry_code: 'VARCHAR',
        year: 'INTEGER',
        title: 'VARCHAR',
        page_url: 'VARCHAR',
        zip_url: 'VARCHAR',
        size: 'BIGINT'
    }
)
order by file
