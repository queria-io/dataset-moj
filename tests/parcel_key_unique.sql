-- 筆のキー (file, fude_id) が一意であること
select file, fude_id, count(*) as n
from {{ ref('parcel') }}
group by file, fude_id
having count(*) > 1
