select
	a.created_at::date as "Дата",
	count(distinct a.user_id) as dau,
	count(*) as attempt_cnt
from attempts a
where true
	and {{date}}
group by "Дата"
order by dau;