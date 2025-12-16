select
	a.created_at::date as dt,
	count(*) as attempt_cnt
from attempts a
where true
	and {{date}}
group by dt 
order by dt;