select
	a.created_at::date as dt,
	count(distinct a.user_id) as dau
from attempts a
where true
	and {{data}}
group by dt 
order by dt;