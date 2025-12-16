select
	a.attempt_type as "Тип попытки",
	count(*) as attempt_type_cnt
from attempts a
where true
	and {{date}}
group by a.attempt_type
order by attempt_type_cnt desc;