select
	case 
		when a.is_correct = True then 'Успешно'
		when a.is_correct = False then 'Неверно'
	end as is_success,
	count(*) as success_cnt
from attempts a
where true
	and a.is_correct is not null 
	and {{date}}
group by is_success
order by success_cnt desc;