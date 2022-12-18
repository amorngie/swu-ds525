select  
	e.emp_number,
    e.gender,
    e.edu_field,
    e.age,
    t.y_atcompany,
    t.totalworkingyears,
    t.numcompaniesworked,
    t.y_incurrentrole,
    t.attrition
from {{ ref('stg_emp') }} as e
join {{ ref('stg_time') }} as t 
on e.emp_number = t.emp_number