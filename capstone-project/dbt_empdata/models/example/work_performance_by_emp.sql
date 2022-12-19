select  
	e.emp_number,
    e.gender,
    e.edu_field,
    e.age,
    e.maritalstatus,
    e.dist_fromhome,
    t.y_atcompany,
    t.totalworkingyears,
    t.numcompaniesworked,
    t.y_incurrentrole,
    w.department,
    w.jobrole,
    w.overtime,
    w.bsn_travel,
    w.performancerating,
    t.attrition,
    s.jobsatisfaction,
    s.relationshipsatisfaction,
    s.env_satisfaction,
    s.worklifebalance,
    l.monthlyincome,
    l.monthlyrate,
    l.hourlyrate,
    l.percentsalaryhike
from {{ ref('stg_emp') }} as e
join {{ ref('stg_time') }} as t on e.emp_number = t.emp_number
join {{ ref('stg_workinf') }} as w on e.emp_number = w.emp_number
join {{ ref('stg_satisfaction') }} as s on e.emp_number = s.emp_number
join {{ ref('stg_salary') }} as l on e.emp_number = l.emp_number