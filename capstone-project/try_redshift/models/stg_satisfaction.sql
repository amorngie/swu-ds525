select 
    ROW_NUMBER() OVER (ORDER BY 1) AS stf_id,
    jobsatisfaction,
    relationshipsatisfaction,
    env_satisfaction,
    worklifebalance,
    emp_number
from hr_emp