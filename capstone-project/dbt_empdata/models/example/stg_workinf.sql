select 
    ROW_NUMBER() OVER (ORDER BY 1) AS inf_id,
    department,
    jobrole,
    overtime,
    bsn_travel,
    performancerating,
    stockoptionlevel,
    emp_number
from hr_emp