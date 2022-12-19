select  
    ROW_NUMBER() OVER (ORDER BY 1) AS time_id,
    totalworkingyears,
    trainingtimeslastyear,
    y_atcompany,
    y_incurrentrole,
    y_sincelastpromotion,
    y_withcurrmanager,
    numcompaniesworked,
    attrition,
    emp_number
from hr_emp