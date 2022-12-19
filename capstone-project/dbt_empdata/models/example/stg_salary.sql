select 
    ROW_NUMBER() OVER (ORDER BY 1) AS salary_id,
    monthlyincome,
    monthlyrate,
    hourlyrate,
    percentsalaryhike,
    emp_number
from hr_emp