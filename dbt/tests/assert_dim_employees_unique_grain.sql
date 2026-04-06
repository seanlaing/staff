-- Fails if duplicate (employee_name, phone_digits) exist after dedupe.
select
    employee_name,
    phone_digits,
    count(*) as occurrences
from {{ ref('dim_employees') }}
group by employee_name, phone_digits
having count(*) > 1
