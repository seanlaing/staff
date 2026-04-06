with src as (
    select * from {{ source('raw', 'employees_raw') }}
)

select
    trim(name) as employee_name,
    trim(title) as job_title,
    trim(office) as office,
    trim(address) as address,
    regexp_replace(trim(phone), '[^0-9]', '') as phone_digits,
    current_timestamp() as transformed_at
from src
