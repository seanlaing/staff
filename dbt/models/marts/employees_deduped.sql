-- One row per (name, phone) keeping the latest transform timestamp.
with ranked as (
    select
        *,
        row_number() over (
            partition by employee_name, phone_digits
            order by transformed_at desc
        ) as row_num
    from {{ ref('stg_employees') }}
)

select
    employee_name,
    job_title,
    office,
    address,
    phone_digits,
    transformed_at
from ranked
where row_num = 1
