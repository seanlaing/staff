-- One row per employee after null filtering and dedupe; primary grain for dashboard joins.
with valid as (
    select *
    from {{ ref('stg_employees') }}
    where employee_name is not null
      and job_title is not null
      and office is not null
      and phone_digits is not null
      and length(phone_digits) >= 10
),

ranked as (
    select
        valid.*,
        row_number() over (
            partition by employee_name, phone_digits
            order by transformed_at desc
        ) as row_num
    from valid
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
