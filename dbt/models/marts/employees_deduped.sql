-- Backwards-compatible alias; use dim_employees for new work.
select * from {{ ref('dim_employees') }}
