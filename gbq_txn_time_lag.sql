with
base as (
  select
    ttl.entropy_keeper_address,
    case when ttl.instruction_type = 'ConsumeEvents' then ttl.instruction_type else 'OtherEvents' end as instruction_type,
    sum(coalesce(ttl.seconds_since_last_txn,0)) as total_time_by_address_instruction_type
  from entropy.txns_time_lag ttl
  where
    date(ttl.date_time) = date('{}')
  group by 1,2
)

select
  b.entropy_keeper_address,
  b.instruction_type,
  b.total_time_by_address_instruction_type,
  sum(b.total_time_by_address_instruction_type) over (partition by instruction_type) as total_time_overall,
  (b.total_time_by_address_instruction_type * 1.0) / sum(b.total_time_by_address_instruction_type) over (partition by instruction_type) as pct_of_time
from base b