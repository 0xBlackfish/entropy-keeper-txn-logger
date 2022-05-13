with
txns as (
  select
    ttl.entropy_keeper_address,
    count(distinct ttl.transaction_id) as total_transactions_confirmed,
    count(distinct case when ttl.instruction_type = 'CachePerpMarkets' then ttl.transaction_id end) as CachePerpMarkets_txns,
    count(distinct case when ttl.instruction_type = 'CachePrices' then ttl.transaction_id end) as CachePrices_txns,
    count(distinct case when ttl.instruction_type = 'CacheRootBanks' then ttl.transaction_id end) as CacheRootBanks_txns,
    count(distinct case when ttl.instruction_type = 'UpdateRootBank' then ttl.transaction_id end) as UpdateRootBank_txns,
    count(distinct case when ttl.instruction_type = 'UpdateFunding' then ttl.transaction_id end) as UpdateFunding_txns,
    count(distinct case when ttl.instruction_type = 'ConsumeEvents' then ttl.transaction_id end) as ConsumeEvents_txns,
    count(distinct ttl.transaction_id) * 0.000005 as total_sol_burn
  from entropy.txns_time_lag ttl
  where
    date(ttl.date_time) <= date('{0}')
  group by 1
  order by 2 desc
),

rewards_and_time as (
  select
    krd.entropy_keeper_address,
    -- KEEPER INFO
    min(krd.date) as first_keeper_day,
    count(distinct krd.date) as number_of_days_keeping,
    -- REWARD STATS
    avg(case when instruction_type = 'ConsumeEvents' then krd.daily_keeper_reward end) as avg_daily_consume_events_reward,
    avg(case when instruction_type = 'OtherEvents' then krd.daily_keeper_reward end) as avg_daily_other_events_reward,
    max(case when instruction_type = 'ConsumeEvents' then krd.daily_keeper_reward end) as best_consume_events_reward,
    max(case when instruction_type = 'OtherEvents' then krd.daily_keeper_reward end) as best_other_events_reward,
    min(case when instruction_type = 'ConsumeEvents' then krd.daily_keeper_reward end) as worst_consume_events_reward,
    min(case when instruction_type = 'OtherEvents' then krd.daily_keeper_reward end) as worst_other_events_reward,
    -- PCT OF TIME STATS
    avg(case when instruction_type = 'ConsumeEvents' then krd.pct_of_time end) as avg_daily_consume_events_pct_of_time,
    avg(case when instruction_type = 'OtherEvents' then krd.pct_of_time end) as avg_daily_other_events_pct_of_time,
    max(case when instruction_type = 'ConsumeEvents' then krd.pct_of_time end) as best_consume_events_pct_of_time,
    max(case when instruction_type = 'OtherEvents' then krd.pct_of_time end) as best_other_events_pct_of_time,
    min(case when instruction_type = 'ConsumeEvents' then krd.pct_of_time end) as worst_consume_events_pct_of_time,
    min(case when instruction_type = 'OtherEvents' then krd.pct_of_time end) as worst_other_events_pct_of_time
  from entropy.keeper_rewards_daily krd
  where
    date(krd.date) <= date('{0}')
  group by 1
  order by 4 desc
)

select
  rnt.*,
  txns.total_transactions_confirmed,
  txns.CachePerpMarkets_txns,
  txns.CachePrices_txns,
  txns.CacheRootBanks_txns,
  txns.UpdateRootBank_txns,
  txns.UpdateFunding_txns,
  txns.ConsumeEvents_txns,
  txns.total_sol_burn
from txns
  left join rewards_and_time rnt on rnt.entropy_keeper_address = txns.entropy_keeper_address