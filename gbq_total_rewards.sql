select
  krd.entropy_keeper_address,
  sum(krd.daily_keeper_reward) as total_keeper_reward
from entropy.keeper_rewards_daily krd
where
  date(krd.date) <= date('{}')
group by 1