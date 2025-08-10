select
  region,
  timestamp,
  demand_mwh,
  lag(demand_mwh, 1) over (partition by region order by timestamp) as demand_last_hour,
  avg(demand_mwh) over (partition by region order by timestamp rows between 6 preceding and current row) as demand_rolling_mean_6h,
  extract(hour from timestamp) as hour_of_day,
  extract(dow from timestamp) as day_of_week
from staging.stg_energy
