/*
Question 4. Largest trip for each day
Which was the day with the largest trip distance Use the pick up time for your calculations.

2019-01-18
2019-01-28
2019-01-15
2019-01-10
*/

select trip_distance, lpep_pickup_datetime::date, lpep_dropoff_datetime::date
from public."green_tripdata_2019-01"
order by 1 desc
limit 1
;