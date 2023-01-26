/*
How many taxi trips were totally made on January 15?

Tip: started and finished on 2019-01-15.

Remember that lpep_pickup_datetime and lpep_dropoff_datetime columns are in the format timestamp (date and hour+min+sec) and not in date.

20689
20530
17630
21090
*/

select count(*)
from public."green_tripdata_2019-01"
where (lpep_pickup_datetime::date = '2019-01-15'::date and lpep_dropoff_datetime::date = '2019-01-15')
;