/*
Question 5. The number of passengers
In 2019-01-01 how many trips had 2 and 3 passengers?

2: 1282 ; 3: 266
2: 1532 ; 3: 126
2: 1282 ; 3: 254
2: 1282 ; 3: 274
*/

select passenger_count, count(*)
from public."green_tripdata_2019-01"
where lpep_pickup_datetime::date = '2019-01-01' --and lpep_dropoff_datetime::date = '2019-01-01'
group by passenger_count
having passenger_count between 2 and 3
;