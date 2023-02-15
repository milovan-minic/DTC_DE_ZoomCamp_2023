-- CREATE TABLE `ace-element-375718.dtc_de_bq_dataset.fhv_tripdata_2019`
-- AS SELECT * FROM `ace-element-375718.dtc_de_bq_dataset.fhv_tripdata_2019-01`;

-- insert into `ace-element-375718.dtc_de_bq_dataset.fhv_tripdata_2019`
-- select 
-- dispatching_base_num,
-- pickup_datetime,
-- dropOff_datetime,
-- PUlocationID,
-- DOlocationID,
-- CAST(SR_Flag AS INTEGER),
-- Affiliated_base_number
-- from `ace-element-375718.dtc_de_bq_dataset.fhv_tripdata_2019-12`;

-- select distinct EXTRACT(year from DATE(pickup_datetime)) AS `YEAR`, EXTRACT(month FROM DATE(pickup_datetime)) AS `MONTH`
-- from `ace-element-375718.dtc_de_bq_dataset.fhv_tripdata_2019`

CREATE or replace TABLE `ace-element-375718.dtc_de_bq_dataset.fhv_tripdata_2019_partitioned_clustered`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY(Affiliated_base_number)
AS SELECT * FROM `ace-element-375718.dtc_de_bq_dataset.fhv_tripdata_2019`;

select distinct affiliated_base_number
from `ace-element-375718.dtc_de_bq_dataset.fhv_tripdata_2019_partitioned_clustered` -- 23.05
-- from `ace-element-375718.dtc_de_bq_dataset.fhv_tripdata_2019_partitioned` -- 23.05
-- from `ace-element-375718.dtc_de_bq_dataset.fhv_tripdata_2019` --647.87
where pickup_datetime between '2019-03-01' and '2019-03-31'
;

select table_name, partition_id, total_rows
from `dtc_de_bq_dataset.INFORMATION_SCHEMA.PARTITIONS`
where table_name = `fhv_tripdata_2019_partitioned`
order by total_rows desc;







