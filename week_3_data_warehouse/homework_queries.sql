-- Q1 => What is the count for fhv vehicle records for year 2019?
select count(1)
from `ace-element-375718.dtc_de_bq_dataset.fhv_tripdata_2019`
where EXTRACT(year from DATE(pickup_datetime)) = 2019
-- A1 => 43,244,696
;

-- Q2 => Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables.
--       What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
select count(Affiliated_base_number)
from `ace-element-375718.dtc_de_bq_dataset.fhv_tripdata_2019`
-- A2 => 0 MB for the External Table and 317.94MB for the BQ Table
;

-- Q3 => How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?
select count(1)
from `ace-element-375718.dtc_de_bq_dataset.fhv_tripdata_2019`
where PUlocationID is null
and  DOlocationID is null
-- A3 => 717,748
;

-- Q4 => What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?
-- A4 => Partition by affiliated_base_number Cluster on pickup_datetime
--;

-- Q5 => Implement the optimized solution you chose for question 4. Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive).
--       Use the BQ table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? Choose the answer which most closely matches.

-- A5 => 
-- 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table
--;

-- Q6 => Where is the data stored in the External Table you created?
-- A6 => GCP Bucket
--;

-- Q7 => It is best practice in Big Query to always cluster your data:
-- A7 => 
-- True
--;

-- (Not required) Q8 => 
-- A better format to store these files may be parquet. Create a data pipeline to download the gzip files and convert them into parquet. Upload the files to your GCP Bucket and create an External and BQ Table.

-- Note: Column types for all files used in an External Table must have the same datatype. While an External Table may be created and shown in the side panel in Big Query, this will need to be validated by running a count query on the External Table to check if any errors occur.
-- A8
--;











