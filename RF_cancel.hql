set hive.cli.print.header=true;

-- create database
CREATE DATABASE IF NOT EXISTS Group_proj;
USE Group_proj;

-- create tables and store data

drop table IF EXISTS delay_RF;

Create table delay_RF(
Year INT,
Month INT,
DayofMonth INT,
DayOfWeek INT,
DepTime STRING,
CRSDepTime STRING,
ArrTime STRING,
CRSArrTime STRING,
UniqueCarrier STRING,
FlightNum STRING,
TailNum STRING,
ActualElapsedTime INT,
CRSElapsedTime INT,
AirTime INT,
ArrDelay INT,
DepDelay INT,
Origin STRING,
Dest STRING,
Distance INT,
TaxiIn INT,
TaxiOut INT,
Cancelled STRING, 
CancellationCode STRING,
Diverted STRING,
CarrierDelay INT,
WeatherDelay INT,
NASDelay INT,
SecurityDelay INT,
LateAircraftDelay INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ",";

-- Store 1999 and 2003 table into delay

load data local inpath "/home/teyaoyl2/Stat480/HW/Group_project/1999.csv"
into table delay_RF;

load data local inpath "/home/teyaoyl2/Stat480/HW/Group_project/2003.csv"
into table delay_RF;

select * from delay_RF limit 1;

-- create airport table
create table airports (
iata String, 
airport string,
city string,
state string,
country string, 
lat DOUBLE,
long DOUBLE 
)
ROW FORMAT SERDE "org.apache.hadoop.hive.serde2.OpenCSVSerde"
WITH SERDEPROPERTIES ( 
"separatorChar" = ",",
   "quoteChar"  = "\"");


load data local inpath "/home/teyaoyl2/Stat480/HW/Group_project/airports.csv"
overwrite into table airports;

-- create carrier table
create table carrier (
code string,
description string
)
ROW FORMAT SERDE "org.apache.hadoop.hive.serde2.OpenCSVSerde"
WITH SERDEPROPERTIES ( 
"separatorChar" = ",",
   "quoteChar"  = "\"");

load data local inpath "/home/teyaoyl2/Stat480/HW/Group_project/carriers.csv"
into table carrier;


-- create plan data table
create table plane_data(
tailnum string,
type string,
manufacturer string,
issue_date string,
model string,
status string,
aircraft_type string,
engine_type string,
year int
);

load data local inpath "/home/teyaoyl2/Stat480/HW/Group_project/plane_data.csv"
into table carrier;

show tables;

-- check if there is missing airport

select distinct d.origin
from delay_RF as d left join airports as a
on d.Origin = a.iata
where a.iata is null;

select distinct d.origin
from delay_RF as d left join airports as a
on d.Dest = a.iata
where a.iata is null;

------------------------------------------------
-- preprocess data for hivemall random forest
------------------------------------------------
-- ID(integer) of ariport 
drop table airport_id;

create table airport_id 
as
select  iata, ID - 1 as ID
from (
select  distinct iata, dense_rank() over (order by iata) as ID
from airports
) t;

-- ID(integer) of airlines
drop table airline_id;
create table airline_id 
as
select  UniqueCarrier, ID - 1 as ID
from (
select  distinct UniqueCarrier, dense_rank() over (order by UniqueCarrier) as ID
from delay_RF
) t;





-- table for random forest
create table final_data
as
select 
-- simple columns
d.Year as Year, 
d.Month as Month, 
d.DayofMonth as DayofMonth, 
d.DayOfWeek as DayOfWeek, 
p.ID as Origin_ID,
p2.ID as Dest_ID,
l.ID as UniqueCarrier,
d.CRSElapsedTime as CRSElapsedTime,
d.Distance as Distance,

CASE LENGTH(CRSDepTime)
when 1 then 0
WHEN 2 then 0
WHEN 3 then cast(SUBSTR(CRSDepTime,1,1) as int)
WHEN 4 then cast(SUBSTR(CRSDepTime,1,2) as int)
ELSE NULL
END
as CRSDep_Hour,

CASE LENGTH(CRSDepTime)
when 1 then cast(CRSDepTime as int)
WHEN 2 then cast(CRSDepTime as int) 
WHEN 3 then cast(SUBSTR(CRSDepTime,3,3) as int)
WHEN 4 then cast(SUBSTR(CRSDepTime,3,4) as int)
ELSE NULL
END
as CRSDep_Minute,

CASE LENGTH(CRSArrTime)
when 1 then 0
WHEN 2 then 0
WHEN 3 then cast(SUBSTR(CRSArrTime,1,1) as int)
WHEN 4 then cast(SUBSTR(CRSArrTime,1,2) as int)
ELSE NULL
END
as CRSArr_Hour,

CASE LENGTH(CRSArrTime)
when 1 then cast(CRSArrTime as int)
WHEN 2 then cast(CRSArrTime as int)
WHEN 3 then cast(SUBSTR(CRSArrTime,3,3) as int)
WHEN 4 then cast(SUBSTR(CRSArrTime,3,4) as int)
ELSE NULL
END
as CRSArr_Minute,

-- response
d.Cancelled as Cancelled

-- reference Origin &  Dest to airport_ID(int), UniqueCarrier to UniqueCarrier_ID(int)
from delay_RF as d left join airport_id as p
on d.Origin = p.iata
left join airport_id as p2
on d.Dest = p2.iata
left join airline_id as l
on d.UniqueCarrier = l.UniqueCarrier;

---------------------------------------


CREATE TABLE airmyp
AS
SELECT * FROM final_data;

INSERT OVERWRITE LOCAL DIRECTORY './delay_outPut/final_data' 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
select * from final_data;


-- table for random forest
create table final_data_str
as
select 
-- simple columns
cast(d.Year as string) as Year, 
cast(d.Month as string) as Month, 
cast(d.DayofMonth as  string) as DayofMonth, 
cast(d.DayOfWeek  as string) as DayOfWeek, 
cast(p.ID as string) as Origin_ID,
cast(p2.ID as  string) as Dest_ID,
cast(l.ID as  string) as UniqueCarrier,
cast(d.CRSElapsedTime as  string) as CRSElapsedTime,
cast(d.Distance as  string) as Distance,

CASE LENGTH(CRSDepTime)
when 1 then "0"
WHEN 2 then "0"
WHEN 3 then SUBSTR(CRSDepTime,1,1)
WHEN 4 then SUBSTR(CRSDepTime,1,2)
ELSE NULL
END
as CRSDep_Hour,

CASE LENGTH(CRSDepTime)
when 1 then CRSDepTime
WHEN 2 then CRSDepTime
WHEN 3 then SUBSTR(CRSDepTime,3,3)
WHEN 4 then SUBSTR(CRSDepTime,3,4)
ELSE NULL
END
as CRSDep_Minute,

CASE LENGTH(CRSArrTime)
when 1 then "0"
WHEN 2 then "0"
WHEN 3 then SUBSTR(CRSArrTime,1,1)
WHEN 4 then SUBSTR(CRSArrTime,1,2)
ELSE NULL
END
as CRSArr_Hour,

CASE LENGTH(CRSArrTime)
when 1 then CRSArrTime
WHEN 2 then CRSArrTime
WHEN 3 then SUBSTR(CRSArrTime,3,3)
WHEN 4 then SUBSTR(CRSArrTime,3,4)
ELSE NULL
END
as CRSArr_Minute,

-- response
cast(d.Cancelled as string) as Cancelled

-- reference Origin &  Dest to airport_ID(int), UniqueCarrier to UniqueCarrier_ID(int)
from delay_RF as d left join airport_id as p
on d.Origin = p.iata
left join airport_id as p2
on d.Dest = p2.iata
left join airline_id as l
on d.UniqueCarrier = l.UniqueCarrier;


-- Create training table--

-- trainig data
create table training
as
select
  rowid() as rowid,
  array(Year, Month,DayofMonth,DayOfWeek,Origin_ID,Dest_ID,UniqueCarrier,CRSElapsedTime,Distance,CRSDep_Hour,CRSDep_Minute,CRSArr_Hour,CRSArr_Minute) as features,
  Cancelled
from final_data;


create table training_vec
as
select
  rowid() as rowid,
  array(Year, Month,DayofMonth,DayOfWeek,Origin_ID,Dest_ID,UniqueCarrier,CRSElapsedTime,Distance,CRSDep_Hour,CRSDep_Minute,CRSArr_Hour,CRSArr_Minute) as features,
  Cancelled
from final_data_str
limit 2
;

create table training_vec_lable
as
select rowid,
vectorize_features( features, '1') as features,
Cancelled
from training_vec limit 1;



-- create model table
CREATE TABLE rf_model 
  STORED AS SEQUENCEFILE 
AS
select 
  train_randomforest_classifier(features, Cancelled,"-trees 2 -subsample 0.1 -min_samples_leaf 40") as (model_id, model_weight, model, var_importance, oob_errors, oob_tests)
 from
  training;

  CREATE TABLE rf_model 
  STORED AS SEQUENCEFILE 
AS
select 
  train_randomforest_classifier(features, Cancelled,"-trees 2 -subsample 0.1 -min_samples_leaf 40")
 from
  training;
  
  
-- Logistic regression 

create table logi
as
select 
 cast(feature as int) as feature,
 avg(weight) as weight
from 
 (select 
     logress(add_bias(features),label) as (feature,weight)
  from 
     training
 ) t 
group by feature;
