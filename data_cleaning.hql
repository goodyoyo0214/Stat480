-- bash part of removing header
-- sed -i '1d' 2003.csv
-- sed -i '1d' 1999.csv


-- Create the delay table
set hive.cli.print.header=true;

Create table delay (
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
Cancelled TINYINT, 
CancellationCode STRING,
Diverted STRING,
CarrierDelay INT,
WeatherDelay INT,
NASDelay INT,
SecurityDelay INT,
LateAircraftDelay INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

-- Store 1999 and 2003 table into delay

load data local inpath '/home/teyaoyl2/Stat480/HW/Group_project/1999.csv'
into table delay;

load data local inpath '/home/teyaoyl2/Stat480/HW/Group_project/2003.csv'
into table delay;


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
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ( 
"separatorChar" = ",",
   "quoteChar"  = "\"");


load data local inpath '/home/teyaoyl2/Stat480/HW/Group_project/airports.csv'
overwrite into table airports;

-- create carrier table
create table carrier (
code string,
description string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ( 
"separatorChar" = ",",
   "quoteChar"  = "\"");

load data local inpath '/home/teyaoyl2/Stat480/HW/Group_project/carriers.csv'
into table carrier;


-- create plan data
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

load data local inpath '/home/teyaoyl2/Stat480/HW/Group_project/plane_data.csv'
into table carrier;

-- view for join delayed and airport data
create view delayed_airport
as
select * from 
delay as d left join airports as a
on d.Origin = a.iata;


-- view for join delayed and airport data (destination)
create view delayed_airport_dest
as
select * from 
delay as d left join airports as a
on d.Dest = a.iata;

select regexp_extract(DepTime , )







