use crime_etl;

drop table crime_etl_temp;



create external table crime_etl_temp (State string,
City string,
Population string,
Violent_crime string,
Murder_and_nonnegligent_manslaughter string,
Rape_revised_definition1 string,
Rape_legacy_definition2 string,
Robbery string, 
Aggravated_assault string,
Property_crime string,
Burglary string,	
Larceny_theft string,	
Motor_vehicle_theft string,
Arson3 string)
partitioned by (ts string)
row format delimited
fields terminated by '|' 
location "/tmp/stream-out/crime_data";

msck repair table crime_etl_temp;

drop table crime_by_state;

create table crime_by_state as select state,count(violent_crime) as violent_crime from crime_etl.crime_etl_temp group by state;