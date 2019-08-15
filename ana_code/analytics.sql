use asb862;

create external table taxi (vendorid int, p_year int, p_mon int, d_year int, d_mon int, passcnt int, tripdist float, p_zoneid int, d_zoneid int, fareamt float, tip float, total float)       row format delimited fields terminated by ','       location '/user/asb862/taxiinput/';

create external table crime (latitude float, longitude float, year int, pdate string, ptime string, ddate string, dtime string, borough string, type string, crimetaxizoneid int, year1 int)       row format delimited fields terminated by ','       location '/user/asb862/crimeinput/';  

--count pickups per zone

create table taxi_p as SELECT p_zoneid, count(*) FROM taxi GROUP BY p_zoneid;

 
ALTER TABLE taxi_p CHANGE `_c1` `cnt` bigint;

select * from taxi_p;

 

--count drop offs pr zone

create table taxi_d as SELECT d_zoneid, count(*) as cnt FROM taxi GROUP BY d_zoneid;


select * from taxi_d;

 

--count crime per zone

create table crime_zone as SELECT crimetaxizoneid, count(*) FROM crime GROUP BY  crimetaxizoneid;

 create external table taxizone (borough string, zone string, zoneid int)  row format delimited fields terminated by ','       location '/user/asb862/taxizone/';

 

--260 NY zones

create table cleantaxizone as select * from taxizone where zoneid is not NULL;

 

--join borough to taxi pickup grouped by zone

 create table taxi_p_b as SELECT t1.p_zoneid, t1.cnt, t2.borough FROM taxi_p t1 LEFT JOIN cleantaxizone t2 ON (t1.p_zoneid=t2.zoneid) ;

 

--join borough in taxi

 create table taxi1 as SELECT t1.*, t2.borough FROM taxi t1 INNER JOIN cleantaxizone t2 ON (t1.p_zoneid=t2.zoneid) ;

 

--taxi pickups by borough

create table taxi_b_cnt_p as SELECT borough, count(*) FROM taxi1 GROUP BY borough;

 

--removing EWR from boroughs

INSERT OVERWRITE TABLE taxi_b_cnt_p  SELECT * FROM taxi_b_cnt_p WHERE borough!='EWR';



--renaming count column

ALTER TABLE taxi_b_cnt_p CHANGE `_c1` `cnt` bigint;

select * from taxi_b_cnt_p sort by cnt;

 

--top 10 zone by pickup count

select * from taxi_p  sort by cnt desc limit 10;

 

--top 10 zone by drop count

 describe taxi_d;

 

select * from taxi_d  sort by cnt desc limit 10;

ALTER TABLE crime_zone CHANGE `_c1` `cnt` bigint;

 

--top 10 zone by crime count

select * from crime_zone  sort by cnt desc limit 10;

 

--top 20 zone by pickup count

select * from taxi_p  sort by cnt desc limit 20;

 

--top 20 zone by drop count

 describe taxi_d;

 
select * from taxi_d  sort by cnt desc limit 20;


--top 10 zone by crime count

select * from crime_zone  sort by cnt desc limit 10;

 
--top 50 intersection

 
Create table top50_t as select * from taxi_p  sort by cnt desc limit 50;

 
Create table top50_c as  select * from crime_zone  sort by cnt desc limit 50;

 
Create table top50 as select t.p_zoneid, t.cnt as pickup_cnt , c.cnt from top50_t t inner join top50_c c ON (t.p_zoneid = c.crimetaxizoneid);

 
--borough wise pickups


--Manhattan

Create table taxi_man as select * from taxi1 where borough= 'Manhattan';

 

--Brooklyn

Create table taxi_bro as select * from taxi1 where borough= 'Brooklyn';

 

--Queens

Create table taxi_que as select * from taxi1 where borough= 'Queens';

 

--staten_island

Create table taxi_sta as select * from taxi1 where borough= 'Staten Island';

 

--bronx

Create table taxi_bronx as select * from taxi1 where borough= 'Bronx';

 
--borough wise crime

#Manhattan

Create table crime_man as select * from crime where borough= 'MANHATTAN';

 

#Brooklyn

Create table crime_bro as select * from crime where borough= 'BROOKLYN';

 

#Queens

Create table crime_que as select * from crime where borough= 'QUEENS';

 

#staten_island

Create table crime_sta as select * from crime where borough= 'STATEN ISLAND';

 

#bronx

Create table crime_bronx as select * from crime where borough= 'BRONX';



--crime taxi pickup pattern in manhattan


Create table man as select p_zoneid, count(*) as cnt from taxi_man GROUP BY p_zoneid sort by cnt desc limit 10;

Create table crime_man1 as Select borough,crimetaxizoneid, count(*) as cnt from crime group by borough, crimetaxizoneid   ;

select crimetaxizoneid, cnt from crime_man1 where borough =  'MANHATTAN' sort by cnt desc limit 10;



-- crime taxi lowest count for pickup and taxi in every borough


select p_zoneid,count(*) as cnt from taxi_man group by p_zoneid sort by cnt asc limit 10;


select crimetaxizoneid,count(*) as cnt from crime_man group by crimetaxizoneid sort by cnt asc limit 10;


select p_zoneid,count(*) as cnt from taxi_bro group by p_zoneid sort by cnt asc limit 10;


select crimetaxizoneid,count(*) as cnt from crime_bro group by crimetaxizoneid sort by cnt asc limit 10;


select p_zoneid,count(*) as cnt from taxi_que group by p_zoneid sort by cnt asc limit 10;


select crimetaxizoneid,count(*) as cnt from crime_que group by crimetaxizoneid sort by cnt asc limit 10;


select p_zoneid,count(*) as cnt from taxi_bronx group by p_zoneid sort by cnt asc limit 10;


select crimetaxizoneid,count(*) as cnt from crime_bronx group by crimetaxizoneid sort by cnt asc limit 10;


select p_zoneid,count(*) as cnt from taxi_sta group by p_zoneid sort by cnt asc limit 10;


select crimetaxizoneid,count(*) as cnt from crime_sta group by crimetaxizoneid sort by cnt asc limit 10;



-- crime taxi highest count for pickup and taxi in every borough



select p_zoneid,count(*) as cnt from taxi_man group by p_zoneid sort by cnt desc limit 10;


select crimetaxizoneid,count(*) as cnt from crime_man group by crimetaxizoneid sort by cnt desc limit 10;


select p_zoneid,count(*) as cnt from taxi_bro group by p_zoneid sort by cnt desc limit 10;


select crimetaxizoneid,count(*) as cnt from crime_bro group by crimetaxizoneid sort by cnt desc limit 10;


select p_zoneid,count(*) as cnt from taxi_que group by p_zoneid sort by cnt desc limit 10;


select crimetaxizoneid,count(*) as cnt from crime_que group by crimetaxizoneid sort by cnt desc limit 10;


select p_zoneid,count(*) as cnt from taxi_bronx group by p_zoneid sort by cnt desc limit 10;


select crimetaxizoneid,count(*) as cnt from crime_bronx group by crimetaxizoneid sort by cnt desc limit 10;


select p_zoneid,count(*) as cnt from taxi_sta group by p_zoneid sort by cnt desc limit 10;


select crimetaxizoneid,count(*) as cnt from crime_sta group by crimetaxizoneid sort by cnt desc limit 10;




-- crime types in different zones

Create table crime_type as select crimetaxizoneid, type, count(*) as cnt from crime GROUP BY crimetaxizoneid,type;
