# Study of relationship between NYPD complaints data and NYC Yellow taxi data
## Steps to ingest the data:
### Crime

Data Source Used: NYPD Complaint Data Current (Year To Date), Historic
https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Current-Year-To-Date-/5uac-w243

1) Download the CSV from the website using export as CSV for years 2017 using filter

2) Setup hpc tunnel

3) Run from cmd prompt
hpc gateway

Consider the downloaded filename as filename.csv

4) scp filename.csv asb862@dumbo:projectRBDA

5) Put the data in HDFS
Log into dumbo cluster

ssh asb862@gw.hpc.nyu.edu

ssh asb862@dumbo.hpc.nyu.edu

cd projectRDBA

hdfs dfs -put filename.csv crimeData.csv

### Taxi
Data Source Used: 2018 Yellow Taxi Trip Data
https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Current-Year-To-Date-/5uac-w243

Click on ‘View Data’ then Export CSV

1) Download the CSV from the website using export as CSV

2) Setup hpc tunnel

3) Copy file to dumbo using WinSCP client

4) Put the data in HDFS
Connect through cisco VPN client

ssh as12578@dumbo.hpc.nyu.edu


hdfs dfs -ls /user/as12578
hdfs dfs -mkdir /user/as12578/project
hdfs dfs -put taxi.csv   taxiData.csv

6) Use the dataset stored in hadoop for cleaning and profiling code


### TaxiZone
Data Source Used: NYC Taxi Zones
https://data.cityofnewyork.us/Transportation/NYC-Taxi-Zones/d3c5-ddgc

1) Download the CSV from the website using export as CSV 

2) Setup hpc tunnel

3) Run from cmd prompt
hpc gateway

Consider the downloaded filename as filename.csv

4) scp filename.csv asb862@dumbo:projectRBDA

5) Put the data in HDFS
Log into dumbo cluster

ssh asb862@gw.hpc.nyu.edu

ssh asb862@dumbo.hpc.nyu.edu

cd projectRDBA

hdfs dfs -put filename.csv taxizone.csv

6) Use the dataset stored in hadoop for cleaning and profiling code

## Steps to clean the data:

### Cleaning: 
1. crimeData.csv must be injested in hadoop as given in ingestion code
2. taxiData.csv must be injested in hadoop as given in ingestion code
3. taxizone.csv must be injested in hadoop as given in ingestion code

javac -classpath `yarn classpath` -d . Cleaning.java CleaningMapper.java CleaningReducer.java
jar -cvf jar.jar *.class 
hadoop jar jar.jar Cleaning crimeData.csv CleaningData   
hdfs dfs -cat CleaningData/part-00000

hdfs dfs -get CleaningData/part-00000 cleanCrimeData.csv
hdfs dfs -put cleanCrimeData.csv

javac -classpath `yarn classpath` -d . CleaningP.java CleaningPMapper.java CleaningPReducer.java
jar -cvf jar.jar *.class 
hadoop jar jar.jar CleaningP taxiData.csv CleaningPData   
hdfs dfs -cat CleaningPData/part-00000

hdfs dfs -get CleaningPData/part-00000 cleanTaxiData.csv
hdfs dfs -put cleanTaxiData.csv

javac -classpath `yarn classpath` -d . CleaningZ.java CleaningZMapper.java CleaningZReducer.java
jar -cvf jar.jar *.class 
hadoop jar jar.jar CleaningZ taxizone.csv CleaningZData   
hdfs dfs -cat CleaningZData/part-00000

hdfs dfs -get CleaningZData/part-00000 cleanTaxiZoneData.csv
hdfs dfs -put cleanTaxiZoneData.csv


## Profiling:
Insert the cleanCrime.csv in Hadoop to profile the required columns

javac -classpath `yarn classpath` -d . Profiling.java ProfilingMapper.java ProfilingReducer.java
jar -cvf jar.jar *.class
hadoop jar jar.jar Profiling cleanCrimeData.csv profilingOutput
hdfs dfs -cat profilingOutput/part-00000
hdfs dfs -get profilingOutput/part-00000 profileOut.txt


## Analytics code

### Put the cleaned taxi data in hadoop in folder taxiinput
hdfs dfs -mkdir taxiinput
hdfs dfs -put cleanTaxiData.csv taxiinput/cleanTaxiData.csv

### Put the cleaned crime data in hadoop in folder crimeinput
hdfs dfs -mkdir crimeinput
hdfs dfs -put cleanCrimeData.csv crimeinput/cleanCrimeData.csv

### Put the cleaned taxizone data in hadoop in folder taxizone
hdfs dfs -mkdir taxizone
hdfs dfs -put cleanTaxiZoneData.csv taxizone/cleanTaxiZoneData.csv

### Execute script in beeline
beeline -u jdbc:hive2://babar.es.its.nyu.edu:10000/ -n netid -p pass -f analytics.sql

