-- Steps to clean the data:

-- Cleaning: 
-- crimeData.csv must be injested in hadoop as given in ingestion code
-- taxiData.csv must be injested in hadoop as given in ingestion code
-- taxizone.csv must be injested in hadoop as given in ingestion code

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
