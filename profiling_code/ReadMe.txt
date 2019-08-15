Cleaning NYPD_Complaint_Data_Current__Year_To_Date_.csv must be injested in hadoop as given in ingestion code

javac -classpath `yarn classpath` -d . Cleaning.java CleaningMapper.java CleaningReducer.java
jar -cvf jar.jar *.class 
hadoop jar jar.jar Cleaning NYPD_Complaint_Data_Current__Year_To_Date_.csv CleaningData   
hdfs dfs -cat CleaningData/part-00000

hdfs dfs -get CleaningData/part-00000 cleanCrimeData.csv
hdfs dfs -put cleanCrimeData.csv

javac -classpath `yarn classpath` -d . CleaningP.java CleaningPMapper.java CleaningPReducer.java
jar -cvf jar.jar *.class 
hadoop jar jar.jar CleaningP NYPD_Complaint_Data_Current__Year_To_Date_.csv CleaningPData   
hdfs dfs -cat CleaningPData/part-00000

hdfs dfs -get CleaningPData/part-00000 cleanTaxiData.csv
hdfs dfs -put cleanTaxiData.csv

javac -classpath `yarn classpath` -d . CleaningZ.java CleaningZMapper.java CleaningZReducer.java
jar -cvf jar.jar *.class 
hadoop jar jar.jar CleaningZ NYPD_Complaint_Data_Current__Year_To_Date_.csv CleaningZData   
hdfs dfs -cat CleaningZData/part-00000

hdfs dfs -get CleaningZData/part-00000 cleanTaxiZoneData.csv
hdfs dfs -put cleanTaxiZoneData.csv

Profiling Insert the cleanCrime.csv in Hadoop to profile the required columns

javac -classpath `yarn classpath` -d . Profiling.java ProfilingMapper.java ProfilingReducer.java
jar -cvf jar.jar *.class
hadoop jar jar.jar Profiling cleanCrimeData.csv profilingOutput
hdfs dfs -cat profilingOutput/part-00000
hdfs dfs -get profilingOutput/part-00000 profileOut.txt



