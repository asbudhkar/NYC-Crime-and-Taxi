// Cleaning the crime data

import java.io.IOException;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapred.MapReduceBase; 
import org.apache.hadoop.mapred.Mapper; 
import org.apache.hadoop.mapred.OutputCollector; 
import org.apache.hadoop.mapred.Reporter;
public class CleaningMapper extends MapReduceBase  implements Mapper<LongWritable, Text, Text, IntWritable> {   
public void map(LongWritable key, Text value,      
OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {        

// Get the line
String line = value.toString();  

// Get all CSV file column values by splitting on ,
String headers[] = line.split(",");   

int length = headers.length;

boolean error = false;


try {
// try if Exception occurs due to missing or NA data then skip the data record


// Check if latitude is null or missing
String latitude = headers[length-4];
if (latitude == null || latitude.equals("")) {
   error = true;
}


// Check if longitude is null or missing
String longitude = headers[length-3];

if (longitude == null || longitude.equals("")) {
   error = true;
}


// Get complaint number
String complaintNo = headers[0];

// Get complaintoccurrenceDate
String complaintOccurrenceDate = headers[3];

// Get the date at which complaint was registered
String year="";
String month="";
String date1 ="";

if (!complaintOccurrenceDate.equals("")) {
   String date[] = headers[3].split("/");

   year = date[2];
   
   int yr = Integer.parseInt(year);

   // Check for wrong years
   if (yr>2019 || yr <2006) 
	error = true;
   
   month = date[0];

   int mon = Integer.parseInt(month);

   // Check for wrong months
   if(mon >12 || mon<1)
	error =true;

   date1 = date[1]; 	

   int day = Integer.parseInt(date1);
   // Check for wrong date
   if(day<0 || day>31)
        error =true;
}
else
   error = true;


// Get date at which complaint was ended
String complaintOccurrenceEndingDate = headers[5];

String endingyear="";
String endingmonth="";
String endingdate1 ="";

if (!complaintOccurrenceEndingDate.equals("")) {
   String date[] = headers[5].split("/");

   endingyear = date[2];

   int yr = Integer.parseInt(endingyear);

   // Check for wrong year
   if (yr>2019 || yr <2006)
        error = true;

   endingmonth = date[0];

   int mon = Integer.parseInt(endingmonth);

// Check for wrong month 
   if(mon >12 || mon<1)
        error =true;

   endingdate1 = date[1];

   int day = Integer.parseInt(endingdate1);
   if(day<0 || day>31)
	error =true;
}
else
   error = true;

// Get time at which complaint was registered
String time = headers[4];
if(!time.equals("")) {
   String timesplit[] = time.split(":");
   int hr = Integer.parseInt(timesplit[0]);

   // Check for proper time range
   if (hr<0 || hr>23)
       error =true;

   int min = Integer.parseInt(timesplit[1]);
   if(min<0 || min>60)
       error = true;

   int sec = Integer.parseInt(timesplit[2]);
   if(sec<0 || sec>60)
       error = true;

   String type = headers[13];

   if (headers.equals(""))
        error = true;
}
else
    error = true;

// Get complaint end time
String endingtime = headers[6];
if (!endingtime.equals("")) {
   String timesplit[] = endingtime.split(":");
   int hr = Integer.parseInt(timesplit[0]);

   // Check for proper time range
   if (hr<0 || hr>23)
       error =true;

   int min = Integer.parseInt(timesplit[1]);
   if(min<0 || min>60)
       error = true;

   int sec = Integer.parseInt(timesplit[2]);
   if(sec<0 || sec>60)
       error = true;

   String type = headers[13];

   if (headers.equals(""))
        error = true;
}
else
    error = true;


// Get the borough name
String borough = headers[2];

if (!borough.equals("MANHATTAN") && !borough.equals("BROOKLYN") && !borough.equals("BRONX") && !borough.equals("STATEN ISLAND") && !borough.equals("QUEENS"))
  error = true; 

String type = headers[15];
String val = latitude+","+longitude+","+date1+"/"+month+"/"+year+","+time+","+endingdate1+"/"+endingmonth+"/"+endingyear+","+endingtime+","+borough+","+type+"."; 

// Pass the entire clean record as key 
if ( error == false) {
    output.collect(new Text(val), new IntWritable(1));    
}
}
catch(Exception e) {
// Skip the record 
}
}  
} 

