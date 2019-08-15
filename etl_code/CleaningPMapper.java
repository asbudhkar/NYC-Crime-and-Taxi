// Cleaning the taxi data

import java.io.IOException;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapred.MapReduceBase; 
import org.apache.hadoop.mapred.Mapper; 
import org.apache.hadoop.mapred.OutputCollector; 
import org.apache.hadoop.mapred.Reporter;
public class CleaningPMapper extends MapReduceBase  implements Mapper<LongWritable, Text, Text, IntWritable> {   
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


// Get vendorID
String vendorID = headers[0];

// Get pickupDateTime
String pickupDateTime = headers[1];

String pickupDate = pickupDateTime.split(" ")[0];

String year="";
String month="";
String date1 ="";

if (!pickupDate.equals("")) {
   String date[] = pickupDate.split("/");

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


String dropoffDateTime = headers[2];
String dropoffDate = dropoffDateTime.split(" ")[0];

String endingyear="";
String endingmonth="";
String endingdate1 ="";

if (!dropoffDate.equals("")) {
   String date[] = dropoffDate.split("/");

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



String passCount = headers[3];
String tripdist = headers[4];
String puzoneid = headers[7];
String dozoneid = headers[8];
String fairamt = headers[10];
String tip = headers[13];
String totalamt = headers[16];

String val = vendorID+","+year+","+month+","+endingyear+","+endingmonth+","+passCount+","+tripdist+","+puzoneid+","+dozoneid+","+fairamt+","+tip+","+totalamt+","; 

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

