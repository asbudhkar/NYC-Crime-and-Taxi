// Cleaning the taxi zone data

import java.io.IOException;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapred.MapReduceBase; 
import org.apache.hadoop.mapred.Mapper; 
import org.apache.hadoop.mapred.OutputCollector; 
import org.apache.hadoop.mapred.Reporter;
public class CleaningZMapper extends MapReduceBase  implements Mapper<LongWritable, Text, Text, IntWritable> {   
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

String zone = headers[length-3];

String zoneid = headers[length-2];

// Get the borough name
String borough = headers[length-1];

String val = borough+","+zone+","+zoneid+","; 

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

