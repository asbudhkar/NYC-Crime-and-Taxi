import java.io.IOException;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapred.MapReduceBase; 
import org.apache.hadoop.mapred.Mapper; 
import org.apache.hadoop.mapred.OutputCollector; 
import org.apache.hadoop.mapred.Reporter;
public class ProfilingMapper extends MapReduceBase  implements Mapper<LongWritable, Text, Text, IntWritable> {   
public void map(LongWritable key, Text value,      
OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {        

// Get a line from cleaned crime dataset
String line = value.toString();

// Get all the columns
String headers[] = line.split(",");

int length = headers.length;

// Get crime date
String date = headers[2];

String datesplit[] = date.split("/");

int year = Integer.parseInt(datesplit[2]);
int month = Integer.parseInt(datesplit[1]);
int day = Integer.parseInt(datesplit[0]);

// Get crime enddate
String enddate = headers[4];

String enddatesplit[] = date.split("/");

int endyear = Integer.parseInt(enddatesplit[2]);
int endmonth = Integer.parseInt(enddatesplit[1]);
int endday = Integer.parseInt(enddatesplit[0]);

// Get time of crime
String time = headers[3];
 

// Get endtime of crime
String endtime = headers[5];

// Get latitude where crime occurred
String latitude = headers[0];

// Get longitude where crime occurred
String longitude = headers[1];

// Get the borough
String borough = headers[6];

// Write the output
output.collect(new Text("endyear"), new IntWritable(endyear));

output.collect(new Text("endmonth"), new IntWritable(endmonth));

output.collect(new Text("endday"), new IntWritable(endday));


output.collect(new Text("year"), new IntWritable(year));  

output.collect(new Text("month"), new IntWritable(month));
  
output.collect(new Text("day"), new IntWritable(day));

output.collect(new Text(borough), new IntWritable(1));

output.collect(new Text("time"), new IntWritable(1));

output.collect(new Text("endtime"), new IntWritable(1));

output.collect(new Text("type"), new IntWritable(1));

output.collect(new Text("latitude"), new IntWritable(1));

output.collect(new Text("longitude"), new IntWritable(1));
}  
} 

