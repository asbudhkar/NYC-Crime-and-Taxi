import java.io.IOException; 
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapred.MapReduceBase; 
import org.apache.hadoop.mapred.OutputCollector; 
import org.apache.hadoop.mapred.Reducer; 
import org.apache.hadoop.mapred.Reporter;
import java.util.*;
public class ProfilingReducer extends MapReduceBase  implements Reducer<Text, IntWritable, Text, IntWritable> 
{
  public void reduce(Text key, Iterator<IntWritable> values,      OutputCollector<Text, IntWritable> output, Reporter reporter)      throws IOException 
{

List<String> types = new ArrayList<String>();        

// Variables for profiling
int total = 0; 
int sum = 0;

// Profile year max and min
if (key.toString().equals("year")) {
int maxValue = Integer.MIN_VALUE;   
int minValue = Integer.MAX_VALUE; 
while (values.hasNext()) 
{      
maxValue = Math.max(maxValue, values.next().get());    
minValue = Math.min(minValue, values.next().get());
}  

Text keyval = new Text("Maxyear");

// Profile DATE
output.collect(new Text("CMPLNT_FR_DT:\tDateTime\t Exact date of occurrence for the reported event (or starting date of occurrence, if CMPLNT_TO_DT exists)"), new IntWritable(0));  

output.collect(keyval, new IntWritable(maxValue));
keyval = new Text("Minyear");
output.collect(keyval, new IntWritable(minValue));  
 
}

// Profile month
if (key.toString().equals("month")) {
int maxValue = Integer.MIN_VALUE;
int minValue = Integer.MAX_VALUE;
while (values.hasNext())
{
maxValue = Math.max(maxValue, values.next().get());
minValue = Math.min(minValue, values.next().get());
}
Text keyval = new Text("Maxmonth");
output.collect(keyval, new IntWritable(maxValue));
keyval = new Text("Minmonth");
output.collect(keyval, new IntWritable(minValue));

}


// Profile day
if (key.toString().equals("day")) {
int maxValue = Integer.MIN_VALUE;
int minValue = Integer.MAX_VALUE;
while (values.hasNext())
{
maxValue = Math.max(maxValue, values.next().get());
minValue = Math.min(minValue, values.next().get());
}
Text keyval = new Text("Maxday");
output.collect(keyval, new IntWritable(maxValue));
keyval = new Text("Minday");
output.collect(keyval, new IntWritable(minValue));

}


if (key.toString().equals("date")) {
int maxValue = Integer.MIN_VALUE;
int minValue = Integer.MAX_VALUE;
while (values.hasNext())
{
maxValue = Math.max(maxValue, values.next().get());
minValue = Math.min(minValue, values.next().get());
}
Text keyval = new Text("Maxdate");
output.collect(keyval, new IntWritable(maxValue));
keyval = new Text("Mindate");
output.collect(keyval, new IntWritable(minValue));

}

// Profile endyear
if (key.toString().equals("endyear")) {
int maxValue = Integer.MIN_VALUE;
int minValue = Integer.MAX_VALUE;
while (values.hasNext())
{
maxValue = Math.max(maxValue, values.next().get());
minValue = Math.min(minValue, values.next().get());
}
Text keyval = new Text("Maxendyear");
output.collect(new Text("CMPLNT_TO_DT:\tDateTime\t Ending date of occurrence for the reported event, if exact time of occurrence is unknown"), new IntWritable(0));
output.collect(keyval, new IntWritable(maxValue));
keyval = new Text("Minendyear");
output.collect(keyval, new IntWritable(minValue));

}

// Profile end month
if (key.toString().equals("endmonth")) {
int maxValue = Integer.MIN_VALUE;
int minValue = Integer.MAX_VALUE;
while (values.hasNext())
{
maxValue = Math.max(maxValue, values.next().get());
minValue = Math.min(minValue, values.next().get());
}
Text keyval = new Text("Maxendmonth");
output.collect(keyval, new IntWritable(maxValue));
keyval = new Text("Minendmonth");
output.collect(keyval, new IntWritable(minValue));

}

//Profile end day
if (key.toString().equals("endday")) {
int maxValue = Integer.MIN_VALUE;
int minValue = Integer.MAX_VALUE;
while (values.hasNext())
{
maxValue = Math.max(maxValue, values.next().get());
minValue = Math.min(minValue, values.next().get());
}
Text keyval = new Text("Maxendday");
output.collect(keyval, new IntWritable(maxValue));
keyval = new Text("Minendday");
output.collect(keyval, new IntWritable(minValue));

}


// Profile borough
if(key.toString().equals("BRONX"))
output.collect(new Text("BORO_NM:\tString\tThe name of the borough in which the incident occurred"), new IntWritable(0));

if(key.toString().equals("MANHATTAN") ||key.toString().equals("BROOKLYN")||key.toString().equals("QUEENS")||key.toString().equals("BRONX")||key.toString().equals("STATEN ISLAND")) {

sum=0;
while(values.hasNext())
{
int val=values.next().get();
sum += val;
}
output.collect(new Text("Total crimes in "+key), new IntWritable(sum));
}


// Profile offense type description
if(key.toString().equals("type")) {
total =0;
while(values.hasNext()) {
total+=values.next().get();	
}
output.collect(new Text("OFNS_DESC:\tString\tDescription of offense corresponding with key code"), new IntWritable(0));
output.collect(new Text("Total Crimes"), new IntWritable(total));
}

// Profile time of crime
if(key.toString().equals("time")) {
   output.collect(new Text("CMPLNT_FR_TM:\tString\tExact time of occurrence for the reported event (or starting time of occurrence, if CMPLNT_TO_TM exists)"), new IntWritable(0));
}

// Profile endtime of crime
if(key.toString().equals("endtime")) {
   output.collect(new Text("CMPLNT_TO_TM:\tString\tEnding time of occurrence for the reported event, if exact time of occurrence is unknown)"), new IntWritable(0));
}


// Profile latitude of crime
if(key.toString().equals("latitude")) {
   output.collect(new Text("Latitude:\tString\tLatitude coordinate for Global Coordinate System, WGS 1984, decimal degrees (EPSG 4326)"), new IntWritable(0));

}

// Profile longitude of crime
if(key.toString().equals("longitude")) {
   output.collect(new Text("Longitude:\tString\tLongitude coordinate for Global Coordinate System, WGS 1984, decimal degrees (EPSG 4326)"), new IntWritable(0));

}
}
}
