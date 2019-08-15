// Cleaning the taxi zone data

import java.io.IOException;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapred.FileInputFormat; 
import org.apache.hadoop.mapred.FileOutputFormat; 
import org.apache.hadoop.mapred.JobClient; 
import org.apache.hadoop.mapred.JobConf;
public class CleaningZ{
public static void main(String[] args) throws IOException 
{    
if (args.length != 2) 
{      System.err.println("Usage: Cleaning <input path> <output path>");      
System.exit(-1);    
}

// Data cleaning job        
JobConf conf = new JobConf(CleaningZ.class);    
conf.setJobName("Data Cleaning");
    
// Add input format and output format
FileInputFormat.addInputPath(conf, new Path(args[0]));    
FileOutputFormat.setOutputPath(conf, new Path(args[1]));  

// Set mapper reducer class      
conf.setMapperClass(CleaningZMapper.class);    
conf.setReducerClass(CleaningZReducer.class);
conf.setOutputKeyClass(Text.class);    
conf.setOutputValueClass(IntWritable.class);
conf.setNumReduceTasks(1);
    
JobClient.runJob(conf);  
}
 }

