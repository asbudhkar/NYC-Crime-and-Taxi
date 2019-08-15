import java.io.IOException;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapred.FileInputFormat; 
import org.apache.hadoop.mapred.FileOutputFormat; 
import org.apache.hadoop.mapred.JobClient; 
import org.apache.hadoop.mapred.JobConf;
public class Profiling{
public static void main(String[] args) throws IOException 
{    
if (args.length != 2) 
{      System.err.println("Usage: Profiling <input path> <output path>");      
System.exit(-1);    
}

// set Job        
JobConf conf = new JobConf(Profiling.class);    
conf.setJobName("Data Profiling");
    
// Add paths
FileInputFormat.addInputPath(conf, new Path(args[0]));    
FileOutputFormat.setOutputPath(conf, new Path(args[1]));        

// Set mapper class 
conf.setMapperClass(ProfilingMapper.class);    
conf.setReducerClass(ProfilingReducer.class);

// Set output class 
conf.setOutputKeyClass(Text.class);    
conf.setOutputValueClass(IntWritable.class);
conf.setNumReduceTasks(1);
    
JobClient.runJob(conf);  
}
 }

