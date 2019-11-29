import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.AbstractCollection;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MinHash
{
  private static int p = 5000011; 
  public static class Kshingles extends Mapper<Object, Text, IntWritable,IntWritable>
{
    private Text word = new Text();
    private int val = 1;
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    char[] line = value.toString().toCharArray();
    for(char le : line){
        if(result.size() != k){
	    result.add(le);
	    }else{
		     
		     //context.write(new IntWritable(result.toString().hashCode()), new IntWritable(val));
		     result.clear();
                     }
    }
  }
}
  // REDUCE
  public static class IntReducer extends Reducer<IntWritable, IntWritable,IntWritable,IntWritable> {

    private IntWritable result = new IntWritable();
    public void reduce(IntWritable key,Iterable<IntWritable> values,  Context context) throws IOException, InterruptedException {
        int sum = 0;
	for(IntWritable val : values)
	    sum+=val.get();
        result.set(sum);
        context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "kshingles");
    job.setJarByClass(MinHash.class);

    job.setMapperClass(Kshingles.class); // MAP
    job.setCombinerClass(IntReducer.class);
    job.setReducerClass(IntReducer.class); // REDUCE
    
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

