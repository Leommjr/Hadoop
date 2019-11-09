/**
 * @author Leonardo Marinho de Melo Junior
 * @version NO Tested
 * Trab TEA
 */
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class LikedFilms
{
    public static class FilmMapper extends Mapper<LongWritable, Text, Text, Text>
    {
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	    {
        	    if (key.get() == 0 && value.toString().contains("userId")) {
            		return;
        	    }
		    String record = value.toString();
		    String[] parts = record.split(",");
		    StringBuilder result = new StringBuilder();
		    result.append(parts[0]);//userID
		    result.append(" ");
		    result.append(parts[2]);//ratings
		    context.write(new Text(parts[1]), new Text(result.toString()));
	    }
		    
	}
    public static class FilmReducer extends Reducer<Text, Text, Text, Text>
    {
	    int soma = 0;
	    int total = 0;
	    int max = 0;
	    ArrayList<String> fim = new ArrayList();

	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	    {
		  for (Text val : values){
			   String parts[] = val.toString().split(" ");
		    	   Float rate = new Float(parts[1]);
			   if(rate >=4.0)
			 	  soma += 1;
			   total += 1;
		  }
		 if((total % 2)==0)
			  max = 2;
		 else
			  max = 3;
		 fim.add(key.toString());
		 if(fim.size() == max){
			 String palavras = String.join(" ",fim);
			 context.write(new Text(palavras.toString()),new Text(Integer.toString(soma)));
			 fim.clear();
			 soma = 0;
		}
	    }
    }
   public static void main(String[] args) throws Exception
   {
	 Configuration conf = new Configuration();
	 Job job = Job.getInstance(conf, "liked films");
       	 job.setJarByClass(LikedFilms.class);

	 job.setMapperClass(FilmMapper.class);
	 job.setReducerClass(FilmReducer.class);

	 job.setOutputKeyClass(Text.class);
	 job.setOutputValueClass(Text.class);

	 FileInputFormat.addInputPath(job, new Path(args[0]));
	 FileOutputFormat.setOutputPath(job, new Path(args[1]));
	 System.exit(job.waitForCompletion(true) ? 0 : 1);
   }
}
