import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class IMDBStudent20180937{
	public static class Map extends Mapper<LongWritable , Text, Text, LongWritable>
	{
		private final static LongWritable one = new LongWritable(1); 
		private Text word = new Text();
		String str=null;
		int i = 0;	
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line,"::"); 
			
			while(tokenizer.hasMoreTokens()){
				str = null; 
				str = tokenizer.nextToken();
			}
			StringTokenizer tokenizer1 = new StringTokenizer(str,"|");
			while(tokenizer1.hasMoreTokens()){
				word.set(tokenizer1.nextToken()); 
				context.write(word,one);
			}
			
		}
	}


	public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable>
	{
		private LongWritable sumWritable = new LongWritable();
	
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
		{
			long sum = 0;
			for(LongWritable val : values){
				sum += val.get();
			}
			sumWritable.set(sum);
			context.write(key, sumWritable);
		}
	}
	
	public static void main(String[] args) throws Exception{
	
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 2)
		{
			System.err.println("Usage : imdbstudent20180937 <in> <out> ");
			System.exit(2);
		}
		Job job = new Job(conf, "imdbstudent20180937");
	
		job.setJarByClass(IMDBStudent20180937.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
	
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);	
	}
}