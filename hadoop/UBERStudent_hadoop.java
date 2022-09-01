//구역과 날짜에 따른 여행의 수와 교통수단 이용수단의 횟수를 재는 코드
import java.io.IOException;
import java.util.*;
import java.text.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class UBERStudent20180937{

	
	public static class Map extends Mapper<LongWritable, Text , Text, Text>
	{
		private Text word = new Text();
		private Text word2 = new Text();
			

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString(); 
			StringTokenizer tokenizer = new StringTokenizer(line,","); 
			String str[] = new String[10];
		
			String str1 = "";	
			String str2 = "";	
			
			int i = 0;
			
			while(tokenizer.hasMoreTokens()){
				str[i] = tokenizer.nextToken();
				i++;
			}
			str1 = str[0] + "," + returnDay(str[1]);
			str2 = str[3] + "," + str[2];
			word.set(str1);
			word2.set(str2);
			context.write(word,word2);			
		}
	}


	public static class Reduce extends Reducer<Text , Text, Text, Text>
	{
		private Text sumWritable = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			long sum = 0;
			long sum2 = 0;
			String num1 = null;
			for(Text val : values){
				String str = val.toString();
				StringTokenizer tokenizer = new StringTokenizer(str, ","); 
				
				sum += Long.parseLong(tokenizer.nextToken());
				sum2 += Long.parseLong(tokenizer.nextToken());		
			}
			num1 =  Long.toString(sum) + "," + Long.toString(sum2);
			sumWritable.set(num1);
			context.write(key, sumWritable);
		}
	}
	
	public static void main(String[] args) throws Exception{
	
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 2)
		{
			System.err.println("Usage : uberstudent20180937 <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "uberstudent20180937");
	
		job.setJarByClass(UBERStudent20180937.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);	
	}

	public static String returnDay(String date){ 
		SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy"); 
		String[] week = {"SUN", "MON", "TUE", "WED", "THR", "FRI", "SAT"}; 
		Calendar cal = Calendar.getInstance(); 
		Date getDate; 
		try { 
			getDate = format.parse(date); 
			cal.setTime(getDate); 
			int w = cal.get(Calendar.DAY_OF_WEEK) - 1; 
			return week[w];
		} catch (ParseException e) { 
			e.printStackTrace(); 
		} catch (Exception e) { 
			e.printStackTrace(); 
		} 
		return "0";
	}
}