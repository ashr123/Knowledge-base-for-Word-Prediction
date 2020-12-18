package il.co.dsp211;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.Spliterators;
import java.util.StringTokenizer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Main
{
	public static void main(String... args) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(Main.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
	{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
//			Stream.of(value.toString().split("[ \t\n\r\f]"))
//					.map(Text::new)
//					.forEach(text ->
//					{
//						try
//						{
//							context.write(text, one);
//						}
//						catch (IOException e)
//						{
//							e.printStackTrace();
//						}
//						catch (InterruptedException e)
//						{
//							e.printStackTrace();
//						}
//					});

//			StringTokenizer itr = new StringTokenizer(value.toString());
			for (String text : (Iterable<String>) new StringTokenizer(value.toString()).asIterator())
			{
				word.set(text);
				context.write(word, one);
			}
//			while (itr.hasMoreTokens())
//			{
//				word.set(itr.nextToken());
//				context.write(word, one);
//			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			result.set(StreamSupport.stream(values.spliterator(), true)
					.mapToInt(IntWritable::get)
					.sum());

//			int sum = 0;
//
//			for (IntWritable val : values)
//			{
//				sum += val.get();
//			}
//			result.set(sum);
			context.write(key, result);
		}
	}
}
