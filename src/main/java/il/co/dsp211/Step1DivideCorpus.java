package il.co.dsp211;

import il.co.dsp211.utils.BooleanLongPair;
import il.co.dsp211.utils.LongLongPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Step1DivideCorpus
{
	public static void main(String... args) throws IOException, ClassNotFoundException, InterruptedException
	{
		Job job = Job.getInstance(new Configuration()); // TODO check purpose
		job.setJarByClass(Step1DivideCorpus.class); // TODO check purpose

		job.setMapperClass(Divider.class);
//		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BooleanLongPair.class);

		job.setReducerClass(CountAndZip.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongLongPair.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setPartitionerClass(HashPartitioner.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class Divider extends Mapper<LongWritable, Text, Text/*3-gram*/, BooleanLongPair/*<true|false, occurrences>*/>
	{
		@Override
		protected void map(LongWritable key, Text value/*<3-gram	year	occurrences	pages	books>*/, Context context) throws IOException, InterruptedException
		{
			String[] components = value.toString().split("\t"); // TODO check about regex
			context.write(new Text(components[0]), new BooleanLongPair(key.get() % 2 == 0, Long.parseLong(components[2])));
		}
	}

	public static class CountAndZip extends Reducer<Text/*3-gram*/, BooleanLongPair/*<true|false, occurrences>*/, Text, LongLongPair>
	{
		@Override
		protected void reduce(Text triGram, Iterable<BooleanLongPair> values, Context context) throws IOException, InterruptedException
		{
			final Map<Boolean, Long> map = StreamSupport.stream(values.spliterator(), true)
					.collect(Collectors.groupingBy(BooleanLongPair::isKey, Collectors.counting()));
			context.write(triGram, new LongLongPair(map.get(true), map.get(false)));
		}
	}

	/**
	 * As in {@link org.apache.hadoop.mapreduce.lib.partition.HashPartitioner#getPartition(java.lang.Object, java.lang.Object, int)}, #official😎
	 */
	public static class HashPartitioner extends Partitioner<Text/*3-gram*/, BooleanLongPair/*<true|false, occurrences>*/>
	{
		@Override
		public int getPartition(Text text, BooleanLongPair booleanLongPair, int numPartitions)
		{
			return (text.hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}
}
