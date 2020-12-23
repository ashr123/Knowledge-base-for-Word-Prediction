package il.co.dsp211;

import il.co.dsp211.utils.BooleanLongPair;
import il.co.dsp211.utils.LongLongPair;
import il.co.dsp211.utils.NCounter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;
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

		job.setCombinerClass(Count.class);

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

	public static class Count extends Reducer<Text/*3-gram*/, BooleanLongPair/*<true|false, occurrences>*/, Text/*3-gram*/, BooleanLongPair/*<true|false, occurrences>*/>
	{
		@Override
		protected void reduce(Text triGram, Iterable<BooleanLongPair> values, Context context) throws IOException, InterruptedException
		{
			final Map<Boolean, Long> map = StreamSupport.stream(values.spliterator(), true)
					.collect(Collectors.groupingBy(BooleanLongPair::isKey, Collectors.counting()));
			context.write(triGram, new BooleanLongPair(true, map.get(true)));
			context.write(triGram, new BooleanLongPair(false, map.get(false)));
		}
	}

	public static class CountAndZip extends Reducer<Text/*3-gram*/, BooleanLongPair/*<true|false, occurrences>*/, Text/*3-gram*/, LongLongPair/*<first group's occurrences, second group's occurrences>*/>
	{
		private Counter counter;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			counter = context.getCounter(NCounter.N_COUNTER); // TODO check if a counter need to created or if it created automatically
		}

		@Override
		protected void reduce(Text triGram, Iterable<BooleanLongPair> values, Context context) throws IOException, InterruptedException
		{
			final Map<Boolean, Long> map = StreamSupport.stream(values.spliterator(), true)
					.collect(Collectors.groupingBy(BooleanLongPair::isKey, Collectors.counting()));
			context.write(triGram, new LongLongPair(map.get(true), map.get(false)));
			counter.increment(map.values().stream().mapToLong(Long::longValue).sum());
		}
	}
}
