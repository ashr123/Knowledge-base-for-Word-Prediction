package il.co.dsp211;

import il.co.dsp211.utils.BooleanLongPair;
import il.co.dsp211.utils.LongLongPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;
import java.util.stream.StreamSupport;

public class Step2N_rT_r
{
	public static void main(String... args) throws IOException, ClassNotFoundException, InterruptedException
	{
		Job job = Job.getInstance(new Configuration()); // TODO check purpose
		job.setJarByClass(Step2N_rT_r.class); // TODO check purpose

		job.setMapperClass(CalcThings.class);
//		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapOutputKeyClass(BooleanLongPair.class);
		job.setMapOutputValueClass(LongWritable.class);

//		job.setCombinerClass(Combiner.class);

		job.setReducerClass(T_rN_rReducer.class);
		job.setOutputKeyClass(BooleanLongPair.class);
		job.setOutputValueClass(LongLongPair.class);

		job.setPartitionerClass(HashPartitioner.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class CalcThings extends Mapper<LongWritable, Text, BooleanLongPair, LongWritable>
	{
		/**
		 *
		 * @param key possition in file,
		 * @param value ⟨3-gram, ⟨first group's occurrences, second group's occurrences⟩⟩
		 * @param context ⟨⟨group, occurrences⟩, occurrences in <b>other</b> group⟩
		 */
		@Override
		protected void map(LongWritable key, Text/*LongLongPair*/ value, Context context) throws IOException, InterruptedException
		{
			LongLongPair value1 = LongLongPair.of(value.toString().split("\t")[1]);
			if (value1.getKey() != 0)
				context.write(new BooleanLongPair(true, value1.getKey()), new LongWritable(value1.getValue()));
			if (value1.getValue() != 0)
				context.write(new BooleanLongPair(false, value1.getValue()), new LongWritable(value1.getKey()));
		}
	}

//	//TODO check if fine
//	public static class Combiner extends Reducer<BooleanLongPair, LongWritable, BooleanLongPair, LongLongPair>
//	{
//		@Override
//		protected void reduce(BooleanLongPair key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
//		{
//			context.write(key,
//					StreamSupport.stream(values.spliterator(), true)
//							.reduce(new LongLongPair(0, 0),
//									(longLongPair, longWritable) -> new LongLongPair(longLongPair.getKey() + longWritable.get(), longLongPair.getValue() + 1),
//									(longLongPair, longLongPair2) -> new LongLongPair(longLongPair.getKey() + longLongPair2.getKey(), longLongPair.getValue() + longLongPair2.getValue())));
//
//
////			int sum = 0, counter = 0;
////			for (final LongWritable longWritable : values)
////			{
////				sum += longWritable.get();
////				counter++;
////			}
////			context.write(key, new LongLongPair(sum, counter));
//		}
//	}

	public static class T_rN_rReducer extends Reducer<BooleanLongPair, LongLongPair, BooleanLongPair, LongLongPair>
	{
		/**
		 *
		 * @param key ⟨⟨group, occurrences⟩,
		 * @param values occurrences in <b>other</b> group⟩
		 * @param context ⟨⟨group, occurrences⟩, ⟨T_r, N_r⟩⟩
		 */
		@Override
		protected void reduce(BooleanLongPair key, Iterable<LongLongPair> values, Context context) throws IOException, InterruptedException
		{
			context.write(key,
					StreamSupport.stream(values.spliterator(), true)
							.reduce(new LongLongPair(0, 0),
									(longLongPair, longLongPair2) -> new LongLongPair(longLongPair.getKey() + longLongPair2.getKey(), longLongPair.getValue() + longLongPair2.getValue())));


//			int sum = 0, counter = 0;
//			for (final LongLongPair longLongPair : values)
//			{
//				sum += longLongPair.getKey();
//				counter += longLongPair.getValue();
//			}
//			context.write(key, new LongLongPair(sum, counter));
		}
	}
}
