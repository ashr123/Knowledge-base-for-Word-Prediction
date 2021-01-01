package il.co.dsp211.assignment2.steps.step1.jobs;

import il.co.dsp211.assignment2.steps.utils.BooleanLongPair;
import il.co.dsp211.assignment2.steps.utils.LongLongPair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.stream.StreamSupport;

public class Job2CalcT_rN_r
{
	public static class SplitRsMapper extends Mapper<LongWritable, Text, BooleanLongPair, LongWritable>
	{
		/**
		 * @param key     position in file
		 * @param value   ⟨⟨w₁, w₂, w₃⟩, ⟨r₀, r₁⟩⟩
		 * @param context ⟨⟨group, r⟩, r in <b>other</b> group⟩
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			final LongLongPair value1 = LongLongPair.of(value.toString().split("\t")[1]);
			if (value1.getKey() != 0)
				context.write(new BooleanLongPair(true, value1.getKey()), new LongWritable(value1.getValue()));
			if (value1.getValue() != 0)
				context.write(new BooleanLongPair(false, value1.getValue()), new LongWritable(value1.getKey()));
		}
	}

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
////
////			int sum = 0, counter = 0;
////			for (final LongWritable longWritable : values)
////			{
////				sum += longWritable.get();
////				counter++;
////			}
////			context.write(key, new LongLongPair(sum, counter));
////		}
//	}

	public static class CalcT_rN_rReducer extends Reducer<BooleanLongPair, LongWritable, BooleanLongPair, LongLongPair>
	{
		/**
		 * @param key     ⟨⟨group, r⟩,
		 * @param values  [r in <b>other</b> group]⟩
		 * @param context ⟨⟨group, r⟩, ⟨T_r, N_r⟩⟩
		 */
		@Override
		protected void reduce(BooleanLongPair key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
		{
			context.write(key,
					StreamSupport.stream(values.spliterator(), false)
							.reduce(new LongLongPair(0, 0),
									(longLongPair, longWritable) -> new LongLongPair(longLongPair.getKey() + longWritable.get(), longLongPair.getValue() + 1),
									(longLongPair, longLongPair2) -> new LongLongPair(longLongPair.getKey() + longLongPair2.getKey(), longLongPair.getValue() + longLongPair2.getValue())));


//			int sum = 0, counter = 0;
//			for (final LongWritable longWritable : values)
//			{
//				sum += longWritable.get();
//				counter++;
//			}
//			context.write(key, new LongLongPair(sum, counter));
		}
	}
}