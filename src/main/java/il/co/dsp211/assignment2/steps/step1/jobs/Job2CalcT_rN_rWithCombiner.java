package il.co.dsp211.assignment2.steps.step1.jobs;

import il.co.dsp211.assignment2.steps.utils.BooleanLongPair;
import il.co.dsp211.assignment2.steps.utils.LongLongPair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.stream.StreamSupport;

public class Job2CalcT_rN_rWithCombiner
{
	public static class SplitRsMapper extends Mapper<Text, LongLongPair, BooleanLongPair, LongLongPair>
	{
		/**
		 * @param key     ⟨⟨w₁, w₂, w₃⟩,
		 * @param value   ⟨r₀, r₁⟩⟩
		 * @param context ⟨⟨group, r⟩, r in <b>other</b> group⟩
		 */
		@Override
		protected void map(Text key, LongLongPair value, Context context) throws IOException, InterruptedException
		{
			if (value.getKey() != 0)
				context.write(new BooleanLongPair(true, value.getKey()), new LongLongPair(value.getValue(), 1));
			if (value.getValue() != 0)
				context.write(new BooleanLongPair(false, value.getValue()), new LongLongPair(value.getKey(), 1));
		}
	}

	public static class CalcT_rN_rCombinerAndReducer extends Reducer<BooleanLongPair, LongLongPair, BooleanLongPair, LongLongPair>
	{
		/**
		 * @param key     ⟨⟨group, r⟩,
		 * @param values  [⟨T_r, N_r⟩ (partial)]⟩
		 * @param context ⟨⟨group, r⟩, ⟨T_r, N_r⟩⟩
		 */
		@Override
		protected void reduce(BooleanLongPair key, Iterable<LongLongPair> values, Context context) throws IOException, InterruptedException
		{
			context.write(key,
					StreamSupport.stream(values.spliterator(), false)
							.reduce(new LongLongPair(0, 0),
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
