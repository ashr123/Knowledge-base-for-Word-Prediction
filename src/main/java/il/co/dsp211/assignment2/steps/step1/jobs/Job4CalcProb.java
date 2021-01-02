package il.co.dsp211.assignment2.steps.step1.jobs;

import il.co.dsp211.assignment2.steps.utils.LongLongPair;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.StreamSupport;

public class Job4CalcProb
{
	public static class IdentityMapper extends Mapper<LongWritable, Text, Text, LongLongPair>
	{
		/**
		 * Identity function
		 *
		 * @param key     position in file
		 * @param value   ⟨⟨w₁, w₂, w₃⟩, ⟨T_r, N_r⟩⟩
		 * @param context ⟨⟨w₁, w₂, w₃⟩, ⟨T_r, N_r⟩⟩
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			final String[] split = value.toString().split("\t");
			context.write(new Text(split[0]), LongLongPair.of(split[1]));
		}
	}

	public static class CalcProbReducer extends Reducer<Text, LongLongPair, Text, DoubleWritable>
	{
		private long N;

		@Override
		protected void setup(Context context)
		{
			N = context.getConfiguration().getLong("N", -1);
		}

		/**
		 * @param key     ⟨⟨w₁, w₂, w₃⟩,
		 * @param values  [⟨T_r, N_r⟩]⟩ (1-2 pairs)
		 * @param context ⟨⟨w₁, w₂, w₃⟩, p⟩
		 */
		@Override
		protected void reduce(Text key, Iterable<LongLongPair> values, Context context) throws IOException, InterruptedException
		{
			final LongLongPair preP = StreamSupport.stream(values.spliterator(), false)
					.reduce(new LongLongPair(0, 0),
							(longLongPair, longLongPair2) -> new LongLongPair(longLongPair.getKey() + longLongPair2.getKey(), longLongPair.getValue() + longLongPair2.getValue()));
			context.write(key, new DoubleWritable(1.0 * preP.getKey() / (N * preP.getValue())));
		}
	}
}
