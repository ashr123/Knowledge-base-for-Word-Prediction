package il.co.dsp211.assignment2.steps.step1.jobs;

import il.co.dsp211.assignment2.steps.utils.BooleanLongPair;
import il.co.dsp211.assignment2.steps.utils.LongLongPair;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.stream.StreamSupport;

public class Step3CalcProb
{
	public static class MapperImpl extends Mapper<LongWritable, Text, LongWritable, LongLongPair>
	{
		/**
		 * @param key     position in file
		 * @param value   ⟨⟨group, r⟩, ⟨T_r, N_r⟩⟩
		 * @param context ⟨r, ⟨T_r, N_r⟩⟩
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			final String[] split = value.toString().split("\t");
			context.write(new LongWritable(BooleanLongPair.of(split[0]).getValue()), LongLongPair.of(split[1]));
		}
	}

	public static class CalcProbReducer extends Reducer<LongWritable, LongLongPair, LongWritable, DoubleWritable>
	{
		private long N;

		@Override
		protected void setup(Context context)
		{
			N = context.getConfiguration().getLong("N", -1);
		}

		/**
		 * @param key     ⟨r,
		 * @param values  [⟨T_r, N_r⟩]⟩
		 * @param context ⟨r, p⟩
		 */
		@Override
		protected void reduce(LongWritable key, Iterable<LongLongPair> values, Context context) throws IOException, InterruptedException
		{
			final LongLongPair preP = StreamSupport.stream(values.spliterator(), true)
					.reduce(new LongLongPair(0, 0),
							(longLongPair, longLongPair2) -> new LongLongPair(longLongPair.getKey() + longLongPair2.getKey(), longLongPair.getValue() + longLongPair2.getValue()));
			context.write(key, new DoubleWritable(1.0 * preP.getKey() / (N * preP.getValue())));
		}
	}
}
