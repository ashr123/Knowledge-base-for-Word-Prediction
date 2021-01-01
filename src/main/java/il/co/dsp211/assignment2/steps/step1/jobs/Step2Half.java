package il.co.dsp211.assignment2.steps.step1.jobs;

import il.co.dsp211.assignment2.steps.utils.BooleanBooleanLongTriple;
import il.co.dsp211.assignment2.steps.utils.BooleanLongPair;
import il.co.dsp211.assignment2.steps.utils.LongLongPair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * ⟨⟨w₁, w₂, w₃⟩, ⟨r₀, r₁⟩⟩
 * ⟨⟨group, r⟩, ⟨T_r, N_r⟩⟩
 */
public class Step2Half //TODO edit EMR
{
	public static class MapperTriGram extends Mapper<LongWritable, Text, BooleanBooleanLongTriple, Text>
	{
		/**
		 * @param key     position in file
		 * @param value   ⟨⟨w₁, w₂, w₃⟩, ⟨r₀, r₁⟩⟩
		 * @param context ⟨⟨{@code true}, {@code true}, r₀⟩, ⟨w₁, w₂, w₃⟩⟩, ⟨⟨{@code true}, {@code false}, r₁⟩, ⟨w₁, w₂, w₃⟩⟩
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			final String[] split = value.toString().split("\t");
			final LongLongPair value1 = LongLongPair.of(split[1]);
			if (value1.getKey() != 0)
				context.write(new BooleanBooleanLongTriple(true, true, value1.getKey()), new Text(split[0]));
			if (value1.getValue() != 0)
				context.write(new BooleanBooleanLongTriple(true, false, value1.getValue()), new Text(split[0]));
		}
	}

	public static class MapperProb extends Mapper<LongWritable, Text, BooleanBooleanLongTriple, Text>
	{
		/**
		 * @param key     position in file
		 * @param value   ⟨⟨group, r⟩, ⟨T_r, N_r⟩⟩
		 * @param context ⟨⟨{@code false}, group, r⟩, ⟨T_r, N_r⟩ (as {@link Text})⟩ (1-2 pairs at reducer)
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			final String[] split = value.toString().split("\t");
			final BooleanLongPair key1 = BooleanLongPair.of(split[0]);
			context.write(new BooleanBooleanLongTriple(false, key1.isKey(), key1.getValue()), new Text(split[1]));
		}
	}

	public static class JoinerReducer extends Reducer<BooleanBooleanLongTriple, Text, Text, Text>
	{
		private Text currentT_rN_r;
		private boolean currentIsGroup0;
		private long currentR = 0;

		/**
		 *
		 * @param key     ⟨⟨isTriGram, group, r⟩,
		 * @param values  [⟨T_r, N_r⟩ (1 pair) | ...⟨w₁, w₂, w₃⟩]⟩
		 * @param context ⟨⟨w₁, w₂, w₃⟩, ⟨T_r, N_r⟩⟩
		 */
		@Override
		protected void reduce(BooleanBooleanLongTriple key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			if (key.isTriGram()) // value is [⟨w₁, w₂, w₃⟩]
				for (final Text triGram : values)
					context.write(triGram, currentT_rN_r);
			else // value is [p] with 1 element
				if (key.isGroup0() != currentIsGroup0 || key.getR() != currentR)
				{
					currentR = key.getR();
					currentIsGroup0 = key.isGroup0();

					final Iterator<Text> iterator = values.iterator();
					if (iterator.hasNext())
						currentT_rN_r = iterator.next();
				}
		}
	}

	public static class JoinPartitioner extends Partitioner<BooleanLongPair, Text> // TODO check
	{
		/**
		 * Ensures that BooleanLongPair with same {@code r} are directed to the same reducer
		 *
		 * @param key           the key to be partitioned.
		 * @param value         the entry value.
		 * @param numPartitions the total number of partitions.
		 * @return the partition number for the <code>key</code>.
		 */
		@Override
		public int getPartition(BooleanLongPair key, Text value, int numPartitions)
		{
			return (Long.hashCode(key.getValue()) & Integer.MAX_VALUE) % numPartitions;
		}
	}
}
