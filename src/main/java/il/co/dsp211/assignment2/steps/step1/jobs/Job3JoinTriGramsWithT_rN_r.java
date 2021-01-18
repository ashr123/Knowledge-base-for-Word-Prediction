package il.co.dsp211.assignment2.steps.step1.jobs;

import il.co.dsp211.assignment2.steps.utils.BooleanBooleanLongTriple;
import il.co.dsp211.assignment2.steps.utils.BooleanLongPair;
import il.co.dsp211.assignment2.steps.utils.LongLongPair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;

public class Job3JoinTriGramsWithT_rN_r
{
	public static class TriGramMapper extends Mapper<Text, LongLongPair, BooleanBooleanLongTriple, Text>
	{
		/**
		 * @param key     ⟨⟨w<sub>1</sub>, w<sub>2</sub>, w<sub>3</sub>⟩,
		 * @param value   ⟨r<sub>0</sub>, r<sub>1</sub>⟩⟩
		 * @param context ⟨⟨{@code true}, {@code true}, r<sub>0</sub>⟩, ⟨w<sub>1</sub>, w<sub>2</sub>, w<sub>3</sub>⟩⟩, ⟨⟨{@code true}, {@code false}, r<sub>1</sub>⟩, ⟨w<sub>1</sub>, w<sub>2</sub>, w<sub>3</sub>⟩⟩
		 */
		@Override
		protected void map(Text key, LongLongPair value, Context context) throws IOException, InterruptedException
		{
			if (value.getKey() != 0)
				context.write(new BooleanBooleanLongTriple(true, true, value.getKey()), new Text(key));
			if (value.getValue() != 0)
				context.write(new BooleanBooleanLongTriple(true, false, value.getValue()), new Text(key));
		}
	}

	public static class T_rN_rMapper extends Mapper<BooleanLongPair, LongLongPair, BooleanBooleanLongTriple, Text>
	{
		/**
		 * @param key     ⟨⟨group, r⟩,
		 * @param value   ⟨T<sub>r</sub>, N<sub>r</sub>⟩⟩
		 * @param context ⟨⟨{@code false}, group, r⟩, ⟨T<sub>r</sub>, N<sub>r</sub>⟩ (as {@link Text})⟩
		 */
		@Override
		protected void map(BooleanLongPair key, LongLongPair value, Context context) throws IOException, InterruptedException
		{
			context.write(new BooleanBooleanLongTriple(false, key.isKey(), key.getValue()), new Text(value.toString()));
		}
	}

	public static class JoinReducer extends Reducer<BooleanBooleanLongTriple, Text, Text, LongLongPair>
	{
		private LongLongPair currentT_rN_r;
		private boolean currentIsGroup1;
		private long currentR;

		/**
		 * @param key     ⟨⟨isTriGram, group, r⟩,
		 * @param values  [⟨T<sub>r</sub>, N<sub>r</sub>⟩ (1 pair as {@link Text}) | ...⟨w<sub>1</sub>, w<sub>2</sub>, w<sub>3</sub>⟩]⟩
		 * @param context ⟨⟨w<sub>1</sub>, w<sub>2</sub>, w<sub>3</sub>⟩, ⟨T<sub>r</sub>, N<sub>r</sub>⟩⟩
		 * @see BooleanBooleanLongTriple#compareTo(BooleanBooleanLongTriple)
		 */
		@Override
		protected void reduce(BooleanBooleanLongTriple key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			if (key.isTriGram()) // value is [...⟨w<sub>1</sub>, w<sub>2</sub>, w<sub>3</sub>⟩]
				if (currentIsGroup1 == key.isGroup1() && currentR == key.getR())
					for (final Text triGram : values)
						context.write(triGram, currentT_rN_r);
				else
					throw new IllegalStateException("Got TriGram-record with isGroup1: " + key.isGroup1() + " and r: " + key.getR() + ", but currently have isGroup1: " + currentIsGroup1 + " and r: " + currentR);
			else // value is [⟨T_r, N_r⟩ (1 pair as Text)]
			{
				final Iterator<Text> iterator = values.iterator();
				if (iterator.hasNext())
					currentT_rN_r = LongLongPair.of(iterator.next().toString());
				if (iterator.hasNext())
					throw new IllegalStateException("Got more then 1 pair of ⟨T_r, N_r⟩");

				currentR = key.getR();
				currentIsGroup1 = key.isGroup1();
			}
		}
	}

	public static class JoinPartitioner extends Partitioner<BooleanBooleanLongTriple, Text>
	{
		/**
		 * Ensures that record with with same {@code r} and group are directed to the same reducer
		 *
		 * @param key           the key to be partitioned.
		 * @param value         the entry value.
		 * @param numPartitions the total number of partitions.
		 * @return the partition number for the <code>key</code>.
		 */
		@Override
		public int getPartition(BooleanBooleanLongTriple key, Text value, int numPartitions)
		{
			return (Objects.hash(key.isGroup1(), key.getR()) & Integer.MAX_VALUE) % numPartitions;
		}
	}
}
