package il.co.dsp211;

import il.co.dsp211.utils.BooleanLongPair;
import il.co.dsp211.utils.LongLongPair;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class Step4JoinTriGramProb
{
	public static class MapperTriGram extends Mapper<LongWritable, Text, BooleanLongPair, Text>
	{
		/**
		 * @param key     position in file
		 * @param value   ⟨⟨w₁, w₂, w₃⟩, ⟨r₀₁, r₁₀⟩⟩
		 * @param context ⟨⟨{@code true}, r₀ + r₁⟩, ⟨w₁, w₂, w₃⟩⟩
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			final String[] split = value.toString().split("\t");
			LongLongPair value1 = LongLongPair.of(split[1]);
			context.write(new BooleanLongPair(true, value1.getKey() + value1.getValue()), new Text(split[0]));
		}
	}

	public static class MapperProb extends Mapper<LongWritable, Text, BooleanLongPair, Text>
	{
		/**
		 * @param key     position in file
		 * @param value   ⟨r, p⟩
		 * @param context ⟨⟨{@code false}, r⟩, p (as {@link Text})⟩
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			final String[] split = value.toString().split("\t");
			context.write(new BooleanLongPair(false, Long.parseLong(split[0])), new Text(split[1]));
		}
	}

	public static class JoinerReducer extends Reducer<BooleanLongPair, Text, Text, DoubleWritable>
	{
		private final DoubleWritable currentP = new DoubleWritable();
		private long currentR = 0;

		/**
		 * counting on proper sorting of {@link BooleanLongPair}: primary key: the {@code BooleanLongPair#value} (i.e r),
		 * secondary field {@code BooleanLongPair#key} (i.e isTriGram) in such a way that first we'll get p and only then its corresponding tri-grams, per r
		 *
		 * @param key     ⟨⟨isTriGram, r⟩,
		 * @param values  [p|⟨w₁, w₂, w₃⟩]⟩
		 * @param context ⟨⟨w₁, w₂, w₃⟩, p⟩
		 */
		@Override
		protected void reduce(BooleanLongPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			if (key.isKey()) // value is [⟨w₁, w₂, w₃⟩]
				for (final Text text : values)
					context.write(text, currentP);
			else // value is [p] with 1 element
				if (key.getValue() > currentR)
				{
					currentR = key.getValue();
					final Iterator<Text> iterator = values.iterator();
					if (iterator.hasNext())
						currentP.set(Double.parseDouble(iterator.next().toString()));
					else
						System.err.println("No P for r: " + key.getValue());
				}
		}
	}

	public static class JoinPartitioner extends Partitioner<BooleanLongPair, Text>
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
