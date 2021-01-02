package il.co.dsp211.assignment2.steps.step1.jobs;

import il.co.dsp211.assignment2.steps.utils.StringStringDoubleTriple;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Job5Sort
{
	public static class CastlingMapper extends Mapper<LongWritable, Text, StringStringDoubleTriple, Text>
	{
		/**
		 * @param key     position in file
		 * @param value   ⟨⟨w₁, w₂, w₃⟩, p⟩
		 * @param context ⟨⟨w₁, w₂, p⟩, w₃⟩
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			final String[] split = value.toString().split("\t");
			final String[] words = split[0].split(" ");
			context.write(new StringStringDoubleTriple(words[0], words[1], Double.parseDouble(split[1])), new Text(words[2]));
		}
	}

	public static class FinisherReducer extends Reducer<StringStringDoubleTriple, Text, Text, DoubleWritable>
	{
		/**
		 * @param key     ⟨⟨w₁, w₂, p⟩,
		 * @param values  [w₃]⟩
		 * @param context ⟨⟨w₁, w₂, w₃⟩, p⟩ sorted as requested
		 */
		@Override
		protected void reduce(StringStringDoubleTriple key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			for (final Text w3 : values)
				context.write(new Text(key.getString1() + " " + key.getString2() + " " + w3.toString()), new DoubleWritable(key.getProb()));
		}
	}

	public static class SameReducerPartitioner extends Partitioner<StringStringDoubleTriple, Text>
	{
		/**
		 * Ensures that all records wil arrive to the same reducer
		 *
		 * @param key           the key to be partitioned.
		 * @param value         the entry value.
		 * @param numPartitions the total number of partitions.
		 * @return the partition number for the <code>key</code>.
		 */
		@Override
		public int getPartition(StringStringDoubleTriple key, Text value, int numPartitions)
		{
			return 0;
		}
	}
}
