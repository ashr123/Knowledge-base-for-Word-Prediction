package il.co.dsp211.assignment2.steps.step1.jobs;

import il.co.dsp211.assignment2.steps.utils.StringStringDoubleTriple;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Job5Sort
{
	public static class CastlingMapper extends Mapper<Text, DoubleWritable, StringStringDoubleTriple, Text>
	{
		/**
		 * @param key     ⟨⟨w<sub>1</sub>, w<sub>2</sub>, w<sub>3</sub>⟩,
		 * @param value   p⟩
		 * @param context ⟨⟨w<sub>1</sub>, w<sub>2</sub>, p⟩, w<sub>3</sub>⟩
		 */
		@Override
		protected void map(Text key, DoubleWritable value, Context context) throws IOException, InterruptedException
		{
			final String[] words = key.toString().split(" ");
			context.write(new StringStringDoubleTriple(words[0], words[1], value.get()), new Text(words[2]));
		}
	}

	public static class FinisherReducer extends Reducer<StringStringDoubleTriple, Text, Text, DoubleWritable>
	{
		/**
		 * @param key     ⟨⟨w<sub>1</sub>, w<sub>2</sub>, p⟩,
		 * @param values  [w<sub>3</sub>]⟩
		 * @param context ⟨⟨w<sub>1</sub>, w<sub>2</sub>, w<sub>3</sub>⟩, p⟩ sorted as requested
		 * @see StringStringDoubleTriple#compareTo(StringStringDoubleTriple)
		 */
		@Override
		protected void reduce(StringStringDoubleTriple key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			for (final Text w3 : values)
				context.write(new Text(key.getString1() + " " + key.getString2() + " " + w3.toString()), new DoubleWritable(key.getProb()));
		}
	}
}
