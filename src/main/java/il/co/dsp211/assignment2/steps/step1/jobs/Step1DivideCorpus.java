package il.co.dsp211.assignment2.steps.step1.jobs;

import il.co.dsp211.assignment2.steps.utils.BooleanLongPair;
import il.co.dsp211.assignment2.steps.utils.LongLongPair;
import il.co.dsp211.assignment2.steps.utils.NCounter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Step1DivideCorpus
{
	public static class Divider extends Mapper<LongWritable, Text, Text, BooleanLongPair>
	{
		/**
		 * @param key     ⟨line number,
		 * @param value   ⟨⟨w₁, w₂, w₃⟩, year, occurrences in this year, pages in this year, books in this year⟩⟩
		 * @param context ⟨⟨w₁, w₂, w₃⟩, ⟨group, occurrences in this year⟩⟩
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			final String[] record = value.toString().split("\t"); // TODO check about regex
			context.write(new Text(record[0]), new BooleanLongPair(key.get() % 2 == 0, Long.parseLong(record[2])));
		}
	}

	public static class Count extends Reducer<Text, BooleanLongPair, Text, BooleanLongPair>
	{
		/**
		 * @param key     ⟨⟨w₁, w₂, w₃⟩,
		 * @param values  [⟨group, occurrences⟩]⟩
		 * @param context ⟨⟨w₁, w₂, w₃⟩, ⟨r₀, r₁⟩⟩
		 */
		@Override
		protected void reduce(Text key, Iterable<BooleanLongPair> values, Context context) throws IOException, InterruptedException
		{
			final Map<Boolean, Long> map = StreamSupport.stream(values.spliterator(), true)
					.collect(Collectors.groupingBy(BooleanLongPair::isKey, Collectors.summingLong(BooleanLongPair::getValue)));
			if (map.containsKey(true))
				context.write(key, new BooleanLongPair(true, map.get(true)));
			if (map.containsKey(false))
				context.write(key, new BooleanLongPair(false, map.get(false)));
		}
	}

	public static class CountAndZip extends Reducer<Text, BooleanLongPair, Text, LongLongPair>
	{
		private Counter counter;

		@Override
		protected void setup(Context context)
		{
			counter = context.getCounter(NCounter.N_COUNTER);
		}

		/**
		 * @param key     ⟨⟨w₁, w₂, w₃⟩,
		 * @param values  [⟨group, occurrences⟩]⟩
		 * @param context ⟨⟨w₁, w₂, w₃⟩, ⟨r₀, r₁⟩⟩
		 */
		@Override
		protected void reduce(Text key, Iterable<BooleanLongPair> values, Context context) throws IOException, InterruptedException
		{
			final Map<Boolean, Long> map = StreamSupport.stream(values.spliterator(), true)
					.collect(Collectors.groupingBy(BooleanLongPair::isKey, Collectors.summingLong(BooleanLongPair::getValue)));
			context.write(key, new LongLongPair(map.containsKey(true) ? map.get(true) : 0, map.containsKey(false) ? map.get(false) : 0));
			counter.increment(map.values().parallelStream()
					.mapToLong(Long::longValue)
					.sum());
//			counter.increment(map.get(true) + map.get(false));
		}
	}
}
