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
import java.util.Collections;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Job1DivideCorpus
{
	public static class DividerMapper extends Mapper<LongWritable, Text, Text, BooleanLongPair>
	{
		private Pattern pattern;

		@Override
		protected void setup(Context context)
		{
			pattern = Pattern.compile("(?<words>" + String.join("+ ", Collections.nCopies(3, context.getConfiguration().get("singleLetterInAWordRegex"))) + "+)\\t\\d{4}\\t(?<occurrences>\\d+).*");
		}

		/**
		 * @param key     ⟨line number,
		 * @param value   ⟨⟨w<sub>1</sub>, w<sub>2</sub>, w<sub>3</sub>⟩, year, occurrences in this year, pages in this year, books in this year⟩⟩
		 * @param context ⟨⟨w<sub>1</sub>, w<sub>2</sub>, w<sub>3</sub>⟩, ⟨group, occurrences in this year⟩⟩
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			final Matcher matcher = pattern.matcher(value.toString());
			if (matcher.matches())
				context.write(new Text(matcher.group("words")), new BooleanLongPair(key.get() % 2 == 1, Long.parseLong(matcher.group("occurrences"))));
		}
	}

	public static class CountCombiner extends Reducer<Text, BooleanLongPair, Text, BooleanLongPair>
	{
		/**
		 * @param key     ⟨⟨w<sub>1</sub>, w<sub>2</sub>, w<sub>3</sub>⟩,
		 * @param values  [⟨group, occurrences in this year⟩]⟩
		 * @param context ⟨⟨w<sub>1</sub>, w<sub>2</sub>, w<sub>3</sub>⟩, ⟨{@code true}, partial r<sub>0</sub>⟩⟩, ⟨⟨w<sub>1</sub>, w<sub>2</sub>, w<sub>3</sub>⟩, ⟨{@code false}, partial r<sub>1</sub>⟩⟩
		 */
		@Override
		protected void reduce(Text key, Iterable<BooleanLongPair> values, Context context) throws IOException, InterruptedException
		{
			final Map<Boolean, Long> map = StreamSupport.stream(values.spliterator(), false)
					.collect(Collectors.groupingBy(BooleanLongPair::isKey, Collectors.summingLong(BooleanLongPair::getValue)));
			if (map.containsKey(true))
				context.write(key, new BooleanLongPair(true, map.get(true)));
			if (map.containsKey(false))
				context.write(key, new BooleanLongPair(false, map.get(false)));
		}
	}

	public static class CountAndZipReducer extends Reducer<Text, BooleanLongPair, Text, LongLongPair>
	{
		private Counter counter;

		@Override
		protected void setup(Context context)
		{
			counter = context.getCounter(NCounter.N_COUNTER);
		}

		/**
		 * @param key     ⟨⟨w<sub>1</sub>, w<sub>2</sub>, w<sub>3</sub>⟩,
		 * @param values  [⟨group, partial r<sub>group</sub>⟩]⟩
		 * @param context ⟨⟨w<sub>1</sub>, w<sub>2</sub>, w<sub>3</sub>⟩, ⟨r<sub>0</sub>, r<sub>1</sub>⟩⟩
		 */
		@Override
		protected void reduce(Text key, Iterable<BooleanLongPair> values, Context context) throws IOException, InterruptedException
		{
			final Map<Boolean, Long> map = StreamSupport.stream(values.spliterator(), false)
					.collect(Collectors.groupingBy(BooleanLongPair::isKey, Collectors.summingLong(BooleanLongPair::getValue)));
			context.write(key, new LongLongPair(map.containsKey(true) ? map.get(true) : 0, map.containsKey(false) ? map.get(false) : 0));
			counter.increment(map.values().stream()
					.mapToLong(Long::longValue)
					.sum());
//			counter.increment((map.containsKey(true) ? map.get(true) : 0) + (map.containsKey(false) ? map.get(false) : 0));
		}
	}
}
