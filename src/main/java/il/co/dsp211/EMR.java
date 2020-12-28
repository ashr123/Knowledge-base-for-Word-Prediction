package il.co.dsp211;

import il.co.dsp211.utils.BooleanLongPair;
import il.co.dsp211.utils.LongLongPair;
import il.co.dsp211.utils.NCounter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;

public class EMR
{
	public static void main(String... args) throws IOException, ClassNotFoundException, InterruptedException
	{
//		"s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"
		final Configuration conf = new Configuration();

		System.out.println("Building job 1...");

		Job job1 = Job.getInstance(conf);
		job1.setJarByClass(Step1DivideCorpus.class);

		job1.setMapperClass(Step1DivideCorpus.Divider.class);
//		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(BooleanLongPair.class);

		job1.setCombinerClass(Step1DivideCorpus.Count.class);

		job1.setReducerClass(Step1DivideCorpus.CountAndZip.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(LongLongPair.class);

		job1.setInputFormatClass(SequenceFileInputFormat.class);

		job1.setPartitionerClass(HashPartitioner.class);

		FileInputFormat.addInputPath(job1, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		System.out.println("""
		                   Done building!
		                   Starting job 1...""");
		System.out.println("Job 1 completed with success status: " + job1.waitForCompletion(true) + "!");

		conf.setLong("N", job1.getCounters().findCounter(NCounter.N_COUNTER).getValue());

		System.out.println("Counter value is: " + conf.getLong("N", -1));

		System.out.println("Building job 2...");
		Job job2 = Job.getInstance(conf); // TODO check purpose
		job2.setJarByClass(Step2N_rT_r.class); // TODO check purpose

		job2.setMapperClass(Step2N_rT_r.CalcThings.class);
//		job2.setInputFormatClass(SequenceFileInputFormat.class);
		job2.setMapOutputKeyClass(BooleanLongPair.class);
		job2.setMapOutputValueClass(LongWritable.class);

//		job2.setCombinerClass(Combiner.class);

		job2.setReducerClass(Step2N_rT_r.T_rN_rReducer.class);
		job2.setOutputKeyClass(BooleanLongPair.class);
		job2.setOutputValueClass(LongLongPair.class);

		job2.setPartitionerClass(HashPartitioner.class);

		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));

		System.out.println("""
		                   Done building!
		                   Starting job 2...""");
		System.out.println("Job 2 completed with success status: " + job2.waitForCompletion(true) + "!");
	}
}
