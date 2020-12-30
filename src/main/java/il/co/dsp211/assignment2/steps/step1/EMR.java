package il.co.dsp211.assignment2.steps.step1;

import il.co.dsp211.assignment2.steps.step1.jobs.*;
import il.co.dsp211.assignment2.steps.utils.BooleanLongPair;
import il.co.dsp211.assignment2.steps.utils.LongLongPair;
import il.co.dsp211.assignment2.steps.utils.NCounter;
import il.co.dsp211.assignment2.steps.utils.StringStringDoubleTriple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
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

//		job1.setInputFormatClass(SequenceFileInputFormat.class); // TODO: Commented for testing input

		job1.setMapperClass(Step1DivideCorpus.Divider.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(BooleanLongPair.class);

		job1.setCombinerClass(Step1DivideCorpus.Count.class);

		job1.setReducerClass(Step1DivideCorpus.CountAndZip.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(LongLongPair.class);

		job1.setPartitionerClass(HashPartitioner.class);

		FileInputFormat.addInputPath(job1, new Path("/WordPred/Input"/*"s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"*/)); // TODO: Testing input
		FileOutputFormat.setOutputPath(job1, new Path("/WordPred/Step1Output"));

		System.out.println("Done building!\n" +
		                   "Starting job 1...");
		System.out.println("Job 1 completed with success status: " + job1.waitForCompletion(true) + "!");

		conf.setLong("N", job1.getCounters().findCounter(NCounter.N_COUNTER).getValue());

		System.out.println("Counter value is: " + conf.getLong("N", -1));

		//--------------------------------------------------------------------------------------------------------------

		System.out.println("Building job 2...");
		Job job2 = Job.getInstance(conf); // TODO check purpose
		job2.setJarByClass(Step2CalcT_rN_r.class); // TODO check purpose

		job2.setMapperClass(Step2CalcT_rN_r.CalcThings.class);
		job2.setMapOutputKeyClass(BooleanLongPair.class);
		job2.setMapOutputValueClass(LongWritable.class);

		job2.setReducerClass(Step2CalcT_rN_r.T_rN_rReducer.class);
		job2.setOutputKeyClass(BooleanLongPair.class);
		job2.setOutputValueClass(LongLongPair.class);

		job2.setPartitionerClass(HashPartitioner.class);

		FileInputFormat.addInputPath(job2, new Path("/WordPred/Step1Output"));
		FileOutputFormat.setOutputPath(job2, new Path("/WordPred/Step2Output"));

		System.out.println("Done building!\n" +
		                   "Starting job 2...");
		System.out.println("Job 2 completed with success status: " + job2.waitForCompletion(true) + "!");

		//--------------------------------------------------------------------------------------------------------------

		System.out.println("Building job 3...");
		Job job3 = Job.getInstance(conf); // TODO check purpose
		job3.setJarByClass(Step3CalcProb.class); // TODO check purpose

		job3.setMapperClass(Step3CalcProb.MapperImpl.class);
		job3.setMapOutputKeyClass(LongWritable.class);
		job3.setMapOutputValueClass(LongLongPair.class);

		job3.setReducerClass(Step3CalcProb.CalcProbReducer.class);
		job3.setOutputKeyClass(LongWritable.class);
		job3.setOutputValueClass(DoubleWritable.class);

		job3.setPartitionerClass(HashPartitioner.class);

		FileInputFormat.addInputPath(job3, new Path("/WordPred/Step2Output"));
		FileOutputFormat.setOutputPath(job3, new Path("/WordPred/Step3Output"));

		System.out.println("Done building!\n" +
		                   "Starting job 3...");
		System.out.println("Job 3 completed with success status: " + job3.waitForCompletion(true) + "!");

		//--------------------------------------------------------------------------------------------------------------

		System.out.println("Building job 4...");
		Job job4 = Job.getInstance(conf); // TODO check purpose
		job4.setJarByClass(Step4JoinTriGramProb.class); // TODO check purpose

		MultipleInputs.addInputPath(job4, new Path("/WordPred/Step1Output"), TextInputFormat.class, Step4JoinTriGramProb.MapperTriGram.class);
		MultipleInputs.addInputPath(job4, new Path("/WordPred/Step3Output"), TextInputFormat.class, Step4JoinTriGramProb.MapperProb.class);

		job4.setMapOutputKeyClass(BooleanLongPair.class);
		job4.setMapOutputValueClass(Text.class);

		job4.setReducerClass(Step4JoinTriGramProb.JoinerReducer.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(DoubleWritable.class);

		job4.setPartitionerClass(Step4JoinTriGramProb.JoinPartitioner.class);

		FileOutputFormat.setOutputPath(job4, new Path("/WordPred/Step4Output"));

		System.out.println("Done building!\n" +
		                   "Starting job 4...");
		System.out.println("Job 4 completed with success status: " + job4.waitForCompletion(true) + "!");

		//--------------------------------------------------------------------------------------------------------------

		System.out.println("Building job 5...");
		Job job5 = Job.getInstance(conf); // TODO check purpose
		job5.setJarByClass(Step5Sort.class); // TODO check purpose

		job5.setMapperClass(Step5Sort.Castling.class);
		job5.setMapOutputKeyClass(StringStringDoubleTriple.class);
		job5.setMapOutputValueClass(Text.class);

		job5.setReducerClass(Step5Sort.Finisher.class);
		job5.setOutputKeyClass(Text.class);
		job5.setOutputValueClass(DoubleWritable.class);

		job5.setPartitionerClass(HashPartitioner.class); // TODO Think about it

		FileInputFormat.addInputPath(job5, new Path("/WordPred/Step4Output"));
		FileOutputFormat.setOutputPath(job5, new Path("/WordPred/Output"));

		System.out.println("Done building!\n" +
		                   "Starting job 5...");
		System.out.println("Job 5 completed with success status: " + job5.waitForCompletion(true) + "!\nExiting...");
	}
}
