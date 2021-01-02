package il.co.dsp211.assignment2.steps.step1;

import il.co.dsp211.assignment2.steps.step1.jobs.*;
import il.co.dsp211.assignment2.steps.utils.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class EMR
{
	public static void main(String... args) throws IOException, ClassNotFoundException, InterruptedException
	{
		final Configuration conf = new Configuration();

		System.out.println("Building job 1...");

		Job job1 = Job.getInstance(conf);
		job1.setJarByClass(Job1DivideCorpus.class);

		job1.setInputFormatClass(SequenceFileInputFormat.class);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);

		job1.setMapperClass(Job1DivideCorpus.DividerMapper.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(BooleanLongPair.class);

		job1.setCombinerClass(Job1DivideCorpus.CountCombiner.class);

		job1.setReducerClass(Job1DivideCorpus.CountAndZipReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(LongLongPair.class);

		FileInputFormat.addInputPath(job1, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"));
		FileOutputFormat.setOutputPath(job1, new Path("s3://word-prediction/Step1Output"));

		System.out.println("Done building!\n" +
		                   "Starting job 1...");
		System.out.println("Job 1 completed with success status: " + job1.waitForCompletion(true) + "!");

		conf.setLong("N", job1.getCounters().findCounter(NCounter.N_COUNTER).getValue());

		System.out.println("Counter value is: " + conf.getLong("N", -1));

		//--------------------------------------------------------------------------------------------------------------

		System.out.println("Building job 2...");
		Job job2 = Job.getInstance(conf);
		job2.setJarByClass(Job2CalcT_rN_r.class);

		job2.setInputFormatClass(SequenceFileInputFormat.class);
		job2.setOutputFormatClass(SequenceFileOutputFormat.class);

		job2.setMapperClass(Job2CalcT_rN_r.SplitRsMapper.class);
		job2.setMapOutputKeyClass(BooleanLongPair.class);
		job2.setMapOutputValueClass(LongWritable.class);

		job2.setReducerClass(Job2CalcT_rN_r.CalcT_rN_rReducer.class);
		job2.setOutputKeyClass(BooleanLongPair.class);
		job2.setOutputValueClass(LongLongPair.class);

		FileInputFormat.addInputPath(job2, new Path("s3://word-prediction/Step1Output"));
		FileOutputFormat.setOutputPath(job2, new Path("s3://word-prediction/Step2Output"));

		System.out.println("Done building!\n" +
		                   "Starting job 2...");
		System.out.println("Job 2 completed with success status: " + job2.waitForCompletion(true) + "!");

		//--------------------------------------------------------------------------------------------------------------

		System.out.println("Building job 3...");
		Job job3 = Job.getInstance(conf);
		job3.setJarByClass(Job3JoinTriGramsWithT_rN_r.class);

		MultipleInputs.addInputPath(job3, new Path("s3://word-prediction/Step1Output"), SequenceFileInputFormat.class, Job3JoinTriGramsWithT_rN_r.TriGramMapper.class);
		MultipleInputs.addInputPath(job3, new Path("s3://word-prediction/Step2Output"), SequenceFileInputFormat.class, Job3JoinTriGramsWithT_rN_r.T_rN_rMapper.class);
		job3.setOutputFormatClass(SequenceFileOutputFormat.class);

		job3.setMapOutputKeyClass(BooleanBooleanLongTriple.class);
		job3.setMapOutputValueClass(Text.class);

		job3.setReducerClass(Job3JoinTriGramsWithT_rN_r.JoinReducer.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(LongLongPair.class);

		job3.setPartitionerClass(Job3JoinTriGramsWithT_rN_r.JoinPartitioner.class);

		FileOutputFormat.setOutputPath(job3, new Path("s3://word-prediction/Step3Output"));

		System.out.println("Done building!\n" +
		                   "Starting job 3...");
		System.out.println("Job 3 completed with success status: " + job3.waitForCompletion(true) + "!");

		//--------------------------------------------------------------------------------------------------------------

		System.out.println("Building job 4...");
		Job job4 = Job.getInstance(conf);
		job4.setJarByClass(Job4CalcProb.class);

		job4.setInputFormatClass(SequenceFileInputFormat.class);
		job4.setOutputFormatClass(SequenceFileOutputFormat.class);

		job4.setMapperClass(Job4CalcProb.IdentityMapper.class);
		job4.setMapOutputKeyClass(Text.class);
		job4.setMapOutputValueClass(LongLongPair.class);

		job4.setReducerClass(Job4CalcProb.CalcProbReducer.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job4, new Path("s3://word-prediction/Step3Output"));
		FileOutputFormat.setOutputPath(job4, new Path("s3://word-prediction/Step4Output"));

		System.out.println("Done building!\n" +
		                   "Starting job 4...");
		System.out.println("Job 4 completed with success status: " + job4.waitForCompletion(true) + "!");

		//--------------------------------------------------------------------------------------------------------------

		System.out.println("Building job 5...");
		Job job5 = Job.getInstance(conf);
		job5.setJarByClass(Job5Sort.class);

		job5.setInputFormatClass(SequenceFileInputFormat.class);
		job5.setOutputFormatClass(TextOutputFormat.class);

		job5.setMapperClass(Job5Sort.CastlingMapper.class);
		job5.setMapOutputKeyClass(StringStringDoubleTriple.class);
		job5.setMapOutputValueClass(Text.class);

		job5.setReducerClass(Job5Sort.FinisherReducer.class);
		job5.setOutputKeyClass(Text.class);
		job5.setOutputValueClass(DoubleWritable.class);

		job5.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job5, new Path("s3://word-prediction/Step4Output"));
		FileOutputFormat.setOutputPath(job5, new Path("s3://word-prediction/FinalOutput"));

		System.out.println("Done building!\n" +
		                   "Starting job 5...");
		System.out.println("Job 5 completed with success status: " + job5.waitForCompletion(true) + "!\n" +
		                   "Exiting...");
	}
}
