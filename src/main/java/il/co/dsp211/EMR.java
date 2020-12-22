package il.co.dsp211;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;

public class EMR
{
	public static void main(String... args)
	{
		AWSCredentials credentials_profile;
		try
		{
			credentials_profile = new ProfileCredentialsProvider("default").getCredentials(); // specifies any named profile in .aws/credentials as the credentials provider
		}
		catch (Exception e)
		{
			throw new AmazonClientException("Cannot load credentials from .aws/credentials file. Make sure that the credentials file exists and that the profile name is defined within it.", e);
		}


		// create an EMR client using the credentials and region specified in order to create the cluster
		final StepConfig
				step1 = new StepConfig("Step 1 - divide corpus", new HadoopJarStepConfig("s3://path/to/divideCorpus.jar") // TODO
				.withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data", )),
		step2 = new StepConfig("Step 2 - calculate  T_r and probabilities", new HadoopJarStepConfig("s3://path/to/___.jar") // TODO
				.withArgs());

		System.out.println("The cluster ID is " + AmazonElasticMapReduceClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(credentials_profile))
				.withRegion(Regions.US_EAST_1)
				.build()
				// create the cluster
				.runJobFlow(new RunJobFlowRequest()
						.withName("MyClusterCreatedFromJava")
						.withReleaseLabel("emr-6.2.0") // specifies the EMR release version label, we recommend the latest release
						// create a step to enable debugging in the AWS Management Console
						.withSteps(
								new StepConfig("Enable debugging", new StepFactory().newEnableDebuggingStep())
										.withActionOnFailure("TERMINATE_CLUSTER"),
								step1,
								step2)
						.withLogUri("s3://path/to/my/emr/logs") // a URI in S3 for log files is required when debugging is enabled
						.withServiceRole("EMR_DefaultRole") // replace the default with a custom IAM service role if one is used
						.withJobFlowRole("EMR_EC2_DefaultRole") // replace the default with a custom EMR role for the EC2 instance profile if one is used
						.withInstances(new JobFlowInstancesConfig()
//										.withEc2SubnetId("subnet-12ab34c56") // TODO check if needed
								.withEc2KeyName("RoysKey") // TODO maybe need to change
								.withInstanceCount(3)
								.withKeepJobFlowAliveWhenNoSteps(true)
								.withMasterInstanceType("m5.xlarge")
								.withSlaveInstanceType("m5.xlarge")
								.withKeepJobFlowAliveWhenNoSteps(false)
								.withPlacement(new PlacementType("us-east-1a")))) // TODO check if needed
				.getJobFlowId());
	}
}
