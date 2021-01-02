package il.co.dsp211.assignment2;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;

/**
 * aws s3 mv out/artifacts/Knowledge_base_for_Word_Prediction_jar/EMR.jar s3://word-prediction
 */
public class Main
{
	public static void main(String... args)
	{
		System.out.println("Creating cluster...");
		// create an EMR client using the credentials and region specified in order to create the cluster
		System.out.println("Cluster created with ID: " + AmazonElasticMapReduceClientBuilder.standard()
				.withRegion(Regions.US_EAST_1)
				.build()
				// create the cluster
				.runJobFlow(new RunJobFlowRequest()
						.withName("Knowledge base for Word Predictor")
						.withReleaseLabel("emr-6.2.0") // specifies the EMR release version label, we recommend the latest release
						// create a step to enable debugging in the AWS Management Console
						.withSteps(
								new StepConfig("Enable debugging", new StepFactory().newEnableDebuggingStep())
										.withActionOnFailure(ActionOnFailure.TERMINATE_CLUSTER.toString()),
								new StepConfig("EMR", new HadoopJarStepConfig("s3://word-prediction/EMR.jar")
										.withArgs(args))
										.withActionOnFailure(ActionOnFailure.TERMINATE_CLUSTER.toString())
						)
						.withLogUri("s3://word-prediction/logs") // a URI in S3 for log files is required when debugging is enabled
						.withServiceRole("EMR_DefaultRole") // replace the default with a custom IAM service role if one is used
						.withJobFlowRole("EMR_EC2_DefaultRole") // replace the default with a custom EMR role for the EC2 instance profile if one is used
						.withInstances(new JobFlowInstancesConfig()
								.withInstanceCount(3)
								.withKeepJobFlowAliveWhenNoSteps(false)
								.withMasterInstanceType(InstanceType.M5Xlarge.toString())
								.withSlaveInstanceType(InstanceType.M5Xlarge.toString())))
				.getJobFlowId());
	}
}
