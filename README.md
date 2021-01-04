# Knowledge-base-for-Word-Prediction
We will generate a knowledge-base for Hebrew word-prediction system, based on Google 3-Gram Hebrew dataset, using Amazon Elastic Map-Reduce (EMR).

## Running instructions:
1. Create s3 bucket.
2. Compile project and create a jar with `il/co/dsp211/assignment2/steps/step1/EMR.java` as your main class.
3. Upload your jar to your S3 bucket.
4. Fill `config.properties` file with the followings:
    1. `bucketName` - the name of your bucket you've created at step 1.
    2. `isWithCombiners` - with one of the following options:
        - `TRUE` - if you want to run this cluster **with** combiners.
        - `FALSE` - if you want to run this cluster **without** combiners.
        - `BOTH` - if you want to run **both** options, in such case this programm will create 2 folders for the products of this program, one for each option.
    3. `jarFileName` - the name of the jar you've created at step 2 **without extension**.
    4. `instanceCount` - number of instances in the cluster.
5. Run `il/co/dsp211/assignment2/Main.java`.
6. The final output will be present in folder `FinalOutput`.

## Map-Reduce Program Flow:
### Step 1: filter & divide 
- For each record we're checking if it consists of 3 words (TriGram) with hebrew letters without special characters.
- 
