#!/bin/sh
aws s3 rm s3://word-prediction-dsp --recursive && aws s3 mv out/artifacts/Knowledge_base_for_Word_Prediction_jar/EMR.jar s3://word-prediction-dsp
