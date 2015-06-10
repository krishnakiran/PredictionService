package com.infosys.spark;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.feature.StandardScaler;
import org.apache.spark.mllib.feature.StandardScalerModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

import scala.Tuple2;


public final class TrainingDataFeeder 
{/*
	
	public static final JavaRDD<String[]> fetchDataFronFile()
	{
		JavaSparkContext sc = SpackContectHolder.getInstace();
		
		JavaRDD<String> rawData=sc.textFile("data/churn-trainingset.csv");
		
		JavaRDD<String[]> records=rawData.map(new Function<String,String[]>(){
			  public String[] call(String s) throws Exception {
                  return s.split(",");
              }
		}
		);

		JavaRDD<double[]> trimmed=records.map((String[] record) -> 
		{
			double[] data=new double[record.length];
			for(int i=0;i<record.length;i++)
			{
				data[i] = Double.parseDouble(record[i].trim().replaceAll("\"", ""));
			}
			return data;
		}
		);
		
		JavaRDD<Vector> features=trimmed.map((double[] record) -> 
		{
			double[] temp = Arrays.copyOfRange(record, 1, record.length);
			return Vectors.dense(temp);
		}
		);
		
		StandardScaler standardScaler = new StandardScaler(true, true);
		StandardScalerModel standardScalerModel=standardScaler.fit(features.rdd());
		
		JavaRDD<LabeledPoint> labelPpoints = trimmed.map((String[] record) ->
	    {
	    	double[] feature=new double[record.length-1];
	    	double label=Double.parseDouble(record[0]);
	    	for(int i=1;i<record.length;i++)
			{
				if(record[i]=="?")
				{
					feature[i-1] = 0.0;
				}
				else
					feature[i-1] = Double.parseDouble(record[i]);
			}
	    	return new LabeledPoint(label,standardScalerModel.transform(Vectors.dense(feature)));
	    });
		
		
	}
	

*/}
