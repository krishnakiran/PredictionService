package com.infosys.spark;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;

import scala.Tuple2;


public class SparkPlayGround 
{
	
	public static void main(String[] args) 
	{
		JavaSparkContext sc = SpackContextHolder.getInstace();
		
		JavaRDD<String> rawData=sc.textFile("data/train_noheader.tsv");
		
		JavaRDD<String[]> records=rawData.map(new Function<String,String[]>(){
			  public String[] call(String s) throws Exception {
                  return s.split("\t");
              }
		}
		);
		
		JavaRDD<String[]> trimmed=records.map((String[] record) -> 
		{
			for(int i=0;i<record.length;i++)
			{
				record[i] = record[i].trim().replaceAll("\"", "");
			}
			return record;
		}
		);
				
		
		/*JavaRDD<Integer> labels=trimmed.map((String[] record) ->
		{
			return Integer.parseInt(record[record.length-1]);
			
		});*/
		
		JavaRDD<LabeledPoint> data= trimmed.map((String[] record) -> 
		{
			return Arrays.copyOfRange(record, 4, record.length-1);
		}
		).map((String[] record) -> 
		{
			Double label=Double.parseDouble(record[record.length-1]);
			
			double[] feature=new double[record.length-1];
			for(int i=0;i<feature.length;i++)
			{
				if(record[i]=="?")
				{
					feature[i] = 0.0;
				}
				else if (record[i].equalsIgnoreCase("no"))
				{
						feature[i] = 0.0;
				}
				else if(record[i].equalsIgnoreCase("yes"))
				{
					feature[i] = 1.0;
				}
				else
					feature[i] = Double.parseDouble(record[i]);
				
			}
			return new LabeledPoint(label.doubleValue(),Vectors.dense(feature));
		});
		
		Integer numClasses = 2;
		JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.8, 0.2});
		JavaRDD<LabeledPoint> trainingData = splits[0];
		JavaRDD<LabeledPoint> testData = splits[1];

		// Set parameters.
		Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
		String impurity = "gini";
		Integer maxDepth = 5;
		Integer maxBins = 32;
		
		// Train a DecisionTree model for classification.
		final DecisionTreeModel model = DecisionTree.trainClassifier(trainingData, 2,
		  categoricalFeaturesInfo, impurity, maxDepth, maxBins);

		// Evaluate model on test instances and compute test error
		JavaPairRDD<Double, Double> predictionAndLabel =
		  testData.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
		    @Override
		    public Tuple2<Double, Double> call(LabeledPoint p) {
		      return new Tuple2<Double, Double>(model.predict(p.features()), p.label());
		    }
		  });
		Double testErr =
		  1.0 * predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
		    @Override
		    public Boolean call(Tuple2<Double, Double> pl) {
		      return !pl._1().equals(pl._2());
		    }
		  }).count() / testData.count();
		System.out.println("Test Error: " + testErr);
		System.out.println("Learned classification tree model:\n" + model.toDebugString());
		
		System.out.println(model.toDebugString());
		
	}
}
