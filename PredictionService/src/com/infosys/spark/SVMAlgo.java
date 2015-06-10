package com.infosys.spark;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.feature.Normalizer;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

import scala.Tuple2;


public class SVMAlgo 
{
	
	public static void main(String[] args) 
	{
		JavaSparkContext sc = SpackContextHolder.getInstace();
		
		JavaRDD<String> rawData=sc.textFile("data/churn-trainingset.csv");
		
		JavaRDD<String[]> records=rawData.map(new Function<String,String[]>(){
			  public String[] call(String s) throws Exception {
                  return s.split(",");
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
		
		/*JavaRDD<LabeledPoint> data= trimmed.map((String[] record) -> 
		{
			return Arrays.copyOfRange(record, 6, record.length-1);
		}*/
		//)
		Normalizer normalizer = new Normalizer(Double.POSITIVE_INFINITY);

		JavaRDD<LabeledPoint> data = trimmed.map((String[] record) -> 
		{
			Double label=Double.parseDouble(record[0]);
			
			double[] feature=new double[record.length-1];
			for(int i=1;i<record.length;i++)
			{
				if(record[i]=="?")
				{
					feature[i-1] = 0.0;
				}
				else if (record[i].equalsIgnoreCase("no"))
				{
						feature[i-1] = 0.0;
				}
				else if(record[i].equalsIgnoreCase("yes"))
				{
					feature[i-1] = 1.0;
				}
				else
					feature[i-1] = Double.parseDouble(record[i]);
				
			}
			return new LabeledPoint(label.doubleValue(),Vectors.dense(feature));
		});

		Integer numClasses = 2;
		JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.8, 0.2});
		JavaRDD<LabeledPoint> trainingData = splits[0];
		JavaRDD<LabeledPoint> testData = splits[1];

		final SVMModel model = SVMWithSGD.train(trainingData.rdd(), 5);
		
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
		System.out.println("Learned classification tree model:\n" + model.toString());
		
		JavaRDD<Double> predictions= model.predict(TestDataFeeder.fetchTestData());
		System.out.println("Predictions on Indepedent Test Data::");
		System.out.println(predictions.collect());

		
	}
}
