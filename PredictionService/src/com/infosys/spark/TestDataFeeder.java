package com.infosys.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;


public final class TestDataFeeder 
{
	public static final JavaRDD<Vector> fetchTestData()
	{
		JavaSparkContext sc = SpackContextHolder.getInstace();
		
		JavaRDD<String> rawData=sc.textFile("data/churn-testingset.csv");
		
		JavaRDD<String[]> records=rawData.map(new Function<String,String[]>(){
			  public String[] call(String s) throws Exception {
                  return s.split(",");
              }
		}
		);
		
		JavaRDD<Vector> features = records.map((String[] record) -> 
		{
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
			return Vectors.dense(feature);		
		});
		return features;
	}

}
