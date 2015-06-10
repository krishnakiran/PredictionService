package com.infosys.spark;


public class FeatureEngineeringTest 
{
	public static void main(String[] args) 
	{/*

		JavaSparkContext sc = SpackContextHolder.getInstace();
		
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
		
		Normalizer normalizer = new Normalizer(Double.POSITIVE_INFINITY);
		
		
		StandardScaler standardScaler = new StandardScaler(false, true);
		StandardScalerModel standardScalerModel=standardScaler.fit(features.rdd());
		
		
		JavaRDD<LabeledPoint> data = trimmed.map((double[] record) -> 
	    {
	    	double[] feature=new double[record.length-1];
	    	double label=record[0];
	    	for(int i=1;i<record.length;i++)
			{
				feature[i-1] = record[i];
			}
	    	//return new LabeledPoint(label,normalizer.transform(Vectors.dense(feature)));
	    	//return new LabeledPoint(label,standardScalerModel.transform(Vectors.dense(feature)));
	    	return new LabeledPoint(label,(Vectors.dense(feature)));
	    });
		
		JavaRDD<LabeledPoint> discretizedData = data.map((LabeledPoint lp) ->{
			        final double[] discretizedFeatures = new double[lp.features().size()];
			        for (int i = 0; i < lp.features().size(); ++i) {
			          discretizedFeatures[i] = lp.features().apply(i) / 16;
			        }
			        return new LabeledPoint(lp.label(), Vectors.dense(discretizedFeatures));
			      });
		
		ChiSqSelector selector = new ChiSqSelector(4);
		
		// Create ChiSqSelector model (selecting features)
		final ChiSqSelectorModel transformer = selector.fit(discretizedData.rdd());
		
		// Filter the top 50 features from each feature vector
		JavaRDD<LabeledPoint> filteredData = discretizedData.map((LabeledPoint lp) ->{
		        return new LabeledPoint(lp.label(), transformer.transform(lp.features()));
		    });
		
		List<LabeledPoint> labeledPoints = filteredData.collect();
		for(LabeledPoint labeledPoint : labeledPoints)
		{
			System.out.println(labeledPoint.features());
		}
		
		Integer numClasses = 2;
		JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.8, 0.2});
		JavaRDD<LabeledPoint> trainingData = splits[0];
		JavaRDD<LabeledPoint> testData = splits[1];

		final LogisticRegressionModel model = LogisticRegressionWithSGD.train(trainingData.rdd(), 5);
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
		
	*/}


}
