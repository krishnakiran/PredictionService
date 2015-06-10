package com.infosys.spark;

import hex.deeplearning.DeepLearning;
import hex.deeplearning.DeepLearningModel;
import hex.deeplearning.DeepLearningModel.DeepLearningParameters;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.h2o.H2OContext;
import org.apache.spark.sql.SchemaRDD;
import org.apache.spark.sql.api.java.DataType;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.api.java.StructField;
import org.apache.spark.sql.api.java.StructType;
import org.junit.Assert;

import water.DKV;
import water.H2O;
import water.Key;
import water.Scope;
import water.fvec.DataFrame;

public class DeepLearningSample 
{
	public static void main(String[] args) 
	{
		SparkConf conf = new SparkConf(true).setMaster("local").setAppName("AirLines");
		JavaSparkContext sc = new JavaSparkContext(conf);
		H2OContext h2oContext = new H2OContext(sc.sc()).start();
		JavaSQLContext sqlCtx = new JavaSQLContext(sc);
		try{
		// The schema is encoded in a string
		String schemaString = "churn,area,vmail,vmail_msgs,day_mins,day_calls,day_charge,eve_mins,eve_calls,eve_charge,night_mins,night_calls,night_charge,intl_mins,intl_calls,intl_charge,svc_calls";
		
		// Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<StructField>();
		for (String fieldName : schemaString.split(","))
		{
			fields.add(DataType.createStructField(fieldName,DataType.StringType, false));
		}
		
		StructType schema = DataType.createStructType(fields);
		JavaRDD<String> trainData = sc.textFile("data/churn-trainingset.csv");
		JavaRDD<String> testData = sc.textFile("data/churn-testingset.csv");
		
		JavaRDD<Row> trainRowRDD = trainData.map(
				  new Function<String, Row>() {
				    public Row call(String record) throws Exception {
				      String[] recordData = record.split(",");
				      for(int i=0;i<recordData.length;i++)
				      {
				    	  recordData[i]=recordData[i].trim();
				      }
				      return Row.create(recordData);
				    }
				  });
		
		JavaRDD<Row> testRowRDD = testData.map(
				  new Function<String, Row>() {
				    public Row call(String record) throws Exception {
				      String[] recordData = record.split(",");
				      for(int i=0;i<recordData.length;i++)
				      {
				    	  recordData[i]=recordData[i].trim();
				      }
				      return Row.create(recordData);
				    }
				  });
		
		JavaSchemaRDD schemaAirlines_Train = sqlCtx.applySchema(trainRowRDD, schema);
		schemaAirlines_Train.registerTempTable("churn");
		JavaSchemaRDD airlines_train_data = sqlCtx.sql("SELECT * FROM churn");
		
		JavaSchemaRDD schemaAirlines_Test = sqlCtx.applySchema(testRowRDD, schema);
		schemaAirlines_Test.registerTempTable("churn_test");
		JavaSchemaRDD airlines_test_data = sqlCtx.sql("SELECT * FROM churn_test");
		
		DataFrame train_table = h2oContext.createDataFrame(airlines_train_data.schemaRDD());
		DataFrame test_table = h2oContext.createDataFrame(airlines_test_data.schemaRDD());
		
		String [] ignoredCols = new String[]{"area"};
	    
		Scope.enter();
		
		int ci = train_table.find("churn"); // Convert response to categorical
		Scope.track(train_table.replace(ci, train_table.vecs()[ci].toEnum())._key);
		DKV.put(train_table);
		
		int ci1 = test_table.find("churn"); // Convert response to categorical
		Scope.track(test_table.replace(ci1, test_table.vecs()[ci1].toEnum())._key);
		DKV.put(test_table);
		
		DeepLearningParameters p = new DeepLearningParameters();
		
        // populate model parameters
        p._destination_key = Key.make("dl_churn_model");
        p._train = train_table._key;
        p._valid = train_table._key;
        p._ignored_columns=ignoredCols;
        p._seed=113974111632823084l;
        p._rho=0.99;
        p._epsilon=1.0E-8;
        p._input_dropout_ratio=0.0;
        p._variable_importances=true;
        p._use_all_factor_levels=true;
        p._response_column = "churn"; // last column is the response
        p._activation = DeepLearningParameters.Activation.Rectifier;
       // p._hidden = new int[]{2500, 2000, 1500, 1000, 500};
        p._train_samples_per_iteration = -2;//airlines_train_data.count() * H2O.getCloudSize(); //process 1500 rows per node per map-reduce step
        p._epochs = 10;//1.8 * (float) p._train_samples_per_iteration / train_table.numRows(); //train long enough to do 2 map-reduce passes (with scoring each time)
        p._hidden=new int[]{200,200};
        p._score_interval=5.0;
        // speed up training
        p._adaptive_rate = true; //disable adaptive per-weight learning rate -> default settings for learning rate and momentum are probably not ideal (slow convergence)
        //p._replicate_training_data = false; //avoid extra communication cost upfront, got enough data on each node for load balancing
        p._override_with_best_model = true; //no need to keep the best model around
        p._diagnostics = false; //no need to compute statistics during training
        //p._score_interval = 20; //score and print progress report (only) every 20 seconds
        //p._score_training_samples = 50; //only score on a small sample of the training set -> don't want to spend too much time scoring (note: there will be at least 1 row per chunk)

        DeepLearning dl = new DeepLearning(p);
        DeepLearningModel model = null;
        try {
          model = dl.trainModel().get();
          if (model != null) {
            Assert.assertTrue(1000. * model.model_info().get_processed_total() / model.run_time > 20); //we expect at least a training speed of 20 samples/second (MacBook Pro: ~50 samples/second)
          }
        } catch (Throwable t) {
          t.printStackTrace();
          throw new RuntimeException(t);
        } finally {
          dl.remove();
          if (model != null) {
            model.delete();
          }
        }
	    //System.out.println("***Model Details ***");
		//System.out.println(model.toJsonString());

		DataFrame predict = null;
	    predict = h2oContext.createDataFrame(model.score(test_table));
		SchemaRDD schemaRDD = h2oContext.asSchemaRDD(predict,sqlCtx.sqlContext());
		
		JavaSchemaRDD javaSchemaRDD = schemaRDD.toJavaSchemaRDD();
		List<String> predictions = javaSchemaRDD.map(new Function<Row, String>() {
			public String call(Row row) {
				return row.toString();
			}
		}).collect();
		
		for(String prediction :predictions)
		{
			System.out.println("Prediction:: "+prediction);
		}
		}
	    finally
	    {
	    	Scope.exit();
	    	sc.stop();
	    }
	}

}
