package com.infosys.spark;

import hex.AUCData;
import hex.tree.gbm.GBM;
import hex.tree.gbm.GBMModel;
import hex.tree.gbm.GBMModel.GBMParameters.Family;
import hex.utils.MSETsk;

import java.io.FileWriter;
import java.io.IOException;
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

import water.DKV;
import water.Scope;
import water.fvec.DataFrame;
import water.fvec.Frame;
import water.util.SB;

public class GBMChurnSample 
{

	public static void main(String[] args) 
	{
		String[] jars = new String[]{"/Users/Admin/Hackathon/spark-1.2.1-bin-hadoop2.4/lib/sparkling-water-assembly-0.2.12-89-all.jar"};
		SparkConf conf = new SparkConf(true).setMaster("spark://192.168.1.14:7077").setAppName("ChurnApp").set("spark.scheduler.mode", "FAIR").setJars(jars);
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
	    
		//Scope.enter();
		
		int ci = train_table.find("churn"); // Convert response to categorical
		Scope.track(train_table.replace(ci, train_table.vecs()[ci].toEnum())._key);
		DKV.put(train_table);
		
		int ci1 = test_table.find("churn"); // Convert response to categorical
		Scope.track(test_table.replace(ci1, test_table.vecs()[ci1].toEnum())._key);
		DKV.put(test_table);
		
		GBMModel.GBMParameters params = new GBMModel.GBMParameters();
		  
	      params._train = train_table._key;
	      params._valid=test_table._key;
	      params._loss = Family.multinomial;
	      params._response_column = "churn"; 
	      params._ntrees = 10;
	      params._max_depth = 5;
	      params._min_rows = 1;
	      params._nbins = 20;
	      params._ignored_columns=new String[]{"area"};
	      params._learn_rate = 1.0f;
	      params._score_each_iteration=true;
	      
	      GBM job = null;
	      GBMModel gbm = null;
	      try 
	      {
	        job = new GBM(params);
	        gbm = job.trainModel().get();
	      } 
	      finally 
	      {
	        if (job != null) job.remove();
	      }
	    
	      
	    DataFrame predict = null;
	    
	    Frame score = gbm.score(test_table);
	    MSETsk mse = new MSETsk().doAll(score.anyVec(), test_table.vec(gbm._output.responseName()));
	    System.out.println("MSE::"+mse._resDev/test_table.numRows());
	    
	    predict = h2oContext.createDataFrame(score);
	    
	    hex.ModelMetricsBinomial mm = hex.ModelMetricsBinomial.getFromDKV(gbm,params.valid());
	    System.out.println("Model Metrics***");
	    System.out.println(mm.toJsonString());
	    
	    AUCData adata = mm._aucdata;
	    System.out.println("Area Under Curve::"+adata.AUC());
	    System.out.println("Accuracy");
		System.out.println(adata.accuracy());
		System.out.println("Confusion Matrix");
		System.out.println(adata.CM().toASCII());
		
		System.out.println("Model::"+gbm._output._variable_importances.toJsonString());
	      
		FileWriter fileWriter=null;
	    try 
	    {
	    	  System.out.println("Writing Java Model to data/GBMJavaModel.java");
	    	  fileWriter = new FileWriter("data/GBMJavaModel.java",false);
		      SB sb = new SB();
		      gbm.toJava(sb);
		      fileWriter.write(sb.toString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    finally
	    {
	    	try {
				fileWriter.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }
	    
	    
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
	    	//Scope.exit();
	    	sc.stop();
	    }
	}
	
	

}
