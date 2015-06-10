package com.infosys.spark;

import hex.glm.GLM;
import hex.glm.GLMModel;
import hex.glm.GLMModel.GLMParameters;
import hex.glm.GLMModel.GLMParameters.Family;
import hex.glm.GLMValidation;

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

public class LinearModelSample 
{
	public static void main(String[] args) 
	{
		SparkConf conf = new SparkConf(true).setMaster("local").setAppName("AirLines");
		JavaSparkContext sc = new JavaSparkContext(conf);
		H2OContext h2oContext = new H2OContext(sc.sc()).start();
		JavaSQLContext sqlCtx = new JavaSQLContext(sc);
		try{
		// The schema is encoded in a string
		String schemaString = "fYear,fMonth,fDayofMonth,fDayOfWeek,DepTime,ArrTime,UniqueCarrier,Origin,Dest,Distance,IsDepDelayed,IsDepDelayed_REC";
		
		// Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<StructField>();
		for (String fieldName : schemaString.split(","))
		{
			fields.add(DataType.createStructField(fieldName,DataType.StringType, false));
		}
		
		StructType schema = DataType.createStructType(fields);
		JavaRDD<String> trainData = sc.textFile("data/AirlinesTrain.csv");
		JavaRDD<String> testData = sc.textFile("data/AirlinesTest.csv");
		
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
		schemaAirlines_Train.registerTempTable("airlines");
		JavaSchemaRDD airlines_train_data = sqlCtx.sql("SELECT * FROM airlines");
		
		JavaSchemaRDD schemaAirlines_Test = sqlCtx.applySchema(testRowRDD, schema);
		schemaAirlines_Test.registerTempTable("airlines_test");
		JavaSchemaRDD airlines_test_data = sqlCtx.sql("SELECT * FROM airlines_test");
		
		DataFrame train_table = h2oContext.createDataFrame(airlines_train_data.schemaRDD());
		DataFrame test_table = h2oContext.createDataFrame(airlines_test_data.schemaRDD());
		
		String [] ignoredCols = new String[]{"fYear", "fMonth", "fDayofMonth", "fDayOfWeek", "DepTime","ArrTime","IsDepDelayed_REC"};
	    
		Scope.enter();
		
		int ci = train_table.find("IsDepDelayed"); // Convert response to categorical
		Scope.track(train_table.replace(ci, train_table.vecs()[ci].toEnum())._key);
		DKV.put(train_table);
		
		int ci1 = test_table.find("IsDepDelayed"); // Convert response to categorical
		Scope.track(test_table.replace(ci1, test_table.vecs()[ci1].toEnum())._key);
		DKV.put(test_table);
		
		
		GLMParameters params = new GLMParameters(Family.binomial);
	    params._response_column = "IsDepDelayed";
	    params._ignored_columns = ignoredCols;
	    params._train = train_table._key;
	    params._valid = test_table._key;
	    params._lambda = new double[]{1e-5};
	    params._standardize = false;
	    params._use_all_factor_levels=true;
	    params._score_each_iteration=false;
	    params._lambda_search=false;
	    params._alpha=new double[]{0.5};
	    
	    GLM job = null;
 	    GLMModel glmModel=null;
 	    
	    try
		{
	    	job = new GLM(params);
	 	    glmModel=job.trainModel().get();
		}
		finally 
		{
			if (job != null)
				job.remove();
		}
	    
	    System.out.println("***Model Details ***");
		System.out.println(glmModel.toJsonString());
	    
	    
	    GLMValidation validation=glmModel.validation();
	    System.out.println("Validation Details");
	    System.out.println(validation.toJsonString());
	    
	    DataFrame predict = null;
	    
	    predict = h2oContext.createDataFrame(glmModel.score(test_table));
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
