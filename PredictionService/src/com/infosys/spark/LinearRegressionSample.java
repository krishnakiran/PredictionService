package com.infosys.spark;

import hex.AUCData;
import hex.Model;
import hex.ModelMetrics;
import hex.glm.GLM;
import hex.glm.GLMModel;
import hex.glm.GLMModel.GLMParameters;
import hex.glm.GLMModel.GLMParameters.Family;

import java.io.File;
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
import water.Key;
import water.Scope;
import water.fvec.DataFrame;
import water.fvec.Frame;
import water.fvec.NFSFileVec;
import water.parser.ParseDataset;

public class LinearRegressionSample 
{
	protected static Frame parse_test_file( Key outputKey, String fname ) 
	  {
		    File f = new File(fname);
		    NFSFileVec nfs = NFSFileVec.make(f);
		    return ParseDataset.parse(outputKey, nfs._key);
	  }
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
	    
		//Scope.enter();
		
		int ci = train_table.find("churn"); // Convert response to categorical
		Scope.track(train_table.replace(ci, train_table.vecs()[ci].toEnum())._key);
		DKV.put(train_table);
		
		int ci1 = test_table.find("churn"); // Convert response to categorical
		Scope.track(test_table.replace(ci1, test_table.vecs()[ci1].toEnum())._key);
		DKV.put(test_table);
		
		GLMParameters params = new GLMParameters(Family.binomial);
	    params._response_column = "churn";
	    params._ignored_columns = ignoredCols;
	    params._train = train_table._key;
	    params._valid = test_table._key;
	    params._n_folds=10;
	    params._score_each_iteration=false;
	    params._link=GLMParameters.Link.logit;
	    params._alpha=new double[]{0.5};
	    params._lambda=new double[]{1e-5};
	    params._solver=GLMParameters.Solver.L_BFGS;
	    
	    GLM job = null;
 	    GLMModel glmModel=null;
 	    
	    try
		{
	    	job = new GLM(params);
	    	job._nclass=1;
	 	    glmModel=job.trainModel().get();
		    glmModel.addMetrics(new ModelMetrics(glmModel, params.valid()));
		}
		finally 
		{
			if (job != null)
				job.remove();
		}
	    
	    /*GLMOutput glmOutput = glmModel._output;
	    ByteArrayOutputStream byteArrayOutputStream=null;
	    FileOutputStream fileOutputStream =null;
	    
	    try
	    {
		    byteArrayOutputStream= new ByteArrayOutputStream();
		    byteArrayOutputStream.write(glmOutput.writeJSON(new AutoBuffer()).buf());
		    fileOutputStream = new FileOutputStream("data/glm_output.json");
		    fileOutputStream.write(byteArrayOutputStream.toByteArray());
	    }
	    catch(Exception ex)
	    {
	    	ex.printStackTrace();
	    }
	    finally
	    {
	    	try {
				fileOutputStream.close();
				byteArrayOutputStream.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	
	    }*/
	   /* System.out.println("coefficients");
	    HashMap<String, Double> coeficients=glmModel.coefficients();
	    coeficients.forEach(new BiConsumer<String, Double>() {
	    	@Override
	    	public void accept(String t, Double u) {
	    		System.out.println("Key::"+t+"\t"+"Value::"+u);
	    	}
		});*/
	    
	    DataFrame predict = null;
	    
	    predict = h2oContext.createDataFrame(glmModel.score(test_table));
	    
	    System.out.println("Model details::"+glmModel);
	    
	    hex.ModelMetricsBinomial mm = hex.ModelMetricsBinomial.getFromDKV(glmModel,params.valid());
	    AUCData adata = mm._aucdata;
	    System.out.println("Accuracy");
	    System.out.println(adata.accuracy());
	    System.out.println("Confusion Matrix");
	    System.out.println(adata.CM().toASCII());
	    
	    
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
	
	 private static Key<ModelMetrics> buildKey(Key model_key, long model_checksum, Key frame_key, long frame_checksum) {
		    return Key.make("modelmetrics_" + model_key + "@" + model_checksum + "_on_" + frame_key + "@" + frame_checksum);
		  }

		  private static Key<ModelMetrics> buildKey(Model model, Frame frame) {
		    return buildKey(model._key, model.checksum(), frame._key, frame.checksum());
		  }
}
