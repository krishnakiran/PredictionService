package com.infosys.spark;

import hex.tree.gbm.GBM;
import hex.tree.gbm.GBMModel;
import hex.tree.gbm.GBMModel.GBMParameters;
import hex.tree.gbm.GBMModel.GBMParameters.Family;

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
import water.H2O;
import water.H2OApp;
import water.Key;
import water.Scope;
import water.fvec.DataFrame;
import water.fvec.Frame;
//Import StructType and StructField
import water.fvec.NFSFileVec;
import water.parser.ParseDataset;


public class SparkilgWaterSample
{
	public static void main(String[] args) 
 {
		GBMModel gbm = null;
		Frame fr = null, test_data = null;
		DataFrame predict = null;

		SparkConf conf = new SparkConf(true).setMaster("local").setAppName("ChurnApp");
		JavaSparkContext sc = new JavaSparkContext(conf);
		H2OContext h2oContext = new H2OContext(sc.sc()).start();
		JavaSQLContext sqlCtx = new JavaSQLContext(sc);

		// The schema is encoded in a string
		String schemaString = "sepal_len sepal_wid petal_len petal_wid class";

		// Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<StructField>();
		for (String fieldName : schemaString.split(" ")) {
			fields.add(DataType.createStructField(fieldName,
					DataType.StringType, false));
		}
		
		StructType schema = DataType.createStructType(fields);
		JavaRDD<String> rawData = sc.textFile("data/iris1.csv");

		JavaRDD<Row> rowRDD = rawData.map(
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
		
		
		JavaSchemaRDD schemaPeople = sqlCtx.applySchema(rowRDD, schema);
		
		schemaPeople.registerTempTable("eyes");
		// SQL can be run over RDDs that have been registered as tables.
		JavaSchemaRDD eyes_data = sqlCtx.sql("SELECT * FROM eyes");

		DataFrame irisTable = h2oContext.createDataFrame(eyes_data.schemaRDD());

		GBMParameters params = new GBMParameters();
		int ci = irisTable.find("class"); // Convert response to categorical
		Scope.track(irisTable.replace(ci, irisTable.vecs()[ci].toEnum())._key);
		DKV.put(irisTable);
		
		params._train = irisTable._key;
		params._valid = irisTable._key;
		params._response_column="class";
		params._ntrees = 5;
		params._loss = Family.multinomial;
		
		GBM job = null;
		try
		{
			job = new GBM(params);
			gbm = job.trainModel().get();
		}
		finally 
		{
			if (job != null)
				job.remove();
		}

		System.out.println("***Model Details ***");
		System.out.println(gbm.toJsonString());

		predict = h2oContext.createDataFrame(gbm.score(irisTable));
		SchemaRDD schemaRDD = h2oContext.asSchemaRDD(predict,sqlCtx.sqlContext());

		JavaSchemaRDD javaSchemaRDD = schemaRDD.toJavaSchemaRDD();
		// The results of SQL queries are DataFrames and support all the normal
		// RDD operations.
		// The columns of a row in the result can be accessed by ordinal.
		List<String> predictions = javaSchemaRDD.map(new Function<Row, String>() {
			public String call(Row row) {
				return row.toString();
			}
		}).collect();
		
		for(String prediction :predictions)
		{
			System.out.println("Prediction:: "+prediction);
		}
		
		sc.stop();

	}

	protected static Frame parse_test_file( Key outputKey, String fname ) 
	  {
		    File f = new File(fname);
		    NFSFileVec nfs = NFSFileVec.make(f);
		    return ParseDataset.parse(outputKey, nfs._key);
	  }
	  
}
