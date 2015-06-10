package com.infosys.spark.algo;

import hex.AUCData;
import hex.tree.gbm.GBM;
import hex.tree.gbm.GBMModel;
import hex.tree.gbm.GBMModel.GBMParameters.Family;
import hex.utils.MSETsk;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.h2o.H2OContext;
import org.apache.spark.sql.SchemaRDD;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;

import water.Key;
import water.fvec.DataFrame;
import water.fvec.Frame;
import water.util.SB;


public class GBMStrategy implements ClassificationAlgorithmStrategy
{
	@Override
	public String trainAlgorithm(H2OContext h2oContext, JavaSQLContext sqlCtx,
			DataFrame train_table, DataFrame test_table, String[] ignoredCols,String responseColumn) 
	{
		return trainAlgo(h2oContext, sqlCtx, train_table, test_table, ignoredCols,responseColumn);
	}

	@Override
	public void testAlgorithm(H2OContext h2oContext, JavaSQLContext sqlCtx,
			DataFrame train_table, DataFrame test_table, String[] ignoredCols,String responseColumn) 
	{
		
	}
	
	private  String trainAlgo(H2OContext h2oContext, JavaSQLContext sqlCtx,
			DataFrame train_table, DataFrame test_table, String[] ignoredCols,String responseColumn) 
	{
			GBMModel.GBMParameters params = new GBMModel.GBMParameters();
			params._train = train_table._key;
			params._valid = test_table._key;
			params._loss = Family.multinomial;
			params._response_column = responseColumn;
			params._ntrees = 10;
			params._max_depth = 5;
			params._min_rows = 1;
			params._nbins = 20;
			params._ignored_columns = ignoredCols;
			params._learn_rate = 1.0f;
			params._score_each_iteration = true;
			params._destination_key = Key.make("gbm.hex"+ System.currentTimeMillis());

			
			GBM job = null;
			GBMModel gbm = null;
			try {
				job = new GBM(params);
				gbm = job.trainModel().get();
			} finally {
				if (job != null)
					job.remove();
			}
			
			StringBuffer responseBuffer = new StringBuffer();
			
			DataFrame predict = null;
		    
		    Frame score = gbm.score(test_table);
		    MSETsk mse = new MSETsk().doAll(score.anyVec(), test_table.vec(gbm._output.responseName()));
		    System.out.println("MSE::"+mse._resDev/test_table.numRows());
		    
		    predict = h2oContext.createDataFrame(score);
		    
		    hex.ModelMetricsBinomial mm = hex.ModelMetricsBinomial.getFromDKV(gbm,params.valid());
		    AUCData adata = mm._aucdata;
		    
		    
		    System.out.println("Model Metrics***");
		    System.out.println(mm.toJsonString());
		    System.out.println("Area Under Curve::"+adata.AUC());
		    System.out.println("Accuracy");
			System.out.println(adata.accuracy());
			System.out.println("Confusion Matrix");
			System.out.println(adata.CM().toASCII());
			
			responseBuffer.append("\n*****Model Metrics*****\n");
			responseBuffer.append("\nMean Square Error::::"+mse._resDev/test_table.numRows());
		    responseBuffer.append("\n*****Area Under Curve::*****\n"+adata.AUC());
		    responseBuffer.append("\n*****Accuracy::*****\n"+adata.accuracy());
			responseBuffer.append("\n*****Confusion Matrix*****\n");
			responseBuffer.append(adata.CM().toASCII());
			responseBuffer.append("\n*****Variable Importances*****\n"+gbm._output._variable_importances.toString());
			System.out.println("Model::"+gbm._output._variable_importances.toJsonString());
		      
			FileWriter fileWriter=null;
		    try 
		    {
		    	  System.out.println("Writing Java Model to data/GBMJavaModel.java");
		    	  fileWriter = new FileWriter("data/GBMJavaModel.java",false);
			      SB sb = new SB();
			      gbm.toJava(sb);
			      fileWriter.write(sb.toString());
			} catch (IOException e) 
		    {
				e.printStackTrace();
			}
		    finally
		    {
		    	try {
					fileWriter.close();
				} catch (IOException e) {
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
			
			responseBuffer.append("\n*****Predictions on Test Data*****\n");
			for(String prediction :predictions)
			{
				System.out.println("Prediction:: "+prediction);
				responseBuffer.append(prediction + "\n");
			}
			return responseBuffer.toString();
	}
}
