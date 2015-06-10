package com.infosys.spark.algo;

import hex.AUCData;
import hex.ModelMetrics;
import hex.glm.GLM;
import hex.glm.GLMModel;
import hex.glm.GLMModel.GLMParameters;
import hex.glm.GLMModel.GLMParameters.Family;

import java.util.List;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.h2o.H2OContext;
import org.apache.spark.sql.SchemaRDD;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;

import com.infosys.spark.util.PropertiesUtility;

import water.Key;
import water.fvec.DataFrame;


public class GLMStrategy implements ClassificationAlgorithmStrategy
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
		 GLM job = null;
	 	 GLMModel glmModel=null;
		try
		{
			GLMParameters params = new GLMParameters(Family.binomial);
			params._response_column = responseColumn;
			params._ignored_columns = ignoredCols;
			params._train = train_table._key;
			params._valid = test_table._key;
			params._n_folds = 10;
			params._score_each_iteration = false;
			params._link = GLMParameters.Link.logit;
			params._alpha = new double[] { 0.5 };
			params._lambda = new double[] { 1e-5 };
			params._solver = GLMParameters.Solver.L_BFGS;
			params._destination_key = Key.make("glm.hex"+ System.currentTimeMillis());

			job = new GLM(params);
			job._nclass = 1;
			glmModel = job.trainModel().get();
			glmModel.addMetrics(new ModelMetrics(glmModel, params.valid()));

			DataFrame predict = null;

			predict = h2oContext.createDataFrame(glmModel.score(test_table));

			StringBuffer responseBuffer = new StringBuffer();

			hex.ModelMetricsBinomial mm = hex.ModelMetricsBinomial.getFromDKV(
					glmModel, params.valid());
			AUCData adata = mm._aucdata;
			System.out.println("Accuracy");
			System.out.println(adata.accuracy());
			System.out.println("Confusion Matrix");
			System.out.println(adata.CM().toASCII());

			responseBuffer.append("\n*****Model Metrics*****\n");
			responseBuffer.append(adata.toJsonString());
			responseBuffer.append("\n*****Confusion Matrix*****\n");
			responseBuffer.append(adata.CM().toASCII());

			SchemaRDD schemaRDD = h2oContext.asSchemaRDD(predict,
					sqlCtx.sqlContext());
			JavaSchemaRDD javaSchemaRDD = schemaRDD.toJavaSchemaRDD();
			List<String> predictions = javaSchemaRDD.map(
					new Function<Row, String>() {
						public String call(Row row) {
							return row.toString();
						}
					}).collect();
			responseBuffer.append("\n*****Predictions on Test Data*****\n");
			for (String prediction : predictions) {
				System.out.println("Prediction:: " + prediction);
				responseBuffer.append(prediction + "\n");
			}
			return responseBuffer.toString();
		}
		finally
		{
			if (job != null)
				job.remove();
		}
	}
}
