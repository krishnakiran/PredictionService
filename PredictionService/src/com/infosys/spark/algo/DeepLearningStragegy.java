package com.infosys.spark.algo;

import hex.AUCData;
import hex.deeplearning.DeepLearning;
import hex.deeplearning.DeepLearningModel;
import hex.deeplearning.DeepLearningModel.DeepLearningParameters;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.h2o.H2OContext;
import org.apache.spark.sql.SchemaRDD;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import org.junit.Assert;

import water.Key;
import water.fvec.DataFrame;
import water.fvec.Frame;
import water.util.SB;

public class DeepLearningStragegy implements ClassificationAlgorithmStrategy 
{

	@Override
	public String trainAlgorithm(H2OContext h2oContext, JavaSQLContext sqlCtx,
			DataFrame train_table, DataFrame test_table, String[] ignoredCols,
			String responseColumn) 
	{
		return trainAlgo(h2oContext, sqlCtx, train_table, test_table, ignoredCols,responseColumn);
	}

	private String trainAlgo(H2OContext h2oContext, JavaSQLContext sqlCtx,
			DataFrame train_table, DataFrame test_table, String[] ignoredCols,
			String responseColumn) 
	{

		DeepLearningParameters params = new DeepLearningParameters();
		
        // populate deepLearningModel parameters
        params._destination_key = Key.make("dl.hex"+System.currentTimeMillis());
        params._train = train_table._key;
        params._valid = test_table._key;
        params._ignored_columns=ignoredCols;
        params._seed=113974111632823084l;
        params._rho=0.99;
        params._epsilon=1.0E-8;
        params._input_dropout_ratio=0.0;
        params._variable_importances=true;
        params._use_all_factor_levels=true;
        params._response_column = responseColumn; // last column is the response
        params._activation = DeepLearningParameters.Activation.Rectifier;
       // p._hidden = new int[]{2500, 2000, 1500, 1000, 500};
        params._train_samples_per_iteration = -2;//airlines_train_data.count() * H2O.getCloudSize(); //process 1500 rows per node per map-reduce step
        params._epochs = 10;//1.8 * (float) p._train_samples_per_iteration / train_table.numRows(); //train long enough to do 2 map-reduce passes (with scoring each time)
        params._hidden=new int[]{200,200};
        params._score_interval=5.0;
        // speed up training
        params._adaptive_rate = true; //disable adaptive per-weight learning rate -> default settings for learning rate and momentum are probably not ideal (slow convergence)
        //p._replicate_training_data = false; //avoid extra communication cost upfront, got enough data on each node for load balancing
        params._override_with_best_model = true; //no need to keep the best deepLearningModel around
        params._diagnostics = false; //no need to compute statistics during training
        //p._score_interval = 20; //score and print progress report (only) every 20 seconds
        //p._score_training_samples = 50; //only score on a small sample of the training set -> don't want to spend too much time scoring (note: there will be at least 1 row per chunk)

        
        DeepLearning dl = new DeepLearning(params);
        DeepLearningModel dModel = null;
        try
        {
        	dModel = dl.trainModel().get();
        	 if (dModel != null) 
        	 {
                 Assert.assertTrue(1000. * dModel.model_info().get_processed_total() / dModel.run_time > 20); //we expect at least a training speed of 20 samples/second (MacBook Pro: ~50 samples/second)
              }
        }
        finally
        {
        	if(dl!=null)
            dl.remove();
        }
		
		StringBuffer responseBuffer = new StringBuffer();
		
		DataFrame predict = null;
	    
	    Frame score = dModel.score(test_table);
	    
	    System.out.println("MSE::"+dModel.mse());
	    
	    predict = h2oContext.createDataFrame(score);
	    
	    hex.ModelMetricsBinomial mm = hex.ModelMetricsBinomial.getFromDKV(dModel,params.valid());
	    AUCData adata = mm._aucdata;
	    
	    
	    System.out.println("deepLearningModel Metrics***");
	    System.out.println(mm.toJsonString());
	    System.out.println("Area Under Curve::"+adata.AUC());
	    System.out.println("Accuracy");
		System.out.println(adata.accuracy());
		System.out.println("Confusion Matrix");
		System.out.println(adata.CM().toASCII());
		

		responseBuffer.append("\n*****Model Metrics*****\n");
		responseBuffer.append("\nMean Square Error::::"+dModel.mse());
	    responseBuffer.append("\n*****Area Under Curve::*****\n"+adata.AUC());
	    responseBuffer.append("\n*****Accuracy::*****\n"+adata.accuracy());
		responseBuffer.append("\n*****Confusion Matrix*****\n");
		responseBuffer.append(dModel.cm().toASCII());
		responseBuffer.append("\n*****Variable Importances*****\n"+dModel.varimp().toJsonString());
		
		System.out.println("deepLearningModel::"+dModel.varimp().toJsonString());
	      
		FileWriter fileWriter=null;
	    try 
	    {
	    	  System.out.println("Writing Java deepLearningModel to data/deepLearningModelJavadeepLearningModel.java");
	    	  fileWriter = new FileWriter("data/deepLearningModelJavadeepLearningModel.java",false);
		      SB sb = new SB();
		      dModel.toJava(sb);
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

	@Override
	public void testAlgorithm(H2OContext h2oContext, JavaSQLContext sqlCtx,
			DataFrame train_table, DataFrame test_table, String[] ignoredCols,
			String responseColumn) {
		// TODO Auto-generated method stub
		
	}
	

}
