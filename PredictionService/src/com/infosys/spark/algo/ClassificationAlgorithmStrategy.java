package com.infosys.spark.algo;

import org.apache.spark.h2o.H2OContext;
import org.apache.spark.sql.api.java.JavaSQLContext;

import water.fvec.DataFrame;

public interface ClassificationAlgorithmStrategy 
{
	String trainAlgorithm(H2OContext h2oContext, JavaSQLContext sqlCtx,
			DataFrame train_table, DataFrame test_table, String[] ignoredCols,String responseColumn);
	
	void testAlgorithm(H2OContext h2oContext, JavaSQLContext sqlCtx,
			DataFrame train_table, DataFrame test_table, String[] ignoredCols,String responseColumn);
}
