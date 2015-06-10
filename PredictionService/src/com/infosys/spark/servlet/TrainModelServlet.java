package com.infosys.spark.servlet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.h2o.H2OContext;
import org.apache.spark.sql.api.java.DataType;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.api.java.StructField;
import org.apache.spark.sql.api.java.StructType;

import water.DKV;
import water.Scope;
import water.fvec.DataFrame;

import com.infosys.spark.algo.ClassificationAlgorithmEnum;
import com.infosys.spark.algo.ClassificationAlgorithmStrategy;
import com.infosys.spark.algo.StrategyFactory;
import com.infosys.spark.util.PropertiesUtility;

@WebServlet(name="TrainModelServlet",urlPatterns={"/TrainGLM"})
public class TrainModelServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;
	public TrainModelServlet() {
		super();
	}
	
	@Override
	public void init() throws ServletException 
	{
		
	}
	
	
	protected void doGet(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
	}

	protected void doPost(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException 
	{
		String trainFilePath = request.getServletContext().getRealPath("/")+PropertiesUtility.getProperty("CHURN_TRAIN_FILE_PATH");
		String schemaString = PropertiesUtility.getProperty("schemaString");
		
		JavaSparkContext sc = (JavaSparkContext) request.getServletContext().getAttribute("SPARK_JAVA_CONTEXT");
		JavaSQLContext sqlCtx = (JavaSQLContext) request.getServletContext().getAttribute("SQL_CONTEXT");
		H2OContext h2oContext = (H2OContext) request.getServletContext().getAttribute("H2O_CONTEXT");
		
		
		// Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<StructField>();
		for (String fieldName : schemaString.split(","))
		{
			fields.add(DataType.createStructField(fieldName,DataType.StringType, false));
		}
		
		StructType schema = DataType.createStructType(fields);
		JavaRDD<String> trainData = sc.textFile(trainFilePath);
		
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
		
		JavaRDD<Row>[] splits = trainRowRDD.randomSplit(new double[]{0.8, 0.2});
		JavaRDD<Row> trainingData = splits[0];
		JavaRDD<Row> testData = splits[1];

		JavaSchemaRDD trainingSchema = sqlCtx.applySchema(trainingData, schema);
		trainingSchema.registerTempTable("training_data");
		JavaSchemaRDD trainSchemaRDD = sqlCtx.sql("SELECT * FROM training_data");
		
		JavaSchemaRDD testSchema = sqlCtx.applySchema(testData, schema);
		testSchema.registerTempTable("testing_data");
		JavaSchemaRDD testSchemaRDD = sqlCtx.sql("SELECT * FROM testing_data");
		
		DataFrame train_table = h2oContext.createDataFrame(trainSchemaRDD.schemaRDD());
		DataFrame test_table = h2oContext.createDataFrame(testSchemaRDD.schemaRDD());
		
		String [] ignoredCols = PropertiesUtility.getProperty("ignoredCols").split(",");
		String responseCol = PropertiesUtility.getProperty("response_column");
		
		 // Convert response to categorical
		
		int ci = train_table.find(PropertiesUtility.getProperty("response_column"));
		Scope.track(train_table.replace(ci, train_table.vecs()[ci].toEnum())._key);
		DKV.put(train_table);
		
		int ci1 = test_table.find(PropertiesUtility.getProperty("response_column"));
		Scope.track(test_table.replace(ci1, test_table.vecs()[ci1].toEnum())._key);
		DKV.put(test_table);
		
		String algorithm = request.getParameter("algorithm");
		ClassificationAlgorithmStrategy algoStrategy=StrategyFactory.getInstance().getStrategyInstance(ClassificationAlgorithmEnum.valueOf(algorithm));
		String modelOutput=algoStrategy.trainAlgorithm(h2oContext,sqlCtx,train_table,test_table,ignoredCols,responseCol);
		response.setContentType("plain/text");
	    response.setCharacterEncoding("UTF-8");
	    response.getWriter().write(modelOutput);
	}
	
}
