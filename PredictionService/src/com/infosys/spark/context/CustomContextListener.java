package com.infosys.spark.context;

import java.io.File;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.h2o.H2OContext;
import org.apache.spark.sql.api.java.JavaSQLContext;

@WebListener
public class CustomContextListener implements ServletContextListener {

	private JavaSparkContext sc=null;
	
    public void contextInitialized(ServletContextEvent servletContextEvent)
    {
    	//create attributes for storing test files. 
    	String rootPath = System.getProperty("catalina.home");
    	ServletContext ctx = servletContextEvent.getServletContext();
    	String relativePath = "temp";
    	System.out.println("RelativePath ::"+relativePath);
    	File file = new File(rootPath + File.separator + relativePath);
    	if(!file.exists()) file.mkdirs();
    	System.out.println("File Directory created to be used for storing files");
    	ctx.setAttribute("FILES_DIR_FILE", file);
    	ctx.setAttribute("FILES_DIR", rootPath + File.separator + relativePath);
    	
    	//initialize spark and h2o contexts
    	SparkConf conf = new SparkConf(true).setMaster("local").setAppName("AirLines");
		sc = new JavaSparkContext(conf);
		H2OContext h2oContext = new H2OContext(sc.sc()).start();
		JavaSQLContext sqlCtx = new JavaSQLContext(sc);
		ctx.setAttribute("SPARK_JAVA_CONTEXT", sc);
		ctx.setAttribute("H2O_CONTEXT", h2oContext);
		ctx.setAttribute("SQL_CONTEXT", sqlCtx);
    }

	public void contextDestroyed(ServletContextEvent servletContextEvent) 
	{
		sc.stop();
		
	}
}