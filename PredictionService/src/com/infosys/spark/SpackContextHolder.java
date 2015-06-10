package com.infosys.spark;

import org.apache.spark.api.java.JavaSparkContext;

public class SpackContextHolder 
{
	private static class Holder {
        static final JavaSparkContext INSTANCE = new JavaSparkContext("local", "Classifier_App");
    }
	
	public static JavaSparkContext getInstace()
	{
        return Holder.INSTANCE;
	}
}
