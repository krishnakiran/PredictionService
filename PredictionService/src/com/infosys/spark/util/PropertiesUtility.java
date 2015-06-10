package com.infosys.spark.util;

import java.io.IOException;
import java.util.Properties;

public class PropertiesUtility 
{
	private static Properties appProperties=null;
	private static final String APP_PROPERTIES = "/resources/app.properties";
	    static
	    {
	    	if (null == appProperties) 
		      {
				appProperties = new Properties();
	  		try {
	  				appProperties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(APP_PROPERTIES));
				} catch (IOException e) {
					e.printStackTrace();
				}
			  }
	    }
	    
	    public static String getProperty(String proepertyName)
		{
			return appProperties.getProperty(proepertyName);
		}
}
