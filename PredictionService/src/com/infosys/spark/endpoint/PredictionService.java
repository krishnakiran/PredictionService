package com.infosys.spark.endpoint;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import org.springframework.stereotype.Service;

@Path("/PredictionService")
public class PredictionService 
{

	@Path("/ping")
	@GET
	@Produces("text/plain")
	public String ping(@QueryParam("text")String text) 
	{
		return "Hi "+text;
	}
	
}
