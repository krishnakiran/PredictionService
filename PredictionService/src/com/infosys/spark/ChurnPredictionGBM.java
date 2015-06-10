/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.infosys.spark;

import hex.ConfusionMatrix;
import hex.tree.gbm.GBM;
import hex.tree.gbm.GBMModel;
import hex.tree.gbm.GBMModel.GBMParameters.Family;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;

import water.DKV;
import water.H2O;
import water.H2OApp;
import water.Iced;
import water.Key;
import water.MRTask;
import water.Scope;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.NFSFileVec;
import water.parser.ParseDataset;
import water.util.SB;

/**
 * H2O bootstrap example.
 *
 * The example implements a library which provides a
 * method putting given greetings message into K/V store.
 */
public class ChurnPredictionGBM  {

  public static final String MSG = "Hello %s!";

  /** Simple Iced-object which will be serialized over network */
  public static final class StringHolder extends Iced {
    final String msg;

    public StringHolder(String msg) {
      this.msg = msg;
    }

    public String hello(String name) {
      return String.format(msg, name);

    }
  }

  /**
   * Creates a key and value holding a simple message in {@link water.ChurnPredictionGBM.GBMH2OJavaDroplet.StringHolder}.
   *
   * @return key referencing stored value.
   */
  public static final Key hello() {
    Key vkey = Key.make("hello.key");
    StringHolder value = new StringHolder(MSG);
    DKV.put(vkey, value);

    return vkey;
  }
  
  protected static Frame parse_test_file( Key outputKey, String fname ) 
  {
	    File f = new File(fname);
	    NFSFileVec nfs = NFSFileVec.make(f);
	    return ParseDataset.parse(outputKey, nfs._key);
  }
  
  private static class CompErr extends MRTask<CompErr> {
	    double _sum;
	    @Override public void map( Chunk resp, Chunk pred ) {
	      double sum = 0;
	      for( int i=0; i<resp._len; i++ ) {
	        double err = resp.atd(i)-pred.atd(i);
	        sum += err*err;
	      }
	      _sum = sum;
	    }
	    @Override public void reduce( CompErr ce ) { _sum += ce._sum; }
	  }


  /** Application Entry Point */
  public static void main(String[] args) 
  {
	  // Setup cloud name
	  String[] args1 = new String[] { "-name", "h2o_test_cloud"};
	  // Run H2O and build a cloud of 1 member
	  H2OApp.main(args1);
	  H2O.waitForCloudSize(1, 10*1000);
	  
	  ChurnPredictionGBM gbmh2oJavaDroplet = new ChurnPredictionGBM();
	  gbmh2oJavaDroplet.trainAndEvaluate();
	  gbmh2oJavaDroplet.predict();
  }
  
  public void trainAndEvaluate()
  {
	  GBMModel gbm = null;
	  Frame fr = null, fr2 = null,test_data=null;
	  try
	  {
		  fr = parse_test_file(Key.make(),"data/churn-trainingset.csv");
		  test_data = parse_test_file(Key.make(),"data/churn-testingset.csv");
		  
		  GBMModel.GBMParameters parms = new GBMModel.GBMParameters();
		  
		  int ci = fr.find("churn");    // Convert response to categorical
	      Scope.track(fr.replace(ci, fr.vecs()[ci].toEnum())._key);
	      DKV.put(fr);                    // Update frame after hacking it
	      
	      parms._train = fr._key;
	      parms._valid=fr._key;
	      parms._loss = Family.multinomial;
	      parms._response_column = "churn"; 
	      parms._ntrees = 10;
	      parms._max_depth = 5;
	      parms._min_rows = 1;
	      parms._nbins = 20;
	      parms._ignored_columns=new String[]{"area"};
	      parms._learn_rate = 1.0f;
	      parms._score_each_iteration=true;
	      
	      GBM job = null;
	      try 
	      {
	        job = new GBM(parms);
	        gbm = job.trainModel().get();
	      } 
	      finally 
	      {
	        if (job != null) job.remove();
	      }
	      
	      fr2 = gbm.score(fr);
	      CompErr compErr = new CompErr();
	      double sq_err = compErr.doAll(job.response(),fr2.vecs()[0])._sum;
	      double mse = sq_err/fr2.numRows();
	      
	      System.out.println("***Model Details ***");
	      System.out.println(gbm.toJsonString());
	      
	      System.out.println("***Validation Details ***");
	      System.out.println(fr2.toJsonString());
	      
	      System.out.println("Root mean Square Error::"+mse);
      
	      hex.ModelMetricsBinomial mm = hex.ModelMetricsBinomial.getFromDKV(gbm,parms.valid());
	      System.out.println("Model Metrics***");
	      System.out.println(mm.toJsonString());
	      System.out.println("Confusion Matrics***");
	      double auc = mm._aucdata.AUC();
	      ConfusionMatrix cmf1 = mm._aucdata.CM();
	      System.out.println(cmf1.toJsonString());
	      
	      SB sb = new SB();
	      gbm.toJava(sb);
	      System.out.println("***Java Model***");
	      System.out.println(sb._sb.toString());
	  }
     finally {
      if( fr  != null ) fr .remove();
      if( fr2 != null ) fr2.remove();
      if( gbm != null ) gbm.delete();
    }
	  
  }
  
  
  public void predict() 
  {
	    GBMModel gbm = null;
	    GBMModel.GBMParameters parms = new GBMModel.GBMParameters();
	    Frame pred=null, res=null;
	    Scope.enter();
	    try {
	      Frame train = parse_test_file(Key.make(),"data/churn-trainingset.csv");
	      train.remove("area").remove();     // Remove unique ID
	      int ci = train.find("churn");
	      Scope.track(train.replace(ci, train.vecs()[ci].toEnum())._key);   // Convert response 'Angaus' to categorical
	      DKV.put(train);                    // Update frame after hacking it
	      parms._train = train._key;
	      parms._response_column = "churn"; // Train on the outcome
	      parms._loss = Family.multinomial;

	      GBM job = new GBM(parms);
	      gbm = job.trainModel().get();
	      job.remove();

	      pred = parse_test_file(Key.make(),"data/churn-testingset.csv" );
	      pred.remove("churn").remove();    // No response column during scoring
	      res = gbm.score(pred);
	      
	      System.out.println(res.toJsonString());
	      
	      InputStream predictions=res.toCSV(true, false);
	      try {
			IOUtils.copy(predictions, System.out);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    } finally {
	      parms._train.remove();
	      if( gbm  != null ) gbm .delete();
	      if( pred != null ) pred.remove();
	      if( res  != null ) res .remove();
	      Scope.exit();
	    }
	  }
   
  
}

