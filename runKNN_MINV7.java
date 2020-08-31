package sparkKNN_MIN.spKNN_MIN.spKNN_MIN;

/* Licensed to the Apache Software Foundation (ASF) under one or more
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

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.Partition;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.function.FlatMapFunction;
import java.util.Arrays;
import java.util.List;
import java.util.Iterator;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



import org.apache.spark.sql.Dataset;

import org.apache.spark.api.java.function.ForeachFunction;

import org.apache.spark.sql.KeyValueGroupedDataset;  

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;



import org.apache.spark.storage.StorageLevel;

import scala.reflect.ClassTag;

import org.apache.spark.broadcast.Broadcast;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;


public final class runKNN_MINV7 {
private static final Pattern SPACE = Pattern.compile(" ");

 public static void main(String[] args) throws Exception {
   
   if (args.length < 1) {
     System.err.println("Error, no parameters have been passed to runKNN_MIN <file>");
     System.exit(1);
   }
   long Inicio=System.nanoTime();
   Logger root = (Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
   int numPartitionMap =4;

   SparkConf conf =  new SparkConf(); 
   
 // added for increased memory needed by spark
   conf.set("spark.testing.memory", "8885500000000");
 
   
  // https://stackoverrun.com/es/q/11564247
   // conf.setMaster("spark://172.1.1.1:7077");
  // conf.set("spark.storage.memoryFraction", "1") ;
   
   SparkSession spark = SparkSession
     .builder()
     .config(conf) 
     .appName("runKNN_MINV7")
     .master("local[2]") 
          
     .getOrCreate();
   
  // spark.logWarning(null);
   
   JavaRDD<String> linesTrain = spark.read().textFile(args[0]).javaRDD();
   JavaRDD<List<Double>> linesTrainDouble = linesTrain.map(new Function<String, List <Double>>() {
       public List<Double> call(String s) {
    	            String[] linea = s.split(",");
                    List <Double> dobles=new ArrayList<Double>();
                          for(int i=0; i<linea.length; i++){
                           dobles.add(Double.parseDouble(linea[i]));
                       }
                return dobles;

} 
});
		   
   JavaRDD<String> linesTest = spark.read().textFile(args[1]).javaRDD();
   JavaRDD<List<Double>> linesTestDouble = linesTest.map(new Function<String, List <Double>>() {
       public List<Double> call(String s) {
    	            String[] linea = s.split(",");
                    List <Double> dobles=new ArrayList<Double>();
                          for(int i=0; i<linea.length; i++){
                           dobles.add(Double.parseDouble(linea[i]));
                       }
                return dobles;

} 
}).persist(StorageLevel.MEMORY_AND_DISK()); 

 
// the number of fields or characteristics to consider, including the label
// comes in parameter 2
   int NumCampos =Integer.parseInt(args[2]);
// parameter 3 indicates if the label comes in the first position
   // or at the last of the field list
   // if it is zero the label comes in the first position, otherwise it comes in the last
   int lmin;
   int lmax;
   if (Integer.parseInt(args[3])==0)
   { lmin=1;
     lmax=NumCampos;
   }
   else
   { lmin=0;
   lmax=NumCampos-1;
 } 
     int K_vecinos = Integer.parseInt(args[4]);
    
 
     
     List <List<Double>> TestCompuesto = linesTestDouble.collect();
     linesTestDouble.unpersist();

   // A KNN_MIN class is defined with the only object to be able to apply a Pipeline
    KNN_MIN knn= new KNN_MIN(linesTrainDouble, TestCompuesto, lmin,lmax, K_vecinos);
  List <Tuple2<Tuple2<Double,Double>,List<Double>>> LisClaseRealClasePredecidaLineaTest = new ArrayList<Tuple2<Tuple2<Double,Double>,List<Double>>>();
  Pipeline pipeline = new Pipeline();
  
  LisClaseRealClasePredecidaLineaTest = knn.RetornaClaseRealClasePredecidaLineaTest();  
	   Double ContAciertos = 0.0;
	   Double ContFallos = 0.0;
	
	  for (int i=0;i< LisClaseRealClasePredecidaLineaTest.size();i++) 
	    {
		  System.out.println(" True class  " + (LisClaseRealClasePredecidaLineaTest.get(i)._1)._1 +
				  " Predicted Class  " + (LisClaseRealClasePredecidaLineaTest.get(i)._1)._2	 + 
				  " Test Line " + LisClaseRealClasePredecidaLineaTest.get(i)._2);
	    	if (((LisClaseRealClasePredecidaLineaTest.get(i)._1)._1).equals((LisClaseRealClasePredecidaLineaTest.get(i)._1)._2))
	    		ContAciertos++;
	     else
	    	 ContFallos++;
	    }
	  
	  System.out.println(" Hits " + ContAciertos + " Mistakes " + ContFallos + " Precision " + ContAciertos/(ContAciertos+ ContFallos));   
	  
     Double FinalParcial =(System.nanoTime()-Inicio)/1000000000.0;
     System.out.println("Total run time= " + FinalParcial );
     
     spark.stop();
     
      
     
     System.exit(0);
           
 }


  
}
