package sparkKNN_MIN.spKNN_MIN.spKNN_MIN;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

import java.io.Serializable;
public class KNN_MIN implements Serializable {
	
	private JavaRDD<List<Double>> linesTrainDouble ;
	List <List<Double>> TestCompuesto;
	private int lmin;
	private int lmax;
	private int K_vecinos;
	

 public KNN_MIN (JavaRDD<List<Double>> linesTrainDouble, List <List<Double>> TestCompuesto,int lmin,int lmax,int K_vecinos) {
		    this.linesTrainDouble=linesTrainDouble;
			this.TestCompuesto=TestCompuesto;
			this.lmin=lmin;
			this.lmax=lmax;
			this.K_vecinos=K_vecinos;
				}
public List <Tuple2<Tuple2<Double,Double>,List<Double>>> RetornaClaseRealClasePredecidaLineaTest(){
	 
	 // Tables of k neighbors for each test line
    List< List <Double>> DistanciasMinimas = new ArrayList <List<Double>>();
	  List< List <Double>> ClasesPredecidas = new ArrayList <List<Double>>();
	 
	   Double DistanciaMAX=999999999.0;
	   Double ClaseMAX=999999999.0;
	   int NumMuestras=TestCompuesto.size();
	   for(int i=0; i<NumMuestras; i++)
	   {
		
		   List<Double> LineaDistanciasMinimas=new ArrayList <Double>();
			  List<Double> LineaClasesPredecidas=new ArrayList <Double>();
		   for(int k=0; k<K_vecinos; k++)
		   {
			   
		    LineaDistanciasMinimas.add(DistanciaMAX);
		    LineaClasesPredecidas.add(ClaseMAX);
		   }
		   DistanciasMinimas.add(LineaDistanciasMinimas);
		   ClasesPredecidas.add(LineaClasesPredecidas);
	
	   }
	 
 
	   // THE CALCULATION OF THE// LISTS OF K NEIGHBORS FOR EACH TEST RECORD WILL BE EXECUTED IN MULTIPLE PARTITIONS,
	   // IN THE END THEY HAVE TO BE UNIFIED
	// 
   JavaRDD<List<Tuple2<Double,Double>>> listaKPorParticion =linesTrainDouble.mapPartitions(s->{
  	 System.out.println(" Start treating a train partition   ");

			 List<Double> lineadelTrain =new ArrayList<Double>();
			 Double Distancia=0.0;
			 Double ClasePredecida = 0.0;
			 
			 while (s.hasNext()) {
				 lineadelTrain =	 s.next();
				 
				 for(int i =0;i<NumMuestras;i++)
				 {
					 List<Double> LineaDistanciasw=DistanciasMinimas.get(i);
					  List<Double> LineaClasesw=ClasesPredecidas.get(i);
			     // calculate distance and class 
				 Distancia=	 CalculaDistancia( lineadelTrain, TestCompuesto.get(i),  lmin,  lmax);
				 ClasePredecida= CalculaClase(lineadelTrain,  lmin, lmax);
				//************************************  
		    	 // add to the array of K nearest neighbors
				 int SwCambio=0;
		         for(int w=0; w<K_vecinos; w++)
		         {
		        	 
		        	if (Distancia < LineaDistanciasw.get(w))
		        	{
		        		
		        		SwCambio=1;
		        		if (LineaDistanciasw.get(w)==DistanciaMAX)
		        		{}
		        		// move the entire array down one position to enter the distance in order
		        		else { 
		        			for(int x=K_vecinos-1; x>w; x--)
		        				
		        			{
		        				
		        				LineaDistanciasw.set(x, LineaDistanciasw.get(x-1));
		        				
		        				LineaClasesw.set(x, LineaClasesw.get(x-1));
		        			}
		        			
		        		}// end if (DistanciasMinimas.get(w)==DistanciaMAX) else
		        			
		        		LineaDistanciasw.set(w, Distancia);
		        		LineaClasesw.set(w, ClasePredecida);
		        		break;
		        	} // end of if (Distancia < DistanciasMinimas.get(i).get(w))
		       	} // end for w	
		         if(SwCambio==1)
		         {
		        	 DistanciasMinimas.set(i, LineaDistanciasw);
		        	 ClasesPredecidas.set(i, LineaClasesw);
		         }
		 } // end for int i=0
		//************************************ 
				 
			
			
	 }// end while s.hasNext
			 System.out.println(" It comes out with a matrix of Minimum Distances  " + DistanciasMinimas.toString());
			 
			 List<List<Tuple2<Double,Double>>> DistanciaClasePredecida = new ArrayList<List<Tuple2<Double,Double>>>();
			 for(int j =0;j<NumMuestras;j++)
			 {
				 List<Double> LineaDistanciasj=DistanciasMinimas.get(j);
				  List<Double> LineaClasesj=ClasesPredecidas.get(j);
				  List<Tuple2<Double,Double>> tt = new ArrayList <Tuple2<Double,Double>>();
				  for(int x =0;x<K_vecinos;x++)
					 {
					  Tuple2<Double,Double> t=new Tuple2(LineaDistanciasj.get(x),LineaClasesj.get(x));
					  tt.add(t);
					 }
				  DistanciaClasePredecida.add(tt);
			 } // fin for j
			 
			 
		    	return DistanciaClasePredecida.iterator();
		    	 
		     }); // end mappartitions
 

  List <List<Tuple2<Double,Double>>> ListlistaKPorParticion = new ArrayList <List<Tuple2<Double,Double>>> ();
  ListlistaKPorParticion=listaKPorParticion.collect();

 Double Distancia=0.0;
 Double ClasePredecida=0.0;
   //---------------------------------------------------------------------------------------------------
  
   int NumParticiones = (ListlistaKPorParticion.size())/(NumMuestras);
   System.out.println(" Number of partitions MAPPARTITION  " + NumParticiones);
   for(int j=0;j<NumParticiones;j++)
   {
  	 
		     for(int i =0;i<NumMuestras;i++)
			 {
				 List<Double> LineaDistanciasw=DistanciasMinimas.get(i);
				  List<Double> LineaClasesw=ClasesPredecidas.get(i);
				  List<Tuple2<Double,Double>> WlistaKPorParticion = new ArrayList <Tuple2<Double,Double>> (NumMuestras*K_vecinos);
			    	 WlistaKPorParticion=ListlistaKPorParticion.get(i+j*NumMuestras);
	     
			 for(int z=0; z<K_vecinos; z++)
			 {
				// int IndiceTupla=i*K_vecinos +z;
				 Distancia=WlistaKPorParticion.get(z)._1;
				 ClasePredecida=WlistaKPorParticion.get(z)._2;
			//************************************  
			 // add to the array of K nearest neighbors
			 int SwCambio=0;
		     for(int w=0; w<K_vecinos; w++)
		     {
		    	 
		    	if (Distancia < LineaDistanciasw.get(w))
		    	{
		    		
		    		SwCambio=1;
		    		if (LineaDistanciasw.get(w)==DistanciaMAX)
		    		{}
		    		// move the entire array down one position to enter the distance in order
		    		else { 
		    			for(int x=K_vecinos-1; x>w; x--)
		    				
		    			{
		    				
		    				LineaDistanciasw.set(x, LineaDistanciasw.get(x-1));
		    				
		    				LineaClasesw.set(x, LineaClasesw.get(x-1));
		    			}
		    			
		    		}// end if (DistanciasMinimas.get(w)==DistanciaMAX) else
		    			
		    		LineaDistanciasw.set(w, Distancia);
		    		LineaClasesw.set(w, ClasePredecida);
		    		break;
		    	} // end of if (Distancia < DistanciasMinimas.get(i).get(w))
		   	} // end for w	
			
		     if(SwCambio==1)
		     {
		    	 DistanciasMinimas.set(i, LineaDistanciasw);
		    	 ClasesPredecidas.set(i, LineaClasesw);
		     }
			 } // end for z
		 } // end for int i=0
} // end for int j=0
//************************************ 
   
// Finished the calculation of the distance of each line of the test with each line of the train
   // in each of the partitions
//The array of the K-neighbors is explored and is added in an array
//the "votes" obtained by each class
   List <Tuple2<Tuple2<Double,Double>,List<Double>>> LisClaseRealClasePredecidaLineaTest = new ArrayList<Tuple2<Tuple2<Double,Double>,List<Double>>>();
   List <Double> VotosClase = new ArrayList <Double>();
	   List <Double> ClasesVotadas = new ArrayList <Double>();
	// Finished the calculation of the distance of each line of the test with each line of the train
	     // in each of the partitions
	// The array of the K-neighbors is explored and is added in an array
	// the "votes" obtained by each class
	   for(int v =0;v<NumMuestras;v++)
	   {
	   Double WvotosClase =0.0;
	   Double NumVotos=0.0;
	   Double ClaseEstimada=0.0;
	   Double ClaseReal=0.0;
	   List<Double> LineaDistanciasMinimas=new ArrayList <Double>();
		  List<Double> LineaClasesPredecidas=new ArrayList <Double>();
		  LineaDistanciasMinimas= DistanciasMinimas.get(v); 
		  LineaClasesPredecidas=ClasesPredecidas.get(v);
	    VotosClase.clear();
	    ClasesVotadas.clear();
	    for(int x=0; x<K_vecinos; x++)
	    {
	        if ( ClasesVotadas.contains(LineaClasesPredecidas.get(x)))
	        {
	        	for(int y=0; y<ClasesVotadas.size(); y++)
	        		
	        	{
	        		if( ClasesVotadas.get(y)==LineaClasesPredecidas.get(x))
	        		{
	        			WvotosClase=VotosClase.get(y);
	        			WvotosClase++;
	        		    VotosClase.set(y, WvotosClase);
	        			break; // salir de  for int y=0
	        		}
	        		 
	        	} // end for int y=0
	        } // end if ( ClasesVotadas.contains(ClasesPredecidas.get(x)))
	        	else
	        	{
	        		ClasesVotadas.add(LineaClasesPredecidas.get(x));
	    			VotosClase.add(1.0);
	    			
	        		
	             } // end else if ( ClasesVotadas.contains(ClasesPredecidas.get(x)))
	    } // end for(int x=0; x<K_vecinos; x++)
	 // the array of classes with their votes is explored and the most voted is taken
	    NumVotos=0.0;
	    for(int z=0; z<ClasesVotadas.size(); z++)
	    	
	    {
	    	if(VotosClase.get(z)> NumVotos)
	        {
	         ClaseEstimada=ClasesVotadas.get(z);
	         NumVotos=VotosClase.get(z);
	        }
	    } // for(int z=0; z<ClasesVotadas.size(); z++)
	    
	 //   List <Tuple2<Tuple2<Double,Double>,Double>> LisClaseRealClasePredecidaLineaTest = new ArrayList<Tuple2<Tuple2<Double,Double>,Double>>();
	    ClaseReal=CalculaClase(TestCompuesto.get(v),  lmin,  lmax);
	   
	    Tuple2<Double,Double> ClaseRealClasePredecida = new Tuple2<Double,Double>(ClaseReal,ClaseEstimada);
	    
	    Tuple2<Tuple2<Double,Double>,List<Double>> ClaseRealClasePredecidaLineaTest = new Tuple2<Tuple2<Double,Double>,List<Double>> (ClaseRealClasePredecida,TestCompuesto.get(v));

	    LisClaseRealClasePredecidaLineaTest.add(ClaseRealClasePredecidaLineaTest);
}  // end  for v
	   return LisClaseRealClasePredecidaLineaTest;
}// end method RetornaClaseRealClasePredecidaLineaTest

static Double CalculaDistancia(List<Double> lineasTrainAComparar,List<Double> lineaTestAComparar,  int lmin, int lmax)
{
	Double  sum=0.0;
	Double  distancia=0.0;
    
    // for at field level   
    	for(int l=lmin; l<lmax; l++){
            if ((l>=lineasTrainAComparar.size()) || (l>=lineaTestAComparar.size()))
            		{
            	System.out.println("  INDEX OVERFLOW");
            	break;
            		}
            distancia=lineasTrainAComparar.get(l)- lineaTestAComparar.get(l);
            distancia=distancia*distancia;
            sum+=distancia;
    	} // fin for l
   	   distancia= Math.sqrt(sum);
	       return distancia;
}
static Double CalculaClase(List<Double> lineasAComparar, int lmin, int lmax)
{
	 if (lmin==0)
		 return lineasAComparar.get(lmax);
		 else
			 return lineasAComparar.get(0);	 
	
}

}

