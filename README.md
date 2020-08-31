# kNN-MIN: A Spark-based design of the k-Neighbors Neighbors classifier for big data, using minimal resources and minimal code.

Its simplicity allows it to be tested directly from Eclipse with big data files such as HIGGS and SUSY. It is with these large files that it manifests its strength both in speed and precision, decreasing when used with smaller files.

Instead of making a join of the train file with the test file, subsequently performing the appropriate reduce. In the KNN_MIN, it has been preferred to transfer the test file to an array in memory, make a map of the train and for each line of the train explore the memory array with the test data, calculate the distance and for each line of the test, Once the distance calculation is finished, keep a two-dimensional table, with one dimension the size of the test array and another dimension K_neighbors, this avoids the tremendous expansion of a join of the test file with the train and its subsequent reduction. It can be said that the reduce is replaced by partial reduces. In turn, the main weakness of the algorithm is pointed out, which is oriented to reduced test files. The tests have been done with test files of 20 records.

KNN_MIN is built in Java and has been structured, minimally, with a director module and a class with a method and two functions. A complete and well explained guide to java, scala and phyton with spark can be found at https://spark.apache.org/docs/2.2.0/rdd-programming-guide.html



### Cite this software as:
 ** Alfonso Blanco Garcï¿½a ** kNN-MIN: A Spark-based design of the k-Neighbors Neighbors classifier for big data, using minimal resources and minimal code.



#How to use

## Previous requirements
It has been tested with the following versions of products, although it will surely be able to run with previous versions:
- Eclipse oxygen 3.0
- Java 8
- spark 2.4.0



## How to run

KNN_MIN expects to receive as parameters: the name of the train file, the name of the test file, the number of fields, if the class field is in the first position (0) or in the last (1) and the number of neighbors.

For example to run a test with HIGGS:
In eclipse we right-click on the runKNN_MINV7 module and choose run as and then run configurations in the arguments tab, it should appear HIGGS.csv HIGGS20Test.txt 29 0 4, where HIGGS20Test.txt is a file of 20 records that serves as a test file (The HIGGS20text.txt and SUSY20text.txt files are included in the module, in which 20 appears in the name), 29 indicates that HIGGS has 29 fields, 0 that the class field is in the first position and 4 that 4 neighbors are considered . The VM arguments window should contain -Dlog4j.configuration = log4j.properties.template.

It is assumed that you have HIGGS.csv in the directory C: \ eclipse-workspace-new \ KNN_MIN, it is not included in the distribution, due to its large size. It can be downloaded from https://archive.ics.uci.edu/ml/datasets/HIGGS


In the case of SUSY, the parameter line would be: SUSY.csv SUSY20Test.txt 19 0 4.

SUSY can be downloaded from https://archive.ics.uci.edu/ml/datasets/SUSY

It can also be run in batch, for which it has to be exported from eclipse to a JAR. Commands: Export-> Jar file and successive next, in the last window a request appears to give the name of the main class, click on the browse button and type runKNN-MINV7, which will export the KNN_MIN file. jar inside the directory C:\Spark\spark-2.4.0-bin-hadoop2.7 \ bin \ KNN_MIN.jar
 Located in that directory using the system command line, Cd C:\Spark\spark-2.4.0-bin-hadoop2.7\bin

KNN_MIN.JAR is also attached, which would be copied to the directory C:\Spark\spark-2.4.0-bin-hadoop2.7\bin

and then, from that directory, assuming HIGGS.csv is in c: directory, the same as HIGGS20Text.txt (which goes with the supplied set of files), running the comand:

spark-submit --master local[2] --executor-memory 2g   --total-executor-cores 2 --class             sparkKNN_MIN.spKNN_MIN.spKNN_MIN.runKNN_MINV7 kNN_MIN.jar    "C:\HIGGS.csv"   "C:\HIGGS20Test.txt" 19 0 4

The same for SUSY.

spark-submit --master local[2] --executor-memory 2g   --total-executor-cores 2 --class             sparkKNN_MIN.spKNN_MIN.spKNN_MIN.runKNN_MINV7 kNN_MIN.jar    "C:\SUSY.csv"   "C:\SUSY20Test.txt" 19 0 4


When executing the above command with a modest computer with only 2 cores, a lot of improvement has been observed by keeping the number of cores in the HLOCAL parameter. In the tests, putting HLOCAL [4] by assuming that with 2 cores, there would be an average of 2 threads for each core, the jobs progressed four by four, they had to finish four so that another four would start again. Putting HLOCAL [2] the works were developed two by two at the beginning and then they were alternated.

KNN_MIN only handles files, like HIGGS and SUSY, whose fields are all numeric.

At the end, the program presents each of the lines of the test file with an indication of the corresponding class and the one calculated by KNN_MIN. The tests have been made with test files that already had a class assigned, in order to establish the differences between the real class and the predicted one.

