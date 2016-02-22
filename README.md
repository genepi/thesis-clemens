## usage information
It is expected that you already have set-up a Hadoop cluster to execute the MapReduce program. For executing the Apache Spark programs you either have to install Apache Spark on your Hadoop cluster or [download](http://spark.apache.org/downloads.html) the standalone binaries for testing purpose.

Maven will resolve all dependencies for you but you have to compile the Apache Spark programs on your own. It is important that you compile them with Scala version 2.10.x

## how to execute the code
The first step is to build a jar-file using Maven.
### MapReduce program 
Input for the MapReduce program has to be placed in the HDFS file system. Apache Hadoop expects a jar-file as the code to execute, an input-path and an output-path submitted via console upon execution. eg:

```bash
hadoop jar Mapreduce_VariantCaller-1.0-SNAPSHOT.jar input/myfile.bam output/variantCaller
```

### Spark programs
The Apache Spark programs get executed by the command ```spark-submit```. If you execute the program locally by using the Apache Spark binary you may need to add the console command ```--master local[2]``` to get the program to work. The parameters for the program to work are basically the same as for the MapReduce program: 

* the jar-file which contains the code to be executed
* an input-file
* an output path

Following example shows how you may start your execution:

```bash
spark-submit --master local[2] Spark_VariantCaller_Scala-1.0-SNAPSHOT.jar input/myFile.bam output/variantCaller
```

### Programs

#### MapReduce:
* [x] *MapReduce_VariantCaller_Baseline*
* [x] *MapReduce_VariantCaller_Combiner*
* [x] *MapReduce_VariantCaller_Optimized*
* [x] *MapReduce_VariantCaller_Optimized_Combiner*
* [x] *MapReduce_FastQ_BaseQual_Baseline*
* [x] *MapReduce_FastQ_BaseQual_Combiner*
* [x] *MapReduce_FastQ_BaseQual_Optimized*
* [x] *MapReduce_FastQ_SequenceQual_Baseline*
* [x] *MapReduce_FastQ_SequenceQual_Combiner*
* [x] *MapReduce_FastQ_SequenceQual_Optimized*
* [ ] *MapReduce_VCF_ReduceSideJoin*

#### Spark:
* [x] *Spark_VariantCaller_Scala_Baseline*
* [x] *Spark_VariantCaller_Scala*
* [x] *Spark_FastQ_BaseQual_Baseline*
* [x] *Spark_FastQ_SequenceQual_Baseline*
* [ ] *Spark_VCF_Join*

#### ADAM:
* [ ] *Adam_VariantCaller_Native_Scala*
* [x] *Adam_FastQ_BaseQual_Baseline*
* [x] *Adam_FastQ_SequenceQual_Baseline*
* [ ] *Adam_VCF_Join*


**please note, that this project is work in progress and may change it's functionality**
© Clemens Banas
