This project is a collection of multipe programs that reflect three NGS use cases. They have been implemented to show and evaluate the versatility and performance of MapReduce and Apache Spark within Bioinformatics. Different Bioinformatics file formats like FastQ, BAM and VCF are processed by the implementations. 


## use cases:
### Quality Statistics:
This use case processes the FastQ file format as input and calculates the sequence quality as well as the base quality statistics. Multiple attempts of MapReduce have been implemented to evaluate the difference that optimizations like a Combiner or In-Memory counting objects have in regards to execution time.

#### sequence quality statistics:
Following programs implement this calculation utilizing MapReduce:

* MapReduce\_FastQ\_SequenceQual\_Baseline
* MapReduce\_FastQ\_SequenceQual\_Combiner
* MapReduce\_FastQ\_SequenceQual\_Optimized

The Apache Spark implementation is contained in following directory:

* Spark\_FastQ\_SequenceQual\_Baseline

#### base quality statistics:
Following programs implement this calculation utilizing MapReduce:  

* MapReduce\_FastQ\_BaseQual\_Baseline
* MapReduce\_FastQ\_BaseQual\_Combiner
* MapReduce\_FastQ\_BaseQual\_Optimized

The Apache Spark implementation is contained in following directory:

* Spark\_FastQ\_BaseQual\_Baseline

Additionally, the following directories contain slighly adapted code that is expected to be run agains a single input file to calculate the base qualtiy statistics. This is because it enables an interesting comparison how the [ADAM genomic analysis platform](https://github.com/bigdatagenomics/adam) performs in comparison to MapReduce and Apache Spark:

* Adam\_FastQ\_BaseQual\_SingleFile\_Baseline
* MapReduce\_FastQ\_BaseQual\_SingleFile\_Combiner
* Spark\_FastQ\_BaseQual\_SingleFile\_Baseline


### Variant Calling:
These programs perform additional quality checks like checking for duplicates or ensuring a sufficient long read length but the main purpose of the variant calling is to compare specific genomic data against an existing reference genome file to filter mutations. Input to these programs are BAM files.

The implementation utilizing ADAM is contained in following directory: 

* Adam\_VariantCaller\_Native\_Scala

Like in the Quality Check use cases multiple MapReduce implementations have been developed to evaluate performance differences:

* MapReduce\_VariantCaller\_Baseline
* MapReduce\_VariantCaller\_Combiner
* MapReduce\_VariantCaller\_Optimized
* MapReduce\_VariantCaller\_Optimized\_Combiner

The following Apache Spark implementations differ regarding the utilized key-objects. The baseline version uses a self implemented complex object whereas the other version relies on Scala-Pair objects. This was done to evaluate performance differences between these two versions:

* Spark\_VariantCaller\_Scala\_Baseline
* Spark\_VariantCaller\_Scala


### VCF Join:
This use cases joins two VCF files to augment the data of a patient with information from an existing reference genome file.

The MapReduce join implementation is conained in following directory:

* MapReduce\_VCF\_Join

Because of the high-level join operation, an Apache Pig script has been implemented to compare its performance against MapReduce. It can be found in following directory:

* Pig\_VCF\_Join

The Apache Spark implementation of the VCF join is contained in following directory:

* Spark\_VCF\_Join


## usage information:
The first step to execute the code is to build a jar-file using Maven by executing ```mvn package``` via command line. The required ```pom.xml``` file is contained within every program directory. To execute the created jar-files it is necessary that you set-up a Hadoop cluster. The pre-configured [Cloudera Hadoop Version](http://www.cloudera.com/downloads.html) may be a good starting point.

The resources-directory of each program contains a ```cloudgene.yaml``` file. [Cloudgene](https://github.com/genepi/cloudgene) provides a convenient open-source platform to improve the usability of MapReduce programs by providing a graphical user interface for the execution, the import and export of data and the reproducibility of workflows. Therefore it is highly recommended that you configer your Hadoop Cluster to run Cloudgene.

However, if you prefer to execute the code manually via command line the following two examples for MapReduce and Spark may provide some help:

#### MapReduce program execution
Input for the MapReduce program has to be placed in the HDFS file system. Apache Hadoop expects a jar-file to execute, an input-path and an output-path submitted via console upon execution. eg:

```bash
hadoop jar Mapreduce_VariantCaller.jar input/myfile.bam output/variantCaller
```

#### Spark programs
The Apache Spark programs get executed by the command ```spark-submit```. If you execute the program locally by using the Apache Spark binary you may need to add the console command ```--master local[2]``` to get the program to work. The parameters for the program to work are basically the same as for the MapReduce program: 

* the jar-file which contains the code to be executed
* an input-file
* an output path

Following example shows how you may start your execution:

```bash
spark-submit --master local[2] Spark_VariantCaller_Scala.jar input/myFile.bam output/variantCaller
```

© Clemens Banas • **this project is part of my master thesis**

