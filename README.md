# ASTROIDE V1.0

_ASTROIDE: Astronomical In-memory Distributed Engine_

ASTROIDE is a distributed data server for astronomical data. 
ASTROIDE introduces effective methods for efficient astronomical query execution on Spark through 
data partitioning with HEALPix and customized optimizer. ASTROIDE offers a simple, expressive and 
unified interface through ADQL, a standard language for querying  databases in astronomy. 

We are in the process of making our source code open.  

# Requirements


- This version requires Spark 2.2.x and Hadoop 2.7+ installed .

- Initialize an environment variable called `ASTROIDE_HOME` to include the directory where you cloned this repository

		$ git clone https://github.com/MBrahem/ASTROIDE

        $ export ASTROIDE_HOME=<path to ASTROIDE directory>

Example: 

		$ export ASTROIDE_HOME=/home/hadoop/ASTROIDE-master/
    
- Add jars in ''conf/spark-defaults.conf'' by adding these lines:

	    spark.driver.extraClassPath <ASTROIDE_HOME>/libs/healpix-1.0.jar:<ASTROIDE_HOME>/libs/adql1.3.jar
    
    	spark.executor.extraClassPath <ASTROIDE_HOME>/libs/healpix-1.0.jar:<ASTROIDE_HOME>/libs/adql1.3.jar
    
These libraries already exists in `libs` directory, for more details please refer to:

- [Healpix library 1](http://healpix.sourceforge.net/index.php)



- [ADQL library 1.3](http://cdsportal.u-strasbg.fr/adqltuto/download.html)




## Input Data 

ASTROIDE supports reading ONLY input format csv or compressed csv.

You can download example of astronomical data [here](https://github.com/MBrahem/ASTROIDE/tree/master/ExampleData) or all GAIA DR1 [here](http://cdn.gea.esac.esa.int/Gaia/gdr1/gaia_source/csv/) and put downloaded files in HDFS.

Example:

	$ cd $ASTROIDE_HOME/ExampleData
	$ hdfs dfs -mkdir data
    $ hdfs dfs -put * data/
    
ASTROIDE allows users to use data whose coordinates are expressed according to the International Celestial Reference System [ICRS](https://hpiers.obspm.fr/icrs-pc/icrs/icrs.html)

Other coordinate systems will be supported in future versions. 

## Partitioning 

Partitioning is a fundamental component for parallel data processing. It reduces computer resources when only a sub-part of relevant data are involved in a query, and distributes tasks evenly when the query concerns a large number of partitions. This hence globally improves the query performances. 

Partitioning is a mandatory  process for executing astronomical queries efficiently in ASTROIDE.

Using ASTROIDE, partitioning is executed as follows:

	$ spark-submit --class fr.uvsq.adam.astroide.executor.BuildHealpixPartitioner [ --master <master-url>  ] <astroide.jar> -fs <hdfs> [<schema>] <infile> <separator> <outfile.parquet> <partitionSize> <healpixOrder> <name_coordinates1> <name_coordinates2> <boundariesFile>

The above line can be explained as:

- *hdfs*: hdfs path. For e.g. hdfs://{NameNodeHost}:8020

- *schema*: text file containing the schema of input file (separator: ","). If your data already contains the schema, you can skip this option


- *infile*: path to input file on HDFS where astronomical data are stored

- *separator*: separator used in the input file (e.g. "," or "|")

- *outfile*: path to output parquet file on HDFS where astronomical data will be stored after partitioning 

- *partitionSize*: approximate partition size in MB(recommended 256MB)

- *healpixOrder*: the value of healpix order to use (recommended value 12)


- *name_coordinates1*: attribute name of the first spherical coordinates (usually ra)


- *name_coordinates2*: attribute name of the second spherical coordinates (usually dec)


- *boundariesFile*: a text file where ASTROIDE will save the metadata (ranges of partitions)
Please precise a path to a local file on your master node.


Example:

	$ hdfs dfs -mkdir data
		
	
	$ spark-submit --class fr.uvsq.adam.astroide.executor.BuildHealpixPartitioner  $ASTROIDE_HOME/ProjectJar/astroide.jar -fs hdfs://localhost:8020 data/gaia.csv "," partitioned/gaia.parquet 32 12 ra dec $ASTROIDE_HOME/gaia.txt

Or using schema:

	$ spark-submit --class fr.uvsq.adam.astroide.executor.BuildHealpixPartitioner $ASTROIDE_HOME/ProjectJar/astroide.jar -fs hdfs://localhost:8020  $ASTROIDE_HOME/ExampleData/tycho2Schema.txt data/tycho2.gz "|" partitioned/tycho2.parquet 32 12 ra dec $ASTROIDE_HOME/tycho2.txt


ASTROIDE retrieves partition boundaries and stores them as metadata. Note that in our case, all we need to store are the three values (n, l, u) where n is the partition number, l is
the first HEALPix cell of the partition number n and u is the last HEALPix cell of the partition number n. It should also be noted that we store the partitioned files, along with the
metadata, on HDFS and use them for future queries.

Below is an example of small file with 5 partitions:

    $ cat $ASTROIDE_HOME/gaia.txt
    
    [3,268313,340507]
    [0,0,86480]
    [1,86481,178805]
    [2,178807,268308]
    
    
The partitioned file will be stored as a parquet file that looks like: 

	$ hdfs dfs -ls partitioned/gaia.parquet/

	Found 5 items
	-rw-r--r--   1 user supergroup          0 2018-06-22 11:32 partitioned/gaia.parquet/_SUCCESS
	drwxr-xr-x   - user supergroup          0 2018-06-22 11:32 partitioned/gaia.parquet/nump=0
	drwxr-xr-x   - user supergroup          0 2018-06-22 11:32 partitioned/gaia.parquet/nump=1
	drwxr-xr-x   - user supergroup          0 2018-06-22 11:32 partitioned/gaia.parquet/nump=2
	drwxr-xr-x   - user supergroup          0 2018-06-22 11:32 partitioned/gaia.parquet/nump=3
	

## Run astronomical queries


AstroSpark focuses on three main basic astronomical queries, these queries require more calculations than ordinary search or join in legacy relational databases:

- Cone Search is one of the most frequent queries in the astronomical domain. It returns a set of stars whose positions lie within a circular region of the sky. 

- Cross-Matching query aims at identifying and comparing astronomical objects belonging to different observations of the same sky region. 

- kNN query returns the k closest sources from a query point q. 

You can run astronomical queries using object `fr.uvsq.adam.astroide.executor.AstroideQueries` which executes ADQL queries using our framework.

Note: Please make sure that your data has been already partitioned to execute astronomcal queries.

After data partitioning, you can start executing astronomical queries using ADQL. 

ASTROIDE supports ADQL Standard. It includes three kinds of astronomical operators as follows. All these operators can be directly passed to astroide throught *queryFile*


    $ spark-submit --class fr.uvsq.adam.astroide.executor.AstroideQueries [--master <master-url>] <astroide.jar> -fs <hdfs> <file1> <file2> <healpixOrder> <queryFile> <action> [<outfile>]
    

> For KNN & ConeSearch queries 



- *file1*: output file created after partitioning (parquet file)



- *file2*: boundaries file created after partitioning (text file)


- *queryFile*: ADQL query (text file) see examples below


- *action*: action that you will execute on query result (show, count, save)

- *outfile*: output file where result can be saved on HDFS


If no action is defined, ASTROIDE will show only the execution plan.


> For CrossMatch queries



- *file1*: first dataset (parquet file)



- *file2*: second dataset (parquet file)



- *queryFile*: ADQL query (text file) see examples below


- *action*: action that you will execute on query result (show, count, save)

- *outfile*: output file where result can be saved on HDFS

If no action is defined, ASTROIDE will show only the execution plan.


Example:

	
	$ spark-submit --class fr.uvsq.adam.astroide.executor.AstroideQueries $ASTROIDE_HOME/ProjectJar/astroide.jar -fs hdfs://localhost:8020 partitioned/gaia.parquet $ASTROIDE_HOME/gaia.txt 12 $ASTROIDE_HOME/ExampleQuery/conesearch.txt show
	
	...
	2018-06-22 11:43:59 INFO  DAGScheduler:54 - Job 2 finished: show at AstroideQueries.scala:87, took 0,508019 s
	+-------------+------------------+-------------------+
	|    source_id|                ra|                dec|
	+-------------+------------------+-------------------+
	|5050881701504| 44.95293718578692|0.13388443523264248|
	|1099511693312|44.966545443077436|0.04631022905873263|
	|1614907863552|44.951154531610996|0.10530901086672136|
	...


## Queries Examples

These are some correct ADQL queries that you can refer to test in ASTROIDE.

You can save only one query in a text file and run your application using `fr.uvsq.adam.astroide.executor.AstroideQueries`. 


### ConeSearch Queries

    SELECT * FROM table WHERE 1=CONTAINS(point('icrs',ra,dec),circle('icrs', 44.97845893652677, 0.09258081167082206, 0.05));
    
    SELECT ra,dec,ipix FROM table WHERE 1=CONTAINS(point('icrs',ra,dec),circle('icrs', 44.97845893652677, 0.09258081167082206, 0.05)) ORDER BY ipix;
    
    SELECT source_id,ra FROM table WHERE (1=CONTAINS(point('icrs',ra,dec),circle('icrs', 44.97845893652677, 0.09258081167082206, 0.05)) AND ra > 0);
    
    SELECT ra,dec,ipix FROM (SELECT * FROM table WHERE 1=CONTAINS(point('icrs',ra,dec),circle('icrs', 44.97845893652677, 0.09258081167082206, 0.05))) As t;
    
### KNN Queries 

    SELECT TOP 10 *, DISTANCE(Point('ICRS', ra, dec), Point('ICRS', 44.97845893652677, 0.09258081167082206)) AS dist FROM table ORDER BY dist;
        
    SELECT t.ra, t.dec,t.dist FROM (SELECT TOP 10 *, DISTANCE(Point('ICRS', ra, dec), Point('ICRS', 44.97845893652677, 0.09258081167082206)) AS dist FROM table1 WHERE ra> 0 ORDER BY dist) AS t;
      

### CrossMatching Queries

    SELECT * FROM table JOIN table2 ON 1=CONTAINS(point('icrs',table.ra,table.dec),circle('icrs',table2.ra,table2.dec,0.01));
    
    SELECT * FROM table JOIN table2 ON 1=CONTAINS(point('icrs',table.ra,table.dec),circle('icrs',table2.ra,table2.dec,0.01)) WHERE table.ra>0 ORDER BY table.ra;
    
    SELECT * FROM (SELECT table.source_id, table.ipix, table2.ipix FROM table JOIN table2 ON 1=CONTAINS(point('icrs',table.ra,table.dec),circle('icrs',table2.ra,table2.dec,0.01))) AS t;

  
Please consider that ASTROIDE translates these three types of queries into internal representation. ASTROIDE don't translate all features of ADQL language. 
For example, the second KNN query is translated to an equivalent query as follows: 

    == Translated Query ==
    SELECT t.ra , t.dec , t.dist
    FROM (SELECT * , SphericalDistance(ra, dec, 44.97845893652677, 0.09258081167082206) AS dist
    FROM table1
    WHERE ra > 0
    ORDER BY dist ASC
    Limit 10) AS t
   
   
ASTROIDE will inject some optimizations rules to the optimizer for the efficient execution of this query; 
In such case, the objective is to avoid reading unnecessary data, and to load only required partitions
using `PartitionFilters: [isnotnull(nump#58), (nump#58 = 0)]`

    == Physical Plan ==
    TakeOrderedAndProject(limit=10, orderBy=[dist#120 ASC NULLS FIRST], output=[ra#4,dec#6,dist#120])
    +- *Project [ra#4, dec#6, if ((isnull(cast(ra#4 as double)) || isnull(cast(dec#6 as double)))) null else UDF:SphericalDistance(cast(ra#4 as double), cast(dec#6 as double), 44.97845893652677, 0.09258081167082206) AS dist#120]
        +- *Filter (isnotnull(ra#4) && (cast(ra#4 as int) > 0))
            +- *FileScan parquet [ra#4,dec#6,nump#58] Batched: true, Format: Parquet, Location: InMemoryFileIndex[hdfs://UbuntuMaster:9000/bucketby/gaiatest.parquet], PartitionCount: 1, PartitionFilters: [isnotnull(nump#58), (nump#58 = 0)], PushedFilters: [IsNotNull(ra)], ReadSchema: struct<ra:string,dec:string>

 
## Dataframe API

A second option for executing astronomical queries is to use the Spark DataFrame API using Scala.

If you need to test other queries using the DataFrame interface, you can refer to other classes in package `fr.uvsq.adam.astroide.executor` called `RunCrossMatch` `RunConeSearch` `RunKNN`

### Cross-matching 

    spark-submit --class fr.uvsq.adam.astroide.executor.RunCrossMatch <application-jar> <file1> <file2> <radius> <healpixOrder>

Cross matching imports this package

    import fr.uvsq.adam.astroide.queries.optimized.CrossMatch._

Given a dataFrame representing the first dataset, this class execute a crossmatch using a precised radius by doing the following:

    val output = df1.ExecuteXMatch(session, df2, radius)

This produces a new dataframe called output which is the result of crossmatching two input dataframes. 


## Publications

1. M. BRAHEM, K. Zeitouni, and L. Yeh. ASTROIDE: A unified astronomical big data processing
engine over spark. IEEE Transactions on Big Data, 2018 [(Link)](http://perso.prism.uvsq.fr/users/zeitouni/papers/TBD2018_Preprint.pdf)

2. M. BRAHEM, K. Zeitouni, and L. Yeh. Efficient astronomical query processing using spark. In
In 26th ACM SIGSPATIAL International Conference on Advances in Geographic Information
Systems, SIGSPATIAL ’18, New York, NY, USA, 2018. ACM [(Link)](http://perso.prism.uvsq.fr/users/zeitouni/papers/SIGSPATIAL2018_Preprint.pdf)

3. M. BRAHEM, K. Zeitouni, and L. Yeh. HX-MATCH: in-memory cross-matching algorithm
for astronomical big data. In Advances in Spatial and Temporal Databases - 15th International
Symposium, SSTD 2017, Arlington, VA, USA, August 21-23, 2017, Proceedings, 2017

4. M. BRAHEM, K. Zeitouni, and L. Yeh. Astrospark: towards a distributed data server for big
data in astronomy. In Proceedings of the 3rd ACM SIGSPATIAL PhD Symposium. ACM, 2016




