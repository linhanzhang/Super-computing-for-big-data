# Super-computing-for-big-data

[![standard-readme compliant](https://img.shields.io/badge/readme%20style-standard-brightgreen.svg?style=flat-square)](https://github.com/RichardLitt/standard-readme)

📖 A brief introduction to the project **build evacuee model with big data techniques**. Include the  installation instructions for each application and result demonstration. 


## Table of Contents
- [Background](#background)
- [Task1 - local application](#task-1-local-application)
	- [Installation](#installation)
  - [Results](#results)
- [Task2 - AWS application](#aws-application)
	- [Installation](#installation)
  - [Results](#results)
- [Task3 - Kafka Streaming Application](#kafka-streaming-application)
	- [Installation](#installatio)
  - [Results](#results)
  
 

## Background


> Your documentation is complete when someone can use your module without ever




1. A well defined **specification**. This can be found in the [Spec document](spec.md). It is a constant work in progress; please open issues to discuss changes.
2. **An example README**. This Readme is fully standard-readme compliant, and there are more examples in the `example-readmes` folder.
3. A **linter** that can be used to look at errors in a given Readme. Please refer to the [tracking issue](https://github.com/RichardLitt/standard-readme/issues/5).
4. A **generator** that can be used to quickly scaffold out new READMEs. See [generator-standard-readme](https://github.com/RichardLitt/generator-standard-readme).
5. A **compliant badge** for users. See [the badge](#badge).



## Task 1 - local application


To learn more details about this task, see [task1-report](https://github.com/linhanzhang/Super-computing-for-big-data/tree/master/local-application/lab-1-group09#readme). 

### Compile and Run

After cloning the repository, navigate to the root directory by typing in the following command in the terminal:

```
cd directory_to_Lab1/lab-1-group-09
```
Then start the sbt container in the root folder; it should start an interactive sbt process. Here, we can compile the sources by writing the compile command.
```
docker run -it --rm -v "`pwd`":/root sbt sbt
sbt:Lab1 >compile
```

Now we are set up to run our program! Consider an integer that represents the height of the rising sea level (unit: meter).<br/>
Use the ` run height `  command to start the process, and you can get information like the image below. This way of running the spark application is mainly used for testing.  
```
sbt:Lab1 >run 5
```  

Next, we will introduce another way of building and running this Spark application, enabling developers to inspect the event log on the spark history server.  

By using the ` spark-submit `  command, we set the application to run on a local Spark "cluster." Since we have already built the JAR, all we need to do is to run the code below:

```
docker run -it --rm -v "`pwd`":/io -v "`pwd`"/spark-events:/spark-events spark-submit --packages 'com.uber:h3:3.7.0' target/scala-2.12/lab-1_2.12-1.0.jar height
```

### Results




## Task 2 - local application


To learn more details about this task, see [task2-report](https://github.com/linhanzhang/Super-computing-for-big-data/blob/master/AWS-application/lab-2-group-09/README.md). 
### Compile and Run
The application is executed via `Planet.jar` , a fat JAR file packaged by the [sbt-assembly](https://github.com/sbt/sbt-assembly) plugin. First log in to your AWS account. 

Then it will come to the AWS management console, from which customers can get access to a variety of services that AWS provides. The one we use to run our Spark application is Elastic MapReduce(EMR). Type `EMR` in the search bar and click the link to the first result. Here, we can run our application in Spark cluster mode. 

To create a cluster, the types and numbers of node instances we used to run our application on the Planet data set is listed below:
|Node type|Instance Type| Number of instances |
|:---:|:---:|:---:|
|Master node|c5.2xlarge |1|
|Core node|c5.24xlarge|4|

The final step is to add a step to the cluster. Choose Spark application as "Step type". Copy and paste the following configures into the "Spark-submit options" part. Select "Application location" from the S3 bucket and add an integer in "Arguments" to represent the rising sea level. Click "Add" and it is all set! 

```
--conf "spark.sql.autoBroadcastJoinThreshold=-1"  
--conf "spark.sql.broadcastTimeout=36000" 
--conf "spark.yarn.am.waitTime=36000"
--conf "spark.yarn.maxAppAttempts=1" 
```



### Results



## Task 3 - local application
To learn more details about this task, see [task3-report](https://github.com/linhanzhang/Super-computing-for-big-data/tree/master/Kafka-Streaming-application/lab-3-group-09#readme). 
### Compile and Run
The following library dependencies should be added to transformer/build.sbt:
```
libraryDependencies ++= Seq(
    "io.circe" %% "circe-core" % "0.14.1",
    "io.circe" %% "circe-generic" % "0.14.1",
    "io.circe" %% "circe-parser" % "0.14.1" ,
)
```

Open a terminal 1, git clone the project repo to /home, run `docker-compose up` in the project root directory. After all the servers be started up, open another terminal and run `docker-compose exec transformer sbt` under root directory.
to open interactive sbt terminal. When the producer starts to produce steady input stream in "events" topic like following:

```
events_1            | 1810821	{"timestamp":1634500171928,"city_id":1810821,"city_name":"Fuzhou","refugees":2953}
events_1            | 5506956	{"timestamp":1634500172053,"city_id":5506956,"city_name":"Las Vegas","refugees":99498}
events_1            | 1793505	{"timestamp":1634500172548,"city_id":1793505,"city_name":"Taizhou","refugees":12371}
events_1            | 1627896	{"timestamp":1634500172679,"city_id":1627896,"city_name":"Semarang","refugees":114243}
events_1            | 1258662	{"timestamp":1634500173010,"city_id":1258662,"city_name":"Rāmgundam","refugees":0}
```

compile the code:
```
sbt:Transformer> compile
```

run the transformer by passing command line argument: window size N (seconds).
Here the window size is set to 2 seconds. Note that the input argument should be a positive integer, otherwise
it will be rejected by the type-check procedure and the kafka context will be shut down
```
sbt:Transformer> run 2
```

The transformer will start to transform the input stream and write to output topic "updates".



### Results



## Related Efforts

- [Art of Readme](https://github.com/noffle/art-of-readme) - 💌 Learn the art of writing quality READMEs.
- [open-source-template](https://github.com/davidbgk/open-source-template/) - A README template to encourage open-source contributions.


## Contributors

Feel free to dive in! [Open an issue](https://github.com/RichardLitt/standard-readme/issues/new) or submit PRs.

Standard Readme follows the [Contributor Covenant](http://contributor-covenant.org/version/1/3/0/) Code of Conduct.



