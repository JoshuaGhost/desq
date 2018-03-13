# DesqSeq and DesqCand: Scalable Frequent Sequence Mining With Flexible Subsequence Constraints

This is an implementation of the algorithms `DesqSeq` and `DesqCand`, two distributed scalable algorithms for frequent sequence mining under flexible subsequence constraints. 
Here, we give a quick overview over the most relevant parts of the code and how one can run experiments. 


## Entry points for reading the code
* [`DDIN.scala`](src/main/scala/de/uni_mannheim/desq/mining/spark/DDIN.scala) contains high-level code for mapping over input sequences, shuffling candidate sequences, and mining partitions.
* [`DesqDfs.java`](src/main/java/de/uni_mannheim/desq/mining/DesqDfs.java) contains low-level code for determining pivot items for input sequences, constructing NFAs, and mining partitions locally. 
* [`OutputNFA.java`](src/main/java/de/uni_mannheim/desq/mining/OutputNFA.java) encodes candidate sequences as an NFA. It contains code to build a tree from accepting paths through the FST, to merge suffixes of an NFA, and to serialize an NFA. 
* [`NFADecoder.java`](src/main/java/de/uni_mannheim/desq/mining/NFADecoder.java) decodes an NFA that was serialized by path using variable-length integer encoding to an internal representation by state, which is we use for local mining. 
* [`DesqRunner.scala`](src/main/scala/de/uni_mannheim/desq/examples/spark/DesqRunner.scala) is a driver class to conveniently run experiments. Contains definitions for the pattern expressions used in the experiments. 

## How to run 
You can either run the algorithms locally from the IDE, or one can use `spark-submit` to a Spark/YARN cluster. Local running is straightforward and can be started by running `DesqRunner`. In the following, we describe running on a cluster. 

### Building 
To build a reduced jar file that can be used in Spark, run:
```bash
mvn package -DskipTests -f pom.spark.xml
```
To run an application on a Spark cluster, one typically creates a jar that contains [the application's dependencies](http://spark.apache.org/docs/latest/submitting-applications.html) and submits this jar to the cluster. The POM file `pom.spark.xml` [excludes dependencies](https://maven.apache.org/plugins/maven-shade-plugin/examples/includes-excludes.html) that are bundled in Spark and some classes that our application does not use. The above command creates this jar in `target/desq-0.0.1-SNAPSHOT.jar`. One can build a full jar by running the command without the `-f pom.spark.xml` part. 

### Running on a cluster
Assuming you created a jar `target/desq-0.0.1-SNAPSHOT.jar`, have set `$SPARK_HOME`, and have set up a valid YARN configuration on you machine, you can run the following:

```bash
${SPARK_HOME}/bin/spark-submit \
--master yarn  \
--deploy-mode cluster \
--class de.uni_mannheim.desq.examples.spark.DesqRunner \
--executor-memory 64g \
--driver-memory 16g \
--num-executors 8  \
--executor-cores 8 \
--driver-cores 1 \
--conf "spark.executor.extraJavaOptions=-XX:+UseG1GC" \
/path-to-ddin-code/target/desq-0.0.1-SNAPSHOT.jar \
input=hdfs:///path-to-input-DesqDataset/ \
output=hdfs:///output-path/ \
case=[case] \
algorithm=[algorithm]
```
The path specified by `input` should contain a [`DesqDataset`](src/main/scala/de/uni_mannheim/desq/mining/spark/DesqDataset.scala). There are pointers on how to create a `DesqDataset` in [`DesqBuilderExample.scala`](src/main/scala/de/uni_mannheim/desq/examples/spark/DesqBuilderExample.scala)  The `case` option gives quick access to pattern expressions used in the thesis: 
* `A1`, `A2`, `A3`, `A4`, `N1`, `N2`, `N3`, `N4`, and `N5` all with pre-defined sigma-values as given in the thesis. 
* `S(sigma, lambda)`: PrefixSpan-style constraints - maximum length constraint *lambda*, no hierarchies
* `M(sigma,gamma,lambda)`: MG-FSM-style constraints - maximum gap constraint *gamma*, maximum length constraint *lambda*, no generalizations
* `L(sigma,gamma,lambda)`: LASH-style constraints - maximum gap constraint *gamma*, maximum length constraint *lambda*, and every match item is generalized

For parameter `algorithm`, the baseline algorithms and algorithm variants from the thesis are available:
* `Naive` and `SemiNaive`: shuffle candidate sequences
* `DesqSeq` and variants `DesqSeq.noGrid`, `DesqSeq.noStop`, and `DesqSeq.noTrim.noStop`: shuffle input sequences to the partitions
* `DesqCand` and variants `DesqCand.tries` and `DesqCand.tries.noAgg`: shuffle candidate sequences, encoded as NFAs

More information about running on YARN can be found in the [Spark documentation](http://spark.apache.org/docs/latest/running-on-yarn.html). You can also run the algorithms using the [Spark standalone mode](http://spark.apache.org/docs/latest/spark-standalone.html#launching-spark-applications). 

### A simple example
We included an example dataset in the code repository at `data/vldb-example/`. With the following, you can run the pattern expression used as example in the paper on the example sequence database:
```bash
${SPARK_HOME}/bin/spark-submit \
--master "local[4]"  \
--class de.uni_mannheim.desq.examples.spark.DesqRunner \
/path-to-code/target/desq-0.0.1-SNAPSHOT.jar \
input=file:///path-to-code/data/vldb-exampleDesqDataset/ \
output=file:///output-path/ \
case=VLDB \
algorithm=DesqSeq
```

