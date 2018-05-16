# Common Crawl Scala Example

This project provides examples how to generate an input file for graph generation. This project is just a Scala version of the [hostlinks_to_graph.py](https://github.com/commoncrawl/cc-pyspark/blob/master/hostlinks_to_graph.py) of Common Crawl project.  

## Getting Started

- Clone project
- Create shadowJar with command ```./gradlew shadowJar```
- Move shadow jar from `build/libs/cc-scala-all.jar` to your spark gateway
- Run code with following format: `spark-submit --class com.ntent.commoncrawl.HostLinksToGraph cc-scala-all.jar args[]` where args:
    - args[0] InputParquet
    - args[1] EdgesOutput
    - args[2] VerticesOutput
    - args[3] ValidateHosts
    - args[4] SaveAsText
    - args[5] NumPartitions
    - args[6] VertexIDs
    
    Please refer to [original code](https://github.com/commoncrawl/cc-pyspark/blob/master/hostlinks_to_graph.py) for the details of the parameters.

### Prerequisites

If you use shadow JAR file, all dependencies are included in the final jar file otherwise you need to import required dependencies to your project. Please refer to [build.gradle](build.gradle) file for all the dependencies.

## Running the tests

This project doesn't have any unit tests yet

## Built With

* [Gradle](https://gradle.org/) - Dependency Management
* [ShadowJar](https://github.com/johnrengelman/shadow) - Used to create an uber jar

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details