# Movielens
**Movielens** is a command line tool developed for ASOS' big data engineer technical test.

The test has the following requirements:

- Load `movie`, `ratings` and `tag` data into a delta staging tables
- Using `userId` and `movieId` as a primary key, update a row if exists else insert a new row
- Define a partition strategy for ratings data that will scale as more data is added
- Cast columns to appropriate data types
- Process new `csv` data as it arrives
- Split `movie genres` into a single row per `genre` and saves the result (`parquet`)
- Save `top 10` movies by `avg rating` where a `movie` has `5` or more ratings to a single `csv` file

## Installation (Windows)
**Movielens** is built using the [mvn]("https://maven.apache.org/"") build tool. The first step is to download the project files locally:

```cmd
$ git clone https://github.com/sradden/asos.git
```

### Project Structure ###
```cmd
$ dir /s
    .gitignore
    <dir> movielens
    README.md

    asos\movielens
    pom.xml
    <dir> src

    asos\movielens\src
    <main> -- scala project code and resources (.csv files)
    <test>  -- scala tests and test resources (.csv files)
$ dir
```

## Building movielens ##
run the following maven commands to compile `movielens`
```cmd
$ asos\movielens mvn clean
$ asos\movielens mvn compile
```
## Executing unit tests ##
run the following maven command to execute the **movielens** unit tests (unit test reports are saved to `\movielens\target\surefire-reports`)
```cmd
$ asos\movielens mvn test
```
## Running the application
`movielens` can be run from the command line or through `spark-submit` to an existing spark cluster.

### via cmd line ###
```scala
mvn scala:run -DmainClass=com.asos.pipeline.App
```
### via spark-submit ###
**movielens** has to be packaged into a `.jar` to be submitted to an existing spark cluster.
```cmd
$ asos\movielens mvn package
```
```cmd
$ spark-submit \
    --class com.asos.pipeline.App \
    --master spark://hostname:port \
    --jars movielens-1.0-SNAPSHOT.jar
```

### Application output ###
Once launched, **movielens** create the following output:

- asos\movielens\out\delta - bronze delta tables for `movie`, `rating` and `tag` data
- asos\movielens\out\split-movie-genres - `parquet` files containing the results of the `genres` split
- asos\movielens\out\top10-films-by-avg-rating.csv - `csv` file containing top 10 films by `avg` rating