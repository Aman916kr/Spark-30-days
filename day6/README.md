# Day6 Word Count

A beginner-friendly Apache Spark project that demonstrates the classic Word Count program using RDD transformations and `reduceByKey`.

The project also performs case-insensitive word counting, removes punctuation and empty words, and finds the top 10 most frequent words in application logs.

## Technologies Used

* Scala 2.12.18
* Apache Spark 3.5.9
* SBT 1.11.7
* Ubuntu Linux
* JDK 11

## Practice Tasks

### 1. Classic Word Count

Implement the classic Spark Word Count program.

The basic Word Count pattern is:

```scala
val wordCounts = logRDD
  .flatMap(line => line.split(" "))
  .map(word => (word, 1))
  .reduceByKey(_ + _)
```

The process is:

```text
Input Text
    ↓
flatMap
    ↓
Words
    ↓
map
    ↓
(word, 1)
    ↓
reduceByKey
    ↓
Word Counts
```

### 2. flatMap

Use `flatMap` to split each log line into individual words.

```scala
val words = logRDD
  .flatMap(line => line.split(" "))
```

For example:

```text
INFO Application started
```

becomes:

```text
INFO
Application
started
```

### 3. map

Use `map` to convert every word into a key-value pair.

```scala
val wordPairs = words
  .map(word => (word, 1))
```

Example:

```text
INFO
INFO
ERROR
```

becomes:

```text
(INFO,1)
(INFO,1)
(ERROR,1)
```

### 4. reduceByKey

Use `reduceByKey` to combine counts for the same word.

```scala
val wordCounts = wordPairs
  .reduceByKey(_ + _)
```

For example:

```text
(INFO,1)
(INFO,1)
(INFO,1)
```

becomes:

```text
(INFO,3)
```

### 5. Case-Insensitive Word Count

Convert all words to lowercase before counting.

```scala
.toLowerCase
```

This makes:

```text
INFO
Info
info
```

count as:

```text
info
```

### 6. Ignore Punctuation

Remove punctuation from the log messages.

```scala
.replaceAll("[^a-z0-9\\s]", "")
```

For example:

```text
failed.
```

becomes:

```text
failed
```

### 7. Ignore Empty Words

Remove empty strings using:

```scala
.filter(_.nonEmpty)
```

### 8. Top 10 Most Frequent Words

Sort the word counts by frequency and return the top 10 words.

```scala
val top10Words = caseInsensitiveCounts
  .sortBy {
    case (word, count) =>
      (-count, word)
  }
  .take(10)
```

## Scenario

### Top 10 Most Frequent Words in Application Logs

The project treats application log messages as input data.

Example:

```text
INFO Application started successfully.
ERROR Database connection failed.
INFO User logged in successfully.
WARNING Disk space is low.
ERROR Database connection failed.
```

The application:

1. Splits the logs into words.
2. Converts words to lowercase.
3. Removes punctuation.
4. Removes empty words.
5. Creates `(word, 1)` pairs.
6. Uses `reduceByKey` to calculate word frequencies.
7. Sorts the results by frequency.
8. Finds the top 10 most frequent words.

## How to Run

Run the following commands from the project directory.

### Compile

```bash
sbt compile
```

### Run

```bash
sbt run
```

## Output

The program displays the word frequencies and the top 10 most frequent words.

Example output:

```text
========== CLASSIC WORD COUNT ==========

(started.,2)
(successfully.,3)
(Application,2)
(ERROR,4)
(Database,2)
(connection,3)
(failed.,3)
(INFO,4)
(User,2)
(logged,2)
(in,2)
(WARNING,2)
(Disk,1)
(space,1)
(is,2)
(low.,1)
(Processing,1)
(request.,1)
(File,2)
(not,2)
(found.,2)
(Network,1)
(logged,1)
(out,1)
(Memory,1)
(usage,1)
(high.,1)

========== CASE-INSENSITIVE WORD COUNT ==========

(error,4)
(info,4)
(successfully,3)
(connection,3)
(failed,3)
(application,2)
(started,2)
(database,2)
(user,2)
(logged,2)

========== TOP 10 MOST FREQUENT WORDS ==========

(error,4)
(info,4)
(connection,3)
(failed,3)
(successfully,3)
(application,2)
(database,2)
(file,2)
(found,2)
(in,2)

========== TOTAL WORDS ==========

Total words: 45
```

