package com.ascc

import org.apache.spark.SparkContext
import com.ascc.util.Files

/**
  * Count the words in a corpus of documents.
  * This version uses the familiar GROUP BY, but we'll see a more efficient
  * version next.
  */
object WordCount2 {

    def main(args: Array[String]): Unit = {
        val inPath  = "resources/all-shakespeare.txt"
        val outPath = "output/word_count2"
        Files.rmrf(outPath)  // delete old output (DON'T DO THIS IN PRODUCTION!)
        println("...creating SparkContext\n")

        val sc = new SparkContext("local[*]", "WordCount2") // create SparkContext where local[*] means run Spark
                                                            // locally with as many worker threads as logical
                                                            // cores on your machine. WordCount pertains to
                                                            // the App Name (spark's point of view)
        sc.setLogLevel("WARN")
        try {
            val input = sc.textFile(inPath) //RDD containing data from all-shakespeare.txt
            val wc = input
                .map(_.toLowerCase) // transforms all characters from input RDD in to lower case by applying
                                    // toLowerCase to the data
                .flatMap(text => text.split("""\W+""")) // transforms the all-lower-case RDD in to a flat array of words
                                                        // by first splitting the words inside the RDD per line by
                                                        // applying the split function and then
                                                        // flattens it in to a 1-D array
                .map(word => (word, 1)) // transform data into tuples (key,value) by using the word as key
                                        // and setting a default value of 1 for every word
                .reduceByKey(_ + _) // reduce the data by adding up values of the same key (reduce function (_ + _))
                                    // resulting to the format (word, total count) for every word

            wc.saveAsTextFile(outPath)
        } finally {
            sc.stop()
        }
    }
}
