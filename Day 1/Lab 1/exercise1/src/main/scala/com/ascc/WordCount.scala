package com.ascc

import org.apache.spark.SparkContext
import com.ascc.util.Files

/**
  * Count the words in a corpus of documents.
  */
object WordCount {

    def main(args: Array[String]): Unit = {
        val inPath = "resources/all-shakespeare.txt"
        val outPath = "output/word_count1"
        Files.rmrf(outPath) // delete old output (DON'T DO THIS IN PRODUCTION!)

        val sc = new SparkContext("local[*]", "WordCount")  // create SparkContext where local[*] means run Spark
                                                            // locally with as many worker threads as logical
                                                            // cores on your machine. WordCount pertains to
                                                            // the App Name (spark's point of view)
        sc.setLogLevel("WARN")
        try {
            val input = sc.textFile(inPath)
            val wc = input
                .map(_.toLowerCase)
                .flatMap(text => text.split("""\W+"""))
                .map(word => (word, 1))
                .groupByKey()
                .mapValues(group => group.size)
                .sortBy(_._2, false)

/*            val wc = input
                .map(_.toLowerCase)
                .flatMap(text => text.split("""\W+"""))
                .map(word => (word, 1))
                .reduceByKey(_ + _)
                .sortBy(_._2, false)*/

            println("Writing output to: $outPath")
            wc.saveAsTextFile(outPath)
        } finally {
            sc.stop()
        }
    }
}
