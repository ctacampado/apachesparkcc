package com.ascc

import com.ascc.util.Files
import org.apache.spark.sql.SparkSession

/**
  * Count the words in a corpus of documents.
  */
object WordCountDS {

    def main(args: Array[String]): Unit = {
        val inPath = "resources/all-shakespeare.txt"
        val outPath = "output/word_countDS"
        Files.rmrf(outPath) // delete old output (DON'T DO THIS IN PRODUCTION!)

        val sparkSession = SparkSession.builder().master("local").appName("WordCountDS").getOrCreate()
        sparkSession.sparkContext.setLogLevel("ERROR")

        import sparkSession.implicits._

        val readFileDS = sparkSession.read.textFile(inPath).as[String]
        val wCount = readFileDS
            .coalesce(1)
            .map(_.toLowerCase)
            .flatMap(_.split("""\W+"""))
            .filter(_ != "")
            .groupBy("Value")
            .count()
            .orderBy($"count".desc)

        //wCount.show()
        wCount.write.format("csv").save(outPath)
    }
}
