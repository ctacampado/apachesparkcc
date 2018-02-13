package course2.module1

import org.apache.spark.SparkContext
import com.ascc.util.Files

/**
  * Count the words in a corpus of documents.
  * This version uses the familiar GROUP BY, but we'll see a more efficient
  * version next.
  */
object WordCount {

    def main(args: Array[String]): Unit = {
        val inPath  = "resources/all-shakespeare.txt"
        val outPath = "output/word_count1"
        Files.rmrf(outPath)  // delete old output (DON'T DO THIS IN PRODUCTION!)
        println("...creating SparkContext\n")
        val sc = new SparkContext("local[*]", "WordCount")
        println("...SparkContext created\n")
        try {
            val input = sc.textFile(inPath)
            val wc = input
                .map(_.toLowerCase)
                .flatMap(text => text.split("""\W+"""))
                .groupBy(word => word)  // Like SQL GROUP BY: RDD[(String,Iterator[String])]
                .mapValues(group => group.size)  // RDD[(String,Int)]

            println(s"Writing output to: $outPath")
            wc.saveAsTextFile(outPath)
            //wc.sortBy(_._2, false).collect().foreach(println)
            printMsg("Enter any key to finish the job...")
            Console.in.read()
        } finally {
            sc.stop()
        }
    }

    private def printMsg(m: String) = {
        println("")
        println(m)
        println("")
    }
}
