package com.ascc

import org.apache.spark.SparkContext
import com.ascc.util.Files


/**
  * Get the Max Price of a stock per year based on historical data.
  * Table Columns are as follows:
  * Date | Open | High | Low | Close | Volume | Adj Close
  */
object MaxPrice {

    def main(args: Array[String]): Unit = {
        val inPath  = "resources/table.csv"
        val outPath = "output/max_price"
        Files.rmrf(outPath)  // delete old output (DON'T DO THIS IN PRODUCTION!)
        println("...creating SparkContext\n")

        val sc = new SparkContext("local[*]", "Max Price")
        sc.setLogLevel("WARN")
        try {
            sc.textFile(inPath)
                .map(_.split(","))
                .map(rec => ((rec(0).split("-"))(0).toInt,(rec(1).toFloat,rec(2).toFloat,rec(3).toFloat,rec(4).toFloat,rec(5).toFloat,rec(6).toFloat)))
                .reduceByKey((x,y) => (math.max(x._1,y._1),math.max(x._2,y._2),math.max(x._3,y._3),math.max(x._4,y._4),math.max(x._5,y._5),math.max(x._6,y._6)))
                .sortBy(_._1, false)
                .coalesce(1,true).saveAsTextFile(outPath)

        } finally {
            sc.stop()
        }
    }
}
