package com.csap.spqrk

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ErrorCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("union").setMaster("local")
    val sc = new SparkContext(conf)
    
    val lines = sc.textFile("D:\\error.log")
    val errors = lines.filter { lines => lines.contains("error") }
    val val_a = lines.filter { lines => lines.contains("a") }
    
    val val_union = errors.union(val_a)
    
    println("errorsCount is :",errors.count())
    
    errors.take(10).foreach(println)
    
    
  }
  
}