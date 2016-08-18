package com.csap.spark
/*
 * qinchaofeng 
 * 20160818
 * Transformations 与 Actions 的用法
 */
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Function {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Mapp").setMaster("local")
    
    val sc = new SparkContext(conf)
    
    val input = sc.parallelize(List(1,2,3,4))
    
    /*
     * 常用Transformations
     * map filter 用法 
     */
    val result1 = input.map(x=>x*x)
    val result2 = input.filter(x=>x!=1)
    
    print(result1.collect().mkString(","))
    print("\n")
    print(result2.collect().mkString(","))
    print("\n")
    
    /*
     * flarMap 用法
     */
    val lines = sc.parallelize(List("hello world","hi"))
    val words = lines.flatMap (lines=>lines.split(","))
    print(words.first())
    print("\n")
    
    /*
     * 常用Actions
     */
    val rdd1 = sc.parallelize(List("coffee","coffee","panda","monkey","tea"))
    val rdd2 = sc.parallelize(List("coffee","monkey","kitty"))
    
    rdd1.distinct().take(100).foreach(println)
    
    rdd1.union(rdd2).take(100).foreach(println)
    
    rdd1.intersection(rdd2).take(100).foreach(println)
    
    rdd1.subtract(rdd2).take(100).foreach(println)
    
    rdd2.reduce((x,y)=>x+y).take(100).foreach(println)
    
    
    
    val rdd3 = rdd1.distinct().count()
    val rdd4 = rdd2.count()
    
    print(rdd3+"\n")
    print(rdd1.countByValue() + "\n")
    
    rdd1.takeOrdered(10).take(100).foreach(println)
    
    rdd1.takeSample(true, 1).foreach(println)
    
    val rdd5 = sc.parallelize(List(1,2,3,4))
    print("reduce action:"+rdd5.reduce((x,y)=>x+y)+"\n")
    
    /*
     * aggregate 用法：
     * 第一步：将rdd5中的元素与初始值遍历进行聚合操作
     * 第二步：将初始值加1进行遍历聚合
     * 第三步：将结果进行聚合
     * 根据本次的RDD 背部实现如下：
     * 第一步：其实是0+1
     * 					1+2
     * 					3+3
     * 					6+4
     * 然后执行：0+1
     * 				1+1
     * 				2+1
     * 				3+1
     * 此时返回(10,4)
     * 本次执行是一个节点，如果在集群中的话，多个节点，会先把数据打到不同的分区上，比如(1,2) (3,4)
     * 得到的结果就会是(3,2) (7,2)
     * 然后进行第二步combine就得到 (10,4)
     */
    val rdd6 = rdd5.aggregate((0, 0))  ((x, y) =>(x._1 + y, x._2+1),  (x, y) =>(x._1 + y._1, x._2 + y._2))
    print ("aggregate action : " + rdd6 + "\n"  )
    
    
  }
}