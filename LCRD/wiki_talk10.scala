package cachereplace.mycache

import cachereplace.MyListener
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//这个实验可以测试Spark的迭代计算性能和内存管理性能
object wiki_talk10 extends Logging{
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName.stripSuffix("$"))
    val sc = new SparkContext(conf)

	val myListener = new MyListener
   
    //迭代次数
    val iters = 10

    //节点个数2394385个
    val num = 2394384

    val lines: RDD[String] = sc.textFile("hdfs://linux01:8020/HiBench/Pagerank/wiki_talk.txt")
    myListener.rddIdMap+=(lines.id->1)
    myListener.rddObjectMap+=(lines.id -> lines)

    val link: RDD[(String, String)] = lines.map(line => {
      val parts: Array[String] = line.split("\\s+")
      (parts(0), parts(1))
    })
    myListener.rddIdMap+=(link.id->1)
    myListener.rddObjectMap+=(link.id -> link)

    val links: RDD[(String, Iterable[String])] = link.groupByKey()
    myListener.rddIdMap+=(links.id->1)
    myListener.rddObjectMap+=(links.id -> links)

    val vertices: RDD[Int] = sc.parallelize(0 to num)
    myListener.rddIdMap+=(vertices.id->1)
    myListener.rddObjectMap+=(vertices.id -> vertices)

    var ranks: RDD[(String, Double)] = vertices.map(x => (x.toString, 1.0))
    myListener.rddIdMap+=(ranks.id->1)
    myListener.rddObjectMap+=(ranks.id -> ranks)

    for (i <- 1 to iters) {
   
      val join: RDD[(String, (Iterable[String], Double))] = links.join(ranks)
      myListener.rddIdMap+=(join.id->1)
      myListener.rddObjectMap+=(join.id -> join)

      val flatMap: RDD[(String, Double)] = join.flatMap({
        case (str, (arrys, rank)) => arrys.map(x => (x, rank / arrys.size))
      })
      myListener.rddObjectMap+=(flatMap.id -> flatMap)
      myListener.rddIdMap+=(flatMap.id->1)

      val reduceByKeyRDD: RDD[(String, Double)] = flatMap.reduceByKey((x, y) => x + y)
      myListener.rddObjectMap+=(reduceByKeyRDD.id -> reduceByKeyRDD)
      myListener.rddIdMap+=(reduceByKeyRDD.id->1)

      ranks = reduceByKeyRDD.mapValues(0.85 * _ + 0.15 / (num + 1))
      myListener.rddObjectMap+=(ranks.id -> ranks)
      myListener.rddIdMap+=(ranks.id->1)
    }
    ranks.count()
    sc.stop()
  }


}
