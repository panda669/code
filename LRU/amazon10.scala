package cachereplace.lru

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//这个实验可以测试Spark的迭代计算性能和内存管理性能
object amazon10 extends Logging{

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName.stripSuffix("$"))
    val sc = new SparkContext(conf)


    //迭代次数
    val iters = 10

    //节点个数262111个
    val num = 262110

    //1.读取数据（边）
    val lines: RDD[String] = sc.textFile("hdfs://linux01:8020/HiBench/Pagerank/Amazon0302.txt")

    //根据边关系数据生成 邻接表 如：(1,(2,3,4,5)) (2,(1,5))...
    val link: RDD[(String, String)] = lines.map(line => {
      val parts: Array[String] = line.split("\\s+")
      (parts(0), parts(1))
    })


    val links: RDD[(String, Iterable[String])] = link.groupByKey()

    val vertices: RDD[Int] = sc.parallelize(0 to num)

    //规定每个网页的初始概率为1，α值为0.85
    var ranks: RDD[(String, Double)] = vertices.map(x => (x.toString, 1.0))

    //3.迭代次数
    for (i <- 1 to iters) {
      // 3.1 将links和ranks表等值连接，连接后为(网页,(跳转网页数组,rank值))
      val join: RDD[(String, (Iterable[String], Double))] = links.join(ranks)

      // 3.2 用于分别计算每个网页在其他网页上的跳转概率
      val flatMap: RDD[(String, Double)] = join.flatMap({
        case (str, (arrys, rank)) => arrys.map(x => (x, rank / arrys.size))
      })
      // 3.3 聚合各个网页跳转概率，得到总的跳转概率（MV）
      //函数式编程: x和y是相同key的不同value值
      val reduceByKeyRDD: RDD[(String, Double)] = flatMap.reduceByKey((x, y) => x + y)
      // 3.4 计算新的rank值:只对一个key的value进行处理 (PR)
      //公式：PR = αMV+(1-α)e   其中α=0.85,e为总节点（网页个数）的倒数
      ranks = reduceByKeyRDD.mapValues(0.85 * _ + 0.15 / (num + 1))
    }
    ranks.count()
    sc.stop()
  }


}
