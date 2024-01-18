package cachereplace

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler._
import org.apache.spark.storage.RDDInfo

import scala.collection.mutable.ListBuffer


class MyListener extends SparkListener with Logging {

  //用于保存RDD的对象映射Map(rdd.id->RDD[_])
  var rddIdMap=Map[Int, Int]()
  var rddObjectMap = Map[Int, RDD[_]]()
  var CacheRddList = ListBuffer[Int]()
  var maxValue:Double=0.0

  //① RDD的重用频率
  var rddReUse: Map[Int, Int] = Map[Int, Int]()
  //② RDD的算子复杂度
  var trans: Map[Int, Int] = Map[Int, Int]()
  //③ 分区数量
  var partionNum: Map[Int, Int] = Map[Int, Int]()
  //④ RDD的阶段血缘长度
  var stageLong: Map[Int, Int] = Map[Int, Int]()
  var stageRddIdList: Map[Int, ListBuffer[Int]] = Map[Int, ListBuffer[Int]]()

  // 当一个job启动开始调用
  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    //遍历每个阶段
    val stageInfos: Seq[StageInfo] = jobStart.stageInfos
    for(i<- 1 to stageInfos.length){
      //获取该阶段所有的rdd信息
      val stage = stageInfos(i - 1)
      val rddInfos = stage.rddInfos
      //保存一个阶段所有RDD的id，用于RDD的阶段血缘长度的计算
      val rddIdOrder =  ListBuffer[Int]()
      for(j<-1 to rddInfos.length){
        val rdd: RDDInfo = rddInfos(j - 1)
        //1.计算RDD的算子复杂度（以RDD的父RDD个数为准）
        val rddId = rdd.id
        val citeNum = rdd.parentIds.length
        val notExist = trans.getOrElse(rddId,-1)
        if(notExist == -1){
          trans+=(rddId->citeNum)
        }
        //2.保存rdd的重用频率：-1表示不存在，则进行保存
        val whetherExist = rddReUse.getOrElse(rddId, -1)
        if(whetherExist == -1){
          rddReUse+=(rddId->1)
        }else{
          rddReUse-=rddId
          rddReUse+=(rddId->(whetherExist+1))
        }
        //3.rdd的分区数量
        val parNum = partionNum.getOrElse(rddId, -1)
        if(parNum == -1){
          partionNum+=(rddId->rdd.numPartitions)
        }
        //4.RDD的id序列列表(此时为乱序)
        val rddidexist = rddIdMap.getOrElse(rddId, -1)
        if(rddidexist!= -1){
          rddIdOrder+=rddId
        }
      }
      //rdd的id升序之后的序列
      val idOder = rddIdOrder.sortWith((x, y) => x < y)
      val stageExist = stageLong.getOrElse(stage.stageId, -1)
      if(stageExist == -1){
        stageRddIdList += (stage.stageId->idOder)
      }
      //求出RDD在阶段中的实际距离
      for(n<-1 to rddInfos.length){
        val rdd: RDDInfo = rddInfos(n - 1)
        val rddId = rdd.id
        val parNum = rdd.parentIds.length
        val noExistRDD = rddObjectMap.getOrElse(rddId, -1)
        var mark=0
        for(m<-1 to idOder.length){
          val id = idOder(m-1)
          //
          if(noExistRDD == -1){
            if(id>rddId){
              if(mark==0){
                mark=1
                trans-=(id)
                trans+=(id->parNum)
              }
            }
          }
          //
          if(rddId==id){
            val isHave = stageLong.getOrElse(rddId, -1)
            //rdd在阶段中第一次出现
            if(isHave == -1){
              stageLong+=(rddId->(m+1))
            }
          }
        }
      }
    }

    /////////////////////////////////////////////////////////////////////////////
    for(stage<- stageInfos){
      val rdds = stage.rddInfos
      for (rdd <- rdds) {
        val rddExist = rddIdMap.getOrElse(rdd.id, -1)
        if (rddExist != -1) {
          //计算每一个RDD的权重并缓存
          rddValue(rdd.id)
        }
      }
      def rddValue(rddId: Int) = {
        //①使用频率
        val rddUse = rddReUse(rddId)
        //② RDD的算子复杂度
        val rddTrans = trans(rddId)
        //③ 分区数量
        val rddPartions = partionNum(rddId)
        //④ RDD的阶段血缘长度
        val rddStageLong = stageLong(rddId)
        val value = ((rddUse * rddTrans * rddStageLong) / rddPartions.toDouble)*100
        if (value > maxValue) {
         maxValue = value
          rddObjectMap(rddId).cache()
          CacheRddList+=rddId
          /*
              缓存的RDD查看
           */
          //println("缓存的RddID："+ rddId + "  权重/价值/重用："+value)
        }
      }
    }
    ////////////////////////////////////////////////////////

  }


  //4.当一个stage提交的时候的时候调用  Called when a stage is submitted
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    for(rddId<-CacheRddList){
      val rddUse = rddReUse(rddId)
      if(rddUse==1){
        rddObjectMap(rddId).unpersist()
      }
    }
  }
  // 2.当一个stage执行成功或者失败的时候调用，包含了已成stage的信息
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {

  }

  // 3.当一个job执行成功或者失败的时候调用，包含了已完成job的信息    Called when a job ends
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    for(rddCacheId <- CacheRddList){
        rddObjectMap(rddCacheId).unpersist()
    }
  }

  // 5.当一个task任务开始时候调用
  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {}

  // 6.当一个task结束时候调用
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
  }

  //7.当一个task执行成功或者失败的时候调用，包含了已完成task的信息
  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {}
  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {}
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {}
  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = {}
  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {}
  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = {}
  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = {}
  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {}
  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {}
  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {}
  override def onExecutorBlacklisted(executorBlacklisted: SparkListenerExecutorBlacklisted): Unit = {}
  override def onExecutorBlacklistedForStage(executorBlacklistedForStage: SparkListenerExecutorBlacklistedForStage): Unit = { }
  override def onNodeBlacklistedForStage(nodeBlacklistedForStage: SparkListenerNodeBlacklistedForStage): Unit = { }
  override def onExecutorUnblacklisted(executorUnblacklisted: SparkListenerExecutorUnblacklisted): Unit = {}
  override def onNodeBlacklisted(nodeBlacklisted: SparkListenerNodeBlacklisted): Unit = {}
  override def onNodeUnblacklisted(nodeUnblacklisted: SparkListenerNodeUnblacklisted): Unit = {}
  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = { }
  override def onSpeculativeTaskSubmitted(speculativeTask: SparkListenerSpeculativeTaskSubmitted): Unit = {}
  override def onOtherEvent(event: SparkListenerEvent): Unit = {}

}