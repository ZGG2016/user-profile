package cn.itcast.tags.models.ml

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.TagTools
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.storage.StorageLevel

/**
 * 挖掘类型标签模型开发：客户价值模型RFM
 *
 * 注意：字段的大小写
 */
class RfmModel extends AbstractModel("用户活跃度RFE", ModelType.ML) {
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    val session: SparkSession = businessDF.sparkSession
    import session.implicits._

    /*
    TODO: 1、计算每个用户RFM值
      按照用户memberid分组，然后进行聚合函数聚合统计
      R：消费周期，finishtime
        日期时间函数：current_timestamp、from_unixtimestamp、datediff
      F: 消费次数 ordersn
        count
      M：消费金额 orderamount
        sum
   */
    val rfmDF: DataFrame = businessDF
      .groupBy($"memberId")
      .agg(
        max($"finishTime").as("max_finishtime"),
        count($"orderSn").as("frequency"),
        sum(
          $"orderAmount".cast(DataTypes.createDecimalType(10, 2))
        ).as("monetary")
      )
      .select(
        $"memberId".as("userId"),
        // 计算R值：消费周期
        datediff(current_timestamp(), from_unixtime($"max_finishtime")).as("recency"),
        $"frequency",
        $"monetary"
      )
    /*
      TODO: 2、按照规则给RFM进行打分（RFM_SCORE)
        R: 1-3天=5分，4-6天=4分，7-9天=3分，10-15天=2分，大于16天=1分
            F: ≥200=5分，150-199=4分，100-149=3分，50-99=2分，1-49=1分
            M: ≥20w=5分，10-19w=4分，5-9w=3分，1-4w=2分，<1w=1分

        使用CASE WHEN ..  WHEN... ELSE .... END
     */
    // R 打分条件表达式
    val rWhen = when(col("recency").between(1, 3), 5.0)
      .when(col("recency").between(4, 6), 4.0)
      .when(col("recency").between(7, 9), 3.0)
      .when(col("recency").between(10, 15), 2.0)
      .when(col("recency").geq(16), 1.0)
    // F 打分条件表达式
    val fWhen = when(col("frequency").between(1, 49), 1.0)
      .when(col("frequency").between(50, 99), 2.0)
      .when(col("frequency").between(100, 149), 3.0)
      .when(col("frequency").between(150, 199), 4.0)
      .when(col("frequency").geq(200), 5.0)
    // M 打分条件表达式
    val mWhen = when(col("monetary").lt(10000), 1.0)
      .when(col("monetary").between(10000, 49999), 2.0)
      .when(col("monetary").between(50000, 99999), 3.0)
      .when(col("monetary").between(100000, 199999), 4.0)
      .when(col("monetary").geq(200000), 5.0)
    val rfmScoreDF: DataFrame = rfmDF.select(
      $"userId",
      rWhen.as("r_score"),
      fWhen.as("f_score"),
      mWhen.as("m_score")
    )

    /*
		TODO: 3、使用RFM_SCORE进行聚类，对用户进行分组
			KMeans算法，其中K=5
		 */
    // 3.1 组合R\F\M列为特征值features
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(Array("r_score", "f_score", "m_score"))
      .setOutputCol("features")
    val featuresDF: DataFrame = assembler.transform(rfmScoreDF)

    featuresDF.persist(StorageLevel.MEMORY_AND_DISK)

    // 3.2 使用KMeans算法聚类，训练模型
    val kMeansModel: KMeansModel = trainModel(featuresDF)

    // 3.3. 使用模型预测
    val predictionDF: DataFrame = kMeansModel.transform(featuresDF)
    featuresDF.unpersist()

    // 查看各个类簇中，RFM之和的最大和最小值
    val clusterDF: DataFrame = predictionDF
      .select(
        $"prediction",
        ($"r_score" + $"f_score" + $"m_score").as("rfm_score")
      )
      .groupBy($"prediction")
      .agg(
        max($"rfm_score").as("max_rfm"),
        min($"rfm_score").as("min_rfm")
      )
    clusterDF.show(10, truncate = false)

    // 3.4 获取类簇中心点
    val clusterCenters: Array[linalg.Vector] = kMeansModel.clusterCenters
    val centerIndexArray: Array[((Int, Double), Int)] = clusterCenters
      // (vector1, 0), (vector2, 1), ....
      .zipWithIndex
      // TODO: 对每个类簇向量进行累加和：R + F + M
      .map { case (vector, centerIndex) => (centerIndex, vector.toArray.sum) }
      // rfm表示将R + F + M之和，越大表示客户价值越高
      .sortBy { case (_, rfm) => -rfm }
      .zipWithIndex
    centerIndexArray.foreach(println)

    // TODO： 4. 打标签
    // 4.1 获取属性标签规则rule和名称tagName，放在Map集合中
    val rulesMap: Map[String, String] = TagTools.convertMap(tagDF)
    // 4.2 聚类类簇关联属性标签数据rule，对应聚类类簇与标签tagName
    val indexTagMap: Map[Int, String] = centerIndexArray
      .map {
        case ((centerIndex, _), index) =>
          val tagName = rulesMap(index.toString)
          (centerIndex, tagName)
      }
      .toMap
    // 4.3 使用KMeansModel预测值prediction打标签
    val indexTagMapBroadcast = session.sparkContext.broadcast(indexTagMap)
    val index_to_tag = udf(
      (prediction: Int) => indexTagMapBroadcast.value(prediction)
    )
    val modelDF: DataFrame = predictionDF
      .select(
        $"userId",
        index_to_tag($"prediction").as("rfm")
      )
    modelDF
  }

  /**
   * 使用KMeans算法训练模型
   * @param dataframe 数据集
   * @return KMeansModel模型
   */
  def trainModel(dataframe: DataFrame): KMeansModel = {
    // 使用KMeans聚类算法模型训练
    val kMeansModel: KMeansModel = new KMeans()
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
      .setK(5) // 设置列簇个数：5
      .setMaxIter(20) // 设置最大迭代次数
      .fit(dataframe)
    println(s"WSSSE = ${kMeansModel.computeCost(dataframe)}")

    // 返回
    kMeansModel
  }
}
object RfmModel{
  def main(args: Array[String]): Unit = {
    val tagModel = new RfmModel()
    tagModel.executeModel(365L)
  }
}