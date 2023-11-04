package cn.itcast.tags.models.statistics

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.TagTools
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class ConsumeCycleModel extends AbstractModel("消费周期标签", ModelType.STATISTICS){
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    businessDF.show(10)

    import businessDF.sparkSession.implicits._
    // 1. 对业务数据进行计算操作
    val consumerDaysDF: DataFrame = businessDF
      .groupBy($"memberId")
      .agg(max($"finishTime").as("max_finishtime"))
      .select(
        $"memberId",
        from_unixtime($"max_finishtime").as("finish_time"),
        current_timestamp().as("now_time")
      )
      .select(
        $"memberId".as("id"),
        datediff($"now_time", $"finish_time").as("consumer_days")
      )

    // 2. 提取属性标签规则数据
    val attrTagRuleDF: DataFrame = TagTools.convertTuple(tagDF)

    // 3. 关联DataFrame，指定条件Where语句
    val modelDF = consumerDaysDF
      .join(attrTagRuleDF)
      .where($"consumer_days".between($"start", $"end"))
      .select($"id".as("userId"), $"name".as("consumercycle"))
    modelDF
  }
}

object ConsumeCycleModel{
  def main(args: Array[String]): Unit = {
    val tagModel = new ConsumeCycleModel()
    tagModel.executeModel(351L)
  }
}