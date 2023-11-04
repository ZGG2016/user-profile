package cn.itcast.tags.models.rule

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.TagTools
import org.apache.spark.sql.DataFrame

class JobTagModel extends AbstractModel("职业标签", ModelType.MATCH){
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    val modelDF: DataFrame = TagTools.ruleMatchTag(
      businessDF, "job", tagDF
    )
    modelDF
  }
}

object JobTagModel{
  def main(args: Array[String]): Unit = {
    val tagModel = new JobTagModel()
    tagModel.executeModel(325L, isHive = true)
  }
}
