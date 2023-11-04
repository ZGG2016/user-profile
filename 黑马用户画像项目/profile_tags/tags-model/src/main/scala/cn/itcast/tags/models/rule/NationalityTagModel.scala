package cn.itcast.tags.models.rule

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.TagTools
import org.apache.spark.sql.DataFrame

class NationalityTagModel extends AbstractModel("国籍标签", ModelType.MATCH){
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    val modelDF: DataFrame = TagTools.ruleMatchTag(
      businessDF, "nationality", tagDF
    )
    modelDF
  }
}

object NationalityTagModel{
  def main(args: Array[String]): Unit = {
    val tagModel = new NationalityTagModel()
    tagModel.executeModel(336L)
  }
}
