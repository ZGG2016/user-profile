package cn.itcast.tags.models.statistics

import cn.itcast.tags.models.{AbstractModel, ModelType}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{regexp_replace, udf}
import org.apache.spark.sql.types.IntegerType

class AgeRangeModel extends AbstractModel("年龄段标签", ModelType.STATISTICS){

  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    import businessDF.sparkSession.implicits._

    // 1. 自定义UDF函数，解析属性标签规则rule
    val rule_to_tuple = udf(
      (rule: String) => {
        val Array(start, end) = rule.trim.split("-").map(_.toInt)
        (start, end)
      }
    )

    // 2. 针对属性标签数据中规则rule使用UDF函数，提取start和end
    val attrTagRuleDF: DataFrame = tagDF
      .filter($"level" === 5) // 5级标签
      .select(
        $"name", rule_to_tuple($"rule").as("rules")
      )
      .select(
        $"name", $"rules._1".as("start"), $"rules._2".as("end")
      )

    // 3. 使用业务数据字段：birthday 与 属性标签规则数据进行JOIN关联
    /*
      attrTagRuleDF： t2
      businessDF: t1
      SELECT t2.userId, t1.name FROM business t1 JOIN  attrTagRuleDF t2
      WHERE t2.start <= t1.birthday AND t2.end >= t1.birthday ;
     */
    val modelDF: DataFrame = businessDF
      // a. 使用正则函数，转换日期
      .select(
        $"id",
        regexp_replace($"birthday", "-", "")
          .cast(IntegerType).as("bornDate")
      )
      // b. 关联属性标签规则数据
      .join(attrTagRuleDF)
      // c. 设置条件，使用WHERE语句
      .where(
        $"bornDate".between($"start", $"end")
      )
      // d. 选取字段值
      .select(
        $"id".as("userId"),
        $"name".as("agerange")
      )
    //modelDF.printSchema()
    //modelDF.show(100, truncate = false)

    // 返回画像标签数据
    modelDF
  }
}

object AgeRangeModel{
  def main(args: Array[String]): Unit = {
    val tagModel = new AgeRangeModel()
    tagModel.executeModel(342L)
  }
}
