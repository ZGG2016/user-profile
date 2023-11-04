package cn.itcast.tags.models.rule

import cn.itcast.tags.meta.HBaseMeta
import cn.itcast.tags.tools.HBaseTools
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * 标签模型应用开发：用户职业标签
 */
object JobModel extends Logging{
  def main(args: Array[String]): Unit = {
    /*
  321	职业
    322	学生		1
    323	公务员	2
    324	军人		3
    325	警察		4
    326	教师		5
    327	白领		6
   */
    val spark: SparkSession = {
      val conf: SparkConf = new SparkConf()
        .setMaster("local[2]")
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .set("spark.sql.shuffle.partitions", "4")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .registerKryoClasses(
          Array(classOf[ImmutableBytesWritable], classOf[Result], classOf[Put])
        )
      val session: SparkSession = SparkSession.builder()
        .config(conf)
        // 启用与Hive集成
        .enableHiveSupport()
        // 设置与Hive集成: 读取Hive元数据MetaStore服务
        .config("hive.metastore.uris", "thrift://bigdata-cdh01.itcast.cn:9083")
        // 设置数据仓库目录: 将SparkSQL数据库仓库目录与Hive数据仓库目录一致
        .config(
          "spark.sql.warehouse.dir", "hdfs://bigdata-cdh01.itcast.cn:8020/user/hive/warehouse"
        )
        .getOrCreate()
      session
    }

    import org.apache.spark.sql.functions._
    import spark.implicits._

    // 1. 依据TagId，从MySQL读取标签数据(4级业务标签和5级属性标签）
    val tagTable: String =
      """
			  |(
			  |SELECT id, name, rule, level  FROM profile_tags.tbl_basic_tag WHERE id = 325
			  |UNION
			  |SELECT id, name, rule, level  FROM profile_tags.tbl_basic_tag WHERE pid = 325
			  |) AS tag_table
			  |""".stripMargin
    val basicTagDF: DataFrame = spark.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url",
        "jdbc:mysql://bigdata-cdh01.itcast.cn:3306/?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC")
      .option("dbtable", tagTable)
      .option("user", "root")
      .option("password", "123456")
      .load()
    basicTagDF.persist(StorageLevel.MEMORY_AND_DISK)

//    basicTagDF.printSchema()
//    basicTagDF.show(10, truncate = false)

    // 2. 解析标签rule，从HBase读取业务数据
    // 2.1 获取业务标签规则
    val tagRule: String = basicTagDF
      .filter($"level" === 4)
      .head()
      .getAs[String]("rule")
    logWarning(s"================< $tagRule >================")

    // 2.2 解析标签规则rule，封装值Map集合
    val tagRuleMap: Map[String, String] = tagRule.split("\\n")
      .map { line =>
        val Array(attrKey, attrValue) = line.trim.split("=")
        (attrKey, attrValue)
      }.toMap
    logWarning(s"================= { ${tagRuleMap.mkString(", ")} } ================")
    // 2.3 判断数据源inType，读取业务数据
    var businessDF: DataFrame = null
    if ("hbase".equals(tagRuleMap("inType").toLowerCase())){
      val hbaseMeta = HBaseMeta.getHBaseMeta(tagRuleMap)
      businessDF = HBaseTools.read(spark, hbaseMeta.zkHosts, hbaseMeta.zkPort,
        hbaseMeta.hbaseTable, hbaseMeta.family, hbaseMeta.selectFieldNames.split(","))
    }else{
      // 如果未获取到数据，直接抛出异常
      new RuntimeException("业务标签未提供数据源信息，获取不到业务数据，无法计算标签")
    }
//    businessDF.printSchema()
//    businessDF.show(100, truncate = false)

    // 3. 业务数据结合标属性签数据，构建标签
    // 3.1 获取属性标签规则，转换为Map集合
    val attrTagRuleMap:Map[String, String] = basicTagDF
      .filter($"level" === 5)
      .select($"rule", $"name")
      // 将DataFrame转换为Dataset，由于DataFrame中只有2个元素，封装值二元组（元组就是CaseClass）中
      .as[(String, String)]
      .rdd
      .collectAsMap()
      .toMap
    val attrTagRuleMapBroadcast: Broadcast[Map[String, String]] = spark.sparkContext.broadcast(attrTagRuleMap)

    // 3.2 自定义UDF函数
    val job_udf: UserDefinedFunction = udf(
      (job: String) =>
        attrTagRuleMapBroadcast.value(job)
    )
    // 3.3 使用UDF函数，打标签  【大小表join优化】
    val modelDF: DataFrame = businessDF.select(
      $"id".as("userId"),
      job_udf($"job").as("job")
    )

    // 4. 画像标签数据存储HBase表
    HBaseTools.write(
      modelDF, "bigdata-cdh01.itcast.cn", "2181",
      "tbl_profile", "user", "userId"
    )
    spark.stop()
  }
}
