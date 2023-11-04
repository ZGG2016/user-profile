package cn.itcast.tags.mr.etl.hfile

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Table}
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.TreeMap

/**
 * 将数据存储文本文件转换为HFile文件，加载到HBase表中
 */
object HBaseBulkLoader {

  /**
   * 依据不同表的数据文件，提取对应数据，封装到KeyValue对象中
   */
  def transLine(line: String, family: String,
                fieldNames: TreeMap[String, Int]): List[(ImmutableBytesWritable, KeyValue)] = {
    val length = fieldNames.size
    val fieldValue: Array[String] = line.split("\\t", -1)

    if(null == fieldValue || fieldValue.length != length) return Nil

    // 获取id，构建RowKey
    val rowKey = Bytes.toBytes(fieldValue(0))
    val rk = new ImmutableBytesWritable(rowKey)

    // 构建KeyValue对象  key:rowkey+cf+fieldName  value: 列值
    // https://www.jianshu.com/p/179d4430956c
    fieldNames.toList.map {
      case (name, index) =>
        val keyValue = new KeyValue(
          rowKey,
          Bytes.toBytes(family),
          Bytes.toBytes(name),
          Bytes.toBytes(fieldValue(index))
        )
        (rk, keyValue)
    }
  }

  def main(args: Array[String]): Unit = {
    // 应用执行时传递5个参数：数据类型、HBase表名称、表列簇、输入路径及输出路径
    /*
    args = Array("1", "tbl_tag_logs", "detail", "/user/hive/warehouse/tags_dat.db/tbl_logs", "/datas/output_hfile/tbl_logs")
    args = Array("2", "tbl_tag_users", "detail", "/user/hive/warehouse/tags_dat.db/tbl_users", "/datas/output_hfile/tbl_users")
    args = Array("3", "tbl_tag_orders", "detail", "/user/hive/warehouse/tags_dat.db/tbl_orders", "/datas/output_hfile/tbl_orders")
    args = Array("4", "tbl_tag_goods", "detail", "/user/hive/warehouse/tags_dat.db/tbl_goods", "/datas/output_hfile/tbl_goods")
    */
    // 将传递赋值给变量， 其中数据类型：1Log、2User、3Order、4Good
    val Array(dataType, tableName, family, inputDir, outputDir) = Array(
      "3", "tbl_orders", "detail",
      "/user/hive/warehouse/tags_dat.db/tbl_orders", "/datas/output_hfile/tbl_orders"
    )

    // 依据参数获取处理数据schema
    val fieldNames: TreeMap[String, Int] = dataType.toInt match {
      case 1 => TableFieldNames.LOG_FIELD_NAMES
      case 2 => TableFieldNames.USER_FIELD_NAMES
      case 3 => TableFieldNames.ORDER_FIELD_NAMES
      case 4 => TableFieldNames.GOODS_FIELD_NAMES
    }

    // 1. 构建SparkContext实例对象
    val sc: SparkContext = {
      val conf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[2]")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .registerKryoClasses(Array(classOf[ImmutableBytesWritable]))
      SparkContext.getOrCreate(conf)
    }

    // 2. 读取文本文件数据，转换格式
    val kvRDD: RDD[(ImmutableBytesWritable, KeyValue)] = sc
      .textFile(inputDir)
      .filter(line => null != line)
      // 提取数据字段，构建二元组(RowKey, KeyValue)
      /*
        Key: rowkey + cf + column + version(timestamp)
        Value: ColumnValue
       */
      .flatMap(line => transLine(line, family, fieldNames))
      .sortByKey()

    // TODO：构建Job，设置相关配置信息，主要为输出格式
    val configuration = HBaseConfiguration.create()
    val dfs: FileSystem = FileSystem.get(configuration)
    val outputPath: Path = new Path(outputDir)
    if (dfs.exists(outputPath)) {
      dfs.delete(outputPath, true)
    }
    dfs.close()

    val connection = ConnectionFactory.createConnection(configuration)
    val htableName = TableName.valueOf(tableName)
    val table: Table = connection.getTable(htableName)
    HFileOutputFormat2.configureIncrementalLoad(
      Job.getInstance(configuration),
      table,
      connection.getRegionLocator(htableName)
    )

    // TODO：3. 保存数据为HFile文件
    kvRDD.saveAsNewAPIHadoopFile(
      outputDir,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      configuration
    )

    // TODO：4. 将输出HFile加载到HBase表
    val load = new LoadIncrementalHFiles(configuration)
    load.doBulkLoad(
      outputPath,
      connection.getAdmin,
      table,
      connection.getRegionLocator(htableName)
      )

    sc.stop()
  }
}
