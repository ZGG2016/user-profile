package cn.itcast.tags.utils

import cn.itcast.tags.config.ModelConfig
import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util
import java.util.Map

/**
 * 创建SparkSession对象工具类
 */
object SparkUtils {

  /**
   * 加载Spark Application默认配置文件，设置到SparkConf中
   * @param resource 资源配置文件名称
   * @return SparkConf对象
   */
  def loadConf(resource: String): SparkConf = {
    val conf = new SparkConf()
    val config: Config = ConfigFactory.load(resource)
    val entrySet: util.Set[Map.Entry[String, ConfigValue]] = config.entrySet()
    import scala.collection.JavaConverters._

    entrySet.asScala.foreach{ entry=>
      val resourceName = entry.getValue.origin().resource()
      if (resource.equals(resourceName)){
        conf.set(entry.getKey, entry.getValue.unwrapped().toString)
      }
    }
    conf
  }

  /**
   * 构建SparkSession实例对象，如果是本地模式，设置master
   * @return
   */
  def createSparkSession(classz: Class[_], isHive: Boolean = false): SparkSession = {
    val conf: SparkConf = loadConf("spark.properties")
    if (ModelConfig.APP_IS_LOCAL){
      conf.setMaster(ModelConfig.APP_SPARK_MASTER)
    }

    var builder: SparkSession.Builder = SparkSession.builder()
      .appName(classz.getSimpleName.stripSuffix("$"))
      .config(conf)
    // 4. 判断应用是否集成Hive，如果集成，设置Hive MetaStore地址
    // 如果在config.properties中设置集成Hive，表示所有SparkApplication都集成Hive；否则判断isHive，表示针对某个具体应用是否集成Hive
    if (ModelConfig.APP_IS_HIVE || isHive){
      builder = builder.config("hive.metastore.uris", ModelConfig.APP_HIVE_META_STORE_URL).enableHiveSupport()
    }

    val session = builder.getOrCreate()

    session
  }
}


