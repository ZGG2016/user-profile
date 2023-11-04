package cn.itcast.tags.up

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hdfs.client.HdfsUtils
import org.apache.oozie.client.OozieClient

import java.util.Properties


/**
 * Oozie Java Client 提交Workflow时封装参数CaseClass
 *
 * @param modelId      模型ID
 * @param mainClass    运行主类
 * @param jarPath      JAR包路径
 * @param sparkOptions Spark Application运行参数
 * @param start        运行开始时间
 * @param end          运行结束时间
 */
case class OozieParam(
                       modelId: Long,
                       mainClass: String,
                       jarPath: String,
                       sparkOptions: String,
                       start: String,
                       end: String
                     )

object OozieUtils {
  val classLoader: ClassLoader = getClass.getClassLoader;

  def genProperties(param: OozieParam): Properties = {
    val props = new Properties()

    val params: Map[String, String] = ConfigHolder.oozie.params
    for (entry <- params){
      props.setProperty(entry._1, entry._2)
    }
    val appPath = ConfigHolder.hadoop.nameNode + genAppPath(param.modelId)
    // hdfs://bigdata-cdh01.itcast.cn:8020/apps/tags/models/tag_0
//    println("==========> " + appPath)
    props.setProperty("appPath", appPath)

    props.setProperty("mainClass", param.mainClass)
    props.setProperty("jarPath", param.jarPath)

    if (StringUtils.isNotBlank(param.sparkOptions)) props.setProperty("sparkOptions", param.sparkOptions)

    props.setProperty("start", param.start)
    props.setProperty("end", param.end)
    props.setProperty(OozieClient.COORDINATOR_APP_PATH, appPath)

    props
  }

  def uploadConfig(modelId: Long): Unit = {
    val workflowFile = classLoader.getResource("oozie/workflow.xml").getPath
    val coordinatorFile = classLoader.getResource("oozie/coordinator.xml").getPath

    val path = genAppPath(modelId)
    HDFSUtils.getInstance().mkdir(path);
    HDFSUtils.getInstance().copyFromFile(workflowFile, path + "/workflow.xml");
    HDFSUtils.getInstance().copyFromFile(coordinatorFile, path + "/coordinator.xml");
  }

  def genAppPath(modelId: Long): String = {
    ConfigHolder.model.path.modelBase + "/tag_" + modelId
  }

  def store(modelId: Long, prop:Properties):Unit = {
    val appPath = genAppPath(modelId)
    // 把前面设置的属性写入到 job.properties 文件中
    prop.store(HDFSUtils.getInstance().createFile(appPath + "/job.properties"), "")
  }

  def start(prop: Properties): Unit = {
    val oozie = new OozieClient(ConfigHolder.oozie.url)
    println(prop)

    val jobId = oozie.run(prop)
    println(jobId)
  }

}
