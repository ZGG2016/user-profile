package cn.itcast.tags.test

import cn.itcast.tags.up.OozieParam
import cn.itcast.tags.up.OozieUtils.{genProperties, start, store, uploadConfig}

object OozieUtilsTest {
	/**
	 * 调用方式展示
	 * 测试oozie调度：
	 *     正常逻辑为：前端系统点击启动后，就会通过oozie根据 schedules workflow属性，调度对应的spark标签任务
	 * 此测试需要注意：
	 *    1.把jar包上传hdfs
	 *    2.确保在resources目录有up.conf文件及其内容，在resources/oozie目录下有xml文件
	 *    3.xml文件不用上传
	 *    4.执行此程序前，修改执行时间
	 */
	def main(args: Array[String]): Unit = {
		// 模拟从前端系统获取参数
		val param = OozieParam(
			0,
			"org.apache.spark.examples.SparkPi",
			"hdfs://bigdata-cdh01.itcast.cn:8020/apps/tags/models/tag_0/lib/model.jar",
			"",
			"2023-10-31T20:53+0800",
			"2023-10-31T20:59+0800"
		)
		// 将读取到配置属性放到 property 对象中
		val prop = genProperties(param)
		// 上传xml文件到hdfs
		uploadConfig(param.modelId)
	  // property 对象中的配置属性写入到job.properties ，并上传到hdfs
		store(param.modelId, prop)
		// 启动任务
		start(prop)
	}
}
