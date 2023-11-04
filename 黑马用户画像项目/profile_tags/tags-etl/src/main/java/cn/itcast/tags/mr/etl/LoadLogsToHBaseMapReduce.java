package cn.itcast.tags.mr.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;

/**
 * 将Hive表数据转换为HFile文件并移动HFile到HBase
 */
public class LoadLogsToHBaseMapReduce extends Configured implements Tool {

    private static Connection connection = null;

    // 思考输出的 k v 是 ImmutableBytesWritable, Pu 类型的原因
    static class LoadLogsToHBase extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\\t");
            if (splits.length == Constants.list.size()) {
                Put put = new Put(Bytes.toBytes(splits[0]));
                for (int i = 1; i < splits.length; i++) {
                    put.addColumn(
                            Constants.COLUMN_FAMILY,
                            Constants.list.get(i),
                            Bytes.toBytes(splits[i])
                    );
                }
                context.write(new ImmutableBytesWritable(put.getRow()), put);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = super.getConf();
        Job job = Job.getInstance(configuration);
        job.setJobName(this.getClass().getSimpleName());
        job.setJarByClass(LoadLogsToHBaseMapReduce.class);

        FileInputFormat.addInputPath(job, new Path(Constants.INPUT_PATH));
        job.setMapperClass(LoadLogsToHBase.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        // TODO: 设置输出格式为HFileOutputFormat2
        job.setOutputFormatClass(HFileOutputFormat2.class);

        FileSystem fs = FileSystem.get(configuration);
        Path outputPath = new Path(Constants.HFILE_PATH);
        if (fs.exists(outputPath)){
            fs.delete(outputPath,true);
        }

        FileOutputFormat.setOutputPath(job, outputPath);
        // TODO：获取HBase Table，对HFileOutputFormat2进行设置
        Table table = connection.getTable(TableName.valueOf(Constants.TABLE_NAME));
        // 在这里配置了reducer
        HFileOutputFormat2.configureIncrementalLoad(
                job,
                table,  // 目的获取HBase表的Region个数，决定生成HFile文件的个数，一个Region中对对应一个文件
                connection.getRegionLocator(TableName.valueOf(Constants.TABLE_NAME))
        );

        boolean isSuccess = job.waitForCompletion(true);

        return isSuccess ? 0 : 1;
    }

    public static void main(String[] args) throws Exception{
        // 获取Configuration对象，读取配置信息
        Configuration configuration = HBaseConfiguration.create();
        // 获取HBase 连接Connection对象
        connection = ConnectionFactory.createConnection(configuration);
        // 运行MapReduce将数据文件转换为HFile文件
        int status = ToolRunner.run(configuration, new LoadLogsToHBaseMapReduce(), args);
        System.out.println("HFile文件生成完毕!~~~");
        // TODO：运行成功时，加载HFile文件数据到HBase表中
        if (0 == status){
            Admin admin = connection.getAdmin();
            Table table = connection.getTable(TableName.valueOf(Constants.TABLE_NAME));
            LoadIncrementalHFiles load = new LoadIncrementalHFiles(configuration);
            // 将HFile文件放到表的region下
            load.doBulkLoad(
                    new Path(Constants.HFILE_PATH),
                    admin,
                    table,
                    connection.getRegionLocator(TableName.valueOf(Constants.TABLE_NAME))
            );
            System.out.println("HFile文件移动完毕!~~~");
        }
    }
}
