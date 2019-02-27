package cn.itcast.mr.demo03.flowPartition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 需求:在FlowSort结果的基础上,将手机号按照开头三位(135-139)进行分组.
 * 实现:使用分组来实现,定义4个分组,不同开头的手机号发送到不同的分组中,由不同的reduceTask来处理,保存到不同的本地文件中
 *
 * @author: Liyuxin wechat:13011800146. @Title: FlowMain @ProjectName hadoop-parent
 * @date: 2019/2/27 17:18
 * @description:
 */
public class FlowMain extends Configured implements Tool {

  public static void main(String[] args) throws Exception {

    Configuration configuration = new Configuration();
    configuration.set("mapreduce.output.fileoutputformat.compress", "true");
    configuration.set("mapreduce.output.fileoutputformat.compress.type", "RECORD");

    configuration.set(
        "mapreduce.output.fileoutputformat.compress.codec",
        "org.apache.hadoop.io.compress.SnappyCodec");

    // 配置map阶段输出的数据进行压缩
    configuration.set("mapreduce.map.output.compress", "true");
    configuration.set(
        "mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");

    int run = ToolRunner.run(configuration, new FlowMain(), args);

    System.exit(run);
  }

  @Override
  public int run(String[] args) throws Exception {
    // 获取job对象
    Job job = Job.getInstance(super.getConf(), "flowPartitionJob");

    job.setJarByClass(FlowMain.class);

    // 第一步 获取资源 输出k1 v1
    job.setInputFormatClass(TextInputFormat.class);
    TextInputFormat.addInputPath(job, new Path(args[1]));

    // 第二步 自定义map逻辑
    job.setMapperClass(FlowMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(FlowBean.class);

    // 第三部 分区
    job.setPartitionerClass(FlowPartition.class);
    // 设置reduceTask个数
    job.setNumReduceTasks(Integer.parseInt(args[0]));

    // 第四步 排序

    // 第五步 规约
    job.setCombinerClass(FlowCombiner.class);

    // 第六步 分组

    // 第七步 自定义reduce逻辑
    job.setReducerClass(FlowReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FlowBean.class);

    // 第八步 输出
    job.setOutputFormatClass(TextOutputFormat.class);
    // 设置输出路径
    TextOutputFormat.setOutputPath(job, new Path(args[2]));
    boolean b = job.waitForCompletion(true);

    return b ? 0 : 1;
  }
}
