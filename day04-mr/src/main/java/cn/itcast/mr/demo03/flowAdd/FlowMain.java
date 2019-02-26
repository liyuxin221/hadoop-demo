package cn.itcast.mr.demo03.flowAdd;

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
 * 流量统计求和 对每个手机号的上行流量,下行流量,上行总流量,下行总流量
 *
 * @author: Liyuxin wechat:13011800146. @Title: FlowMain @ProjectName hadoop-parent
 * @date: 2019/2/26 23:26
 * @description:
 */
public class FlowMain extends Configured implements Tool {
  public static void main(String[] args) throws Exception {
    // 执行程序
    int run = ToolRunner.run(new Configuration(), new FlowMain(), args);

    System.exit(run);
  }

  @Override
  public int run(String[] strings) throws Exception {

    // 获取job对象
    Job job = Job.getInstance(this.getConf(), "flowAddJob");

    // 第一步:获取资源
    job.setInputFormatClass(TextInputFormat.class);
    TextInputFormat.addInputPath(
        job,
        new Path("file:///D:\\001 JavaWeb\\00 itheima\\04 就业班\\day53-hadoop day03\\离线阶段第三天课\\第四天与第五天的资料\\4、大数据离线第四天\\流量统计\\input"));

    //第二步:自定义mapper类
    job.setMapperClass(FlowMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(FlowBean.class);

    //分区,排序,规约
    //设置自定义规约类
    job.setCombinerClass(FlowCombiner.class);

    //第七步 自定义reducer类
    job.setReducerClass(FlowReducer.class);

    //第八步 输出结果
    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(
        job,
        new Path(
            "file:///D:\\001 JavaWeb\\00 itheima\\04 就业班\\day53-hadoop day03\\离线阶段第三天课\\第四天与第五天的资料\\4、大数据离线第四天\\流量统计\\output_xxx2"));
    boolean b = job.waitForCompletion(true);

    return b?0:1;
  }
}
