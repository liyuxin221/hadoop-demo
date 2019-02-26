package cn.itcast.mr.demo02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 将数据分开处理,15以上的结果及15以下的结果进行分开成两个文件保存.
 *
 * @author: Liyuxin wechat:13011800146. @Title: CompareMain @ProjectName hadoop-parent
 * @date: 2019/2/26 19:27
 * @description:
 */
public class CompareMain extends Configured implements Tool {

  /**
   * 程序的入口函数
   *
   * @param args
   */
  public static void main(String[] args) throws Exception {

    int i = ToolRunner.run(new Configuration(), new CompareMain(), args);

    System.exit(i);
  }

  @Override
  public int run(String[] strings) throws Exception {

    // 第一步 获取资源,读取文件
    Job job = Job.getInstance(this.getConf(), "compareJob");

    // 集群运行需要添加
    job.setJarByClass(CompareMain.class);

    job.setInputFormatClass(TextInputFormat.class);
    TextInputFormat.addInputPath(job, new Path("hdfs://node-1:8020/compare_in"));

    // 第二步 自定义mapper 将k1 v1转换成 k2 v2
    job.setMapperClass(CompareMapper.class);
    job.setMapOutputKeyClass(PairTwo.class);
    job.setMapOutputValueClass(NullWritable.class);

    // 第三步 分区 自定义分区设置
    // 第四步 排序
    // 第五步 规约

    //设置规约类
    job.setCombinerClass(MyCombiner.class);

    // 第七步 自定义reduce逻辑,将接收的k2 v2转换成k3 v3输出
    job.setReducerClass(CompareReducer.class);
    job.setOutputKeyClass(PairTwo.class);
    job.setOutputValueClass(NullWritable.class);

    // 第八步 输出k3 v3
    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, new Path("hdfs://node-1:8020/compare_out"));

    // 设置reduceTask数量
    // job.setNumReduceTasks(2);

    // 组装job
    boolean b = job.waitForCompletion(true);
    return b ? 0 : 1;
  }
}
