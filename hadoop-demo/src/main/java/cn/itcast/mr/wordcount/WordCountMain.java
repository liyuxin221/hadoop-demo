package cn.itcast.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * mapreduce程序执行的入口类 继承confired 实现Tool的工具类
 *
 * @author: Liyuxin wechat:13011800146. @Title: WordCountMain @ProjectName hadoop-parent
 * @date: 2019/2/25 21:49
 * @description:
 */
public class WordCountMain extends Configured implements Tool {

  /**
   * 程序的入口类
   *
   * @param args
   */
  public static void main(String[] args) throws Exception {
    Configuration configuration = new Configuration();

    // 提交job任务,准备执行,返回一个int类型的值,程序的退出状态码
    int run = ToolRunner.run(configuration, new WordCountMain(), args);

    // 根据状态码,退出整个程序
    System.exit(run);
  }

  // 用于组织job对象
  @Override
  public int run(String[] strings) throws Exception {

    // 第一步:获取job对象
    Job job = Job.getInstance(this.getConf(), "wordCount");

    //如果需要mr程序打包到集群上面去运行,我们需要设置我们的class类
    job.setJarByClass(WordCountMain.class);

    // 第一步:读取文件,解析成key value对, k1 v1
    job.setInputFormatClass(TextInputFormat.class);

    // TextInputFormat.addInputPath(job, new Path("hdfs://node-1:8020//wordcount"));
    // 本地模式运行
    TextInputFormat.addInputPath(
        job,
        new Path(
            "file:///D:\\001 JavaWeb\\00 itheima\\04 就业班\\day53-hadoop day03\\离线阶段第三天课\\第三天的资料与视频\\3、大数据离线第三天\\wordcount\\input"));

    // 第二步:自定义mapper逻辑,接收k1,v1 转换成 k2,v2
    job.setMapperClass(WordCountMapper.class);
    // 设置k2输出的类型
    job.setMapOutputKeyClass(Text.class);
    // 设置v2输出的类型
    job.setOutputValueClass(IntWritable.class);

    // 第三步 分区,省掉 相同的key的数据发送到同一个reducer里面去.相同的key合并,value形成一个集合
    // 第四步 省略
    // 第五步 省略
    // 第六步 分组 省略

    // 第七部 自定义reduce逻辑 ,接收 k2 v2 输出 k3 v3
    job.setReducerClass(WordCountReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    // 第八步 输出k3,v3
    job.setOutputFormatClass(TextOutputFormat.class);
    // 设置输出路径,注意:输出路径一定不能存在,否则报错
    // TextOutputFormat.setOutputPath(job, new Path("hdfs://node-1:8020/wordcount_out"));

    // 使用本地模式运行
    TextOutputFormat.setOutputPath(
        job,
        new Path(
            "file:///D:\\001 JavaWeb\\00 itheima\\04 就业班\\day53-hadoop day03\\离线阶段第三天课\\第三天的资料与视频\\3、大数据离线第三天\\wordcount\\input\\output_result"));

    boolean b = job.waitForCompletion(true);

    return b?0:1;
  }
}
