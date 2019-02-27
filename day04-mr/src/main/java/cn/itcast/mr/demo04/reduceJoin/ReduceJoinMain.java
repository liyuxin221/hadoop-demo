package cn.itcast.mr.demo04.reduceJoin;

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
 * @author: Liyuxin wechat:13011800146. @Title: ReduceJoinMain @ProjectName hadoop-parent
 * @date: 2019/2/27 20:17
 * @description:
 */
public class ReduceJoinMain extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int run = ToolRunner.run(new Configuration(), new ReduceJoinMain(), args);

		System.exit(run);
	}

	@Override
	public int run(String[] args) throws Exception {
		// 获取job
		Job job = Job.getInstance();

		// 第一步 获取资源
		job.setInputFormatClass(TextInputFormat.class);
    TextInputFormat.addInputPath(
        job,
        new Path(
            "file:///D:\\001 JavaWeb\\00 itheima\\04 就业班\\day53-hadoop day03\\离线阶段第三天课\\第四天与第五天的资料\\4、大数据离线第四天\\map端join\\input"));

		// 第二步 自定义map逻辑
		job.setMapperClass(ReduceMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// 第三步 分区  第四步 排序

		// 第五步 规约
		job.setCombinerClass(ReduceCombiner.class);

		// 第六步 分组

		// 第七步 自定义reduce逻辑
		job.setReducerClass(ReduceReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// 第八步 输出
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(
				job,
				new Path(
						"file:///D:\\001 JavaWeb\\00 itheima\\04 就业班\\day53-hadoop day03\\离线阶段第三天课\\第四天与第五天的资料\\4、大数据离线第四天\\map端join\\reduce_join_lyx1"));

		// 组装job
		boolean b = job.waitForCompletion(true);

		return b ? 0 : 1;
	}
}
