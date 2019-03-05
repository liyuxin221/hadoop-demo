package cn.itcast.mr2.preprocess;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author: Liyuxin wechat:13011800146. @Title: WebLogPreProcess @ProjectName hadoop-parent
 * @date: 2019/3/5 19:25
 * @description:
 */
public class WebLogPreProcess extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int run = ToolRunner.run(new Configuration(), new WebLogPreProcess(), args);

		System.exit(run);
	}

	@Override
	public int run(String[] strings) throws Exception {
		// 获取job对象
		Job job = Job.getInstance(super.getConf(), "webLogPreProcessJob");

		job.setJarByClass(WebLogPreProcess.class);

		// 第一步 获取资源
		job.setInputFormatClass(TextInputFormat.class);
		// TODO: 2019/3/ a
		TextInputFormat.addInputPath(
				job, new Path("D:\\001 JavaWeb\\00 itheima\\04 就业班\\day58-项目 day02\\input"));

		// 第二步 自定义map逻辑
		job.setMapperClass(WebLogMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		// 第三步 分区

		// 第四步 排序

		// 第五步 规约

		// 第六步 分组

		job.setNumReduceTasks(0);
		// 第七步 自定义reduce逻辑

		// 第八步 输出
		// job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(
				job, new Path("D:\\001 JavaWeb\\00 itheima\\04 就业班\\day58-项目 day02\\ouputOfmine"));

		boolean b = job.waitForCompletion(true);

		return b ? 0 : 1;
	}
}
