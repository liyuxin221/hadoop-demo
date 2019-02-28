package cn.itcast.mr.demo01.step02;

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
 * @author: Liyuxin wechat:13011800146.
 * @Title: Step02Main
 * @ProjectName hadoop-parent
 * @date: 2019/2/28 17:35
 * @description:
 */
public class Step02Main extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int run = ToolRunner.run(new Configuration(), new Step02Main(), args);

		System.exit(run);

	}

	@Override
	public int run(String[] args) throws Exception {
		// 获取job对象
		Job job = Job.getInstance(super.getConf(), "step02Job");

		//第一步 获取资源
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(
				job,
				new Path(
						"file:///D:\\001 JavaWeb\\00 itheima\\04 就业班\\day53-hadoop day03\\离线阶段第三天课\\第四天与第五天的资料\\5、大数据离线第五天\\共同好友\\out_put_xxx"));

		//第二步 自定义map逻辑
		job.setMapperClass(Step02Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		//第三部 分区

		//第四步 排序

		//第五步 规约
		job.setCombinerClass(Step02Combiner.class);

		//第六步 分组

		//第七步 自定义reduce逻辑
		job.setReducerClass(Step02Reducer.class);

		//第八步 输出
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path("file:///D:\\001 JavaWeb\\00 itheima\\04 就业班\\day53-hadoop day03\\离线阶段第三天课\\第四天与第五天的资料\\5、大数据离线第五天\\共同好友\\step02_out_put_xxx"));

		//组装job
		boolean b = job.waitForCompletion(true);

		return b ? 0 : 1;
	}
}
