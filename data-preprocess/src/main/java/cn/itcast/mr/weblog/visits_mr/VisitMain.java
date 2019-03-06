package cn.itcast.mr.weblog.visits_mr;

import cn.itcast.mr.weblog.pageviews.PageViewsBean;
import cn.itcast.mr.weblog.visits.VisitBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class VisitMain extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int run = ToolRunner.run(new Configuration(), new VisitMain(), args);
		System.exit(run);
	}

	@Override
	public int run(String[] strings) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setMapperClass(VisitMapper.class);
		job.setReducerClass(VisitReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PageViewsBean.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(VisitBean.class);

		FileInputFormat.setInputPaths(job, new Path("F:\\heima\\就业班\\hadoop项目讲义与资料\\2、网站流量日志分析-项目资料\\step1-数据采集--埋点\\out\\out\\part-r-00000"));
		FileOutputFormat.setOutputPath(job, new Path("F:\\heima\\就业班\\hadoop项目讲义与资料\\2、网站流量日志分析-项目资料\\step1-数据采集--埋点\\out\\out\\out"));

		boolean res = job.waitForCompletion(true);
		return res ? 0 : 1;
	}
}
