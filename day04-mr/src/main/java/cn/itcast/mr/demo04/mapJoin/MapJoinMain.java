package cn.itcast.mr.demo04.mapJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * @author: Liyuxin wechat:13011800146. @Title: MapJoinMain @ProjectName hadoop-parent
 * @date: 2019/2/27 21:43
 * @description:
 */
public class MapJoinMain extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		//
		int run = ToolRunner.run(new Configuration(), new MapJoinMain(), args);

		System.exit(run);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = super.getConf();
		// 将文件放入分布式缓存中
//		DistributedCache.addCacheFile(new URI(args[0]), conf);
		DistributedCache.addCacheFile(new URI("hdfs://node-1:8020/mapJoin_pro/pdts.txt"), conf);

		// 获取job
		Job job = Job.getInstance(conf, "mapJoinJob");

		// job.setJarByClass(MapJoinMain.class);

		// 第一步 获取资源
		job.setInputFormatClass(TextInputFormat.class);
		//		TextInputFormat.addInputPath(job, new Path(args[1]));
		TextInputFormat.addInputPath(
				job,
				new Path(
						"file:///D:\\001 JavaWeb\\00 itheima\\04 就业班\\day53-hadoop day03\\离线阶段第三天课\\第四天与第五天的资料\\4、大数据离线第四天\\map端join\\map_join_iput"));

		// 第二步 自定义map逻辑
		job.setMapperClass(MapJoinMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		// 第三步 分区

		// 第四步 排序

		// 第五步 规约

		// 第六步 分组

		// 第七步 自定义 reduce逻辑

		// 第八步 输出

		job.setOutputFormatClass(TextOutputFormat.class);
		//		TextOutputFormat.setOutputPath(job, new Path(args[2]));
		TextOutputFormat.setOutputPath(
				job,
				new Path(
						"file:///D:\\001 JavaWeb\\00 itheima\\04 就业班\\day53-hadoop day03\\离线阶段第三天课\\第四天与第五天的资料\\4、大数据离线第四天\\map端join\\mapreduce_output_lyx"));

		boolean b = job.waitForCompletion(true);

		return b ? 0 : 1;
	}
}
