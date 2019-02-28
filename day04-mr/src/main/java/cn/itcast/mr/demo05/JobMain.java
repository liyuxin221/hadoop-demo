package cn.itcast.mr.demo05;

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

public class JobMain extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int run = ToolRunner.run(new Configuration(), new JobMain(), args);
		System.exit(run);

	}

	@Override
	public int run(String[] strings) throws Exception {
		//注意pdts.txt文件是我们先上传到hdfs文件系统上面的
		//提前将hdfs://node01:8020/cache/pdts.txt的pdts.txt文件，缓存到mapreduce里面来
		//即向我们的分布式缓存当中添加文件
		DistributedCache.addCacheFile(new URI("hdfs://node-1:8020/cache/pdts.txt"), super.getConf());

		//第一步：获得Job对象
		Job job = Job.getInstance(super.getConf(), "mapperJoin_job");
		//要打包运行，必须添加这个
		job.setJarByClass(JobMain.class);

		//第二步：mapreduce的天龙八步
		job.setInputFormatClass(TextInputFormat.class);
		//TextInputFormat.addInputPath(job,new Path("C:\\Users\\mengq\\Desktop\\就业班\\课件\\视频\\day53_Hadoop\\第四天与第五天的资料\\4、大数据离线第四天\\map端join\\map_join_iput"));
		TextInputFormat.addInputPath(job, new Path("hdfs://node-1:8020/mapJoin_ord/orders.txt"));
		//2.2 自定义mapper逻辑将k1 v1 转换为 k2 v2 输出
		job.setMapperClass(JoinMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		//2.3 分区 相同key的value发送到同一个reduce里面去。key合并，value形成一个集合
		//2.4 排序
		//2.5 规约
		//2.6 【压缩】分组
		//2.7 自定义reduce 算法 不要了，因为在mapper里面，直接输出了我们想要的数据
		//2.8 输出
		job.setOutputFormatClass(TextOutputFormat.class);
		//TextOutputFormat.setOutputPath(job,new Path("C:\\Users\\mengq\\Desktop\\就业班\\课件\\视频\\day53_Hadoop\\第四天与第五天的资料\\4、大数据离线第四天\\map端join\\mapJoin_result"));
		TextOutputFormat.setOutputPath(job, new Path("hdfs://node-1:8020/mapJoin_result"));
		//第三步：提交job
		boolean b = job.waitForCompletion(true);

		return b ? 0 : 1;
	}
}
