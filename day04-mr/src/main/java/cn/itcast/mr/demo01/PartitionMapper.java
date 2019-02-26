package cn.itcast.mr.demo01;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * k1 v1
 * k2 v2 一行文本,null
 * @author: Liyuxin wechat:13011800146.
 * @Title: PartitionMapper
 * @ProjectName hadoop-parent
 * @date: 2019/2/26 19:30
 * @description:
 */
public class PartitionMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		context.write(value,NullWritable.get());
	}
}
