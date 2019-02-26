package cn.itcast.mr.demo03.flowAdd;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author: Liyuxin wechat:13011800146.
 * @Title: FlowMapper
 * @ProjectName hadoop-parent
 * @date: 2019/2/26 23:29
 * @description:
 */
public class FlowMapper extends Mapper<LongWritable, Text,Text,FlowBean> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] splits = value.toString().split("\t");

		FlowBean flowBean = new FlowBean();
		flowBean.setUpFlow(Integer.parseInt(splits[6]));
		flowBean.setDownFlow(Integer.parseInt(splits[7]));
		flowBean.setUpCountFlow(Integer.parseInt(splits[8]));
		flowBean.setDownCountFlow(Integer.parseInt(splits[9]));

		context.write(new Text(splits[1]),flowBean);
	}
}
