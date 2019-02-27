package cn.itcast.mr.demo03.flowPartition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 第二步: 自定义map逻辑,接收k1 v1 转换成 k2 v2
 *
 * @author: Liyuxin wechat:13011800146. @Title: FlowMapper @ProjectName hadoop-parent
 * @date: 2019/2/27 17:24
 * @description:
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
  private FlowBean flowBean = new FlowBean();
  private Text phonenumText;

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String[] split = value.toString().split("\t");

    flowBean.setUpFlow(Integer.parseInt(split[0]));
    flowBean.setDownFlow(Integer.parseInt(split[1]));
    flowBean.setUpCountFlow(Integer.parseInt(split[2]));
    flowBean.setDownCountFlow(Integer.parseInt(split[3]));

    phonenumText = new Text(split[4]);

    context.write(phonenumText,flowBean);
  }
}
