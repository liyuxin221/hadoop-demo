package cn.itcast.mr.demo03.flowPartition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 规约
 *
 * @author: Liyuxin wechat:13011800146. @Title: FlowCombiner @ProjectName hadoop-parent
 * @date: 2019/2/27 17:25
 * @description:
 */
public class FlowCombiner extends Reducer<Text, FlowBean, Text, FlowBean> {
  @Override
  protected void reduce(Text key, Iterable<FlowBean> values, Context context)
      throws IOException, InterruptedException {
    for (FlowBean value : values) {
      // 规约
      context.write(key, value);
    }
  }
}
