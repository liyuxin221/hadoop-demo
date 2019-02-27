package cn.itcast.mr.demo04.reduceJoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author: Liyuxin wechat:13011800146. @Title: ReduceCombiner @ProjectName hadoop-parent
 * @date: 2019/2/27 20:17
 * @description:
 */
public class ReduceCombiner extends Reducer<Text, Text, Text, Text> {

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    for (Text value : values) {
      context.write(key, value);
    }
  }
}
