package cn.itcast.mr.demo04.reduceJoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author: Liyuxin wechat:13011800146. @Title: ReduceReducer @ProjectName hadoop-parent
 * @date: 2019/2/27 20:18
 * @description:
 */
public class ReduceReducer extends Reducer<Text, Text, Text, Text> {

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    StringBuffer infos = new StringBuffer();
    for (Text value : values) {
      infos.append(value);
      infos.append("\t");
    }

    context.write(key, new Text(infos.toString()));
  }
}
