package cn.itcast.mr.demo04.reduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author: Liyuxin wechat:13011800146. @Title: ReduceMapper @ProjectName hadoop-parent
 * @date: 2019/2/27 20:17
 * @description:
 */
public class ReduceMapper extends Mapper<LongWritable, Text, Text, Text> {
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String[] split = value.toString().split(",");

    // 通过分片,获取数据来源于哪个文件
    FileSplit inputSplit = (FileSplit) context.getInputSplit();
    String name = inputSplit.getPath().getName();
    // 根据不同的文件,转换k2,v2
    if (name.contains("orders")) {
      // 订单文件
      Text text = new Text(split[2]);
      context.write(text, value);
    }
    if (name.contains("product")) {
      // 商品文件
      Text text = new Text(split[0]);
      context.write(text, value);
    }

  }
}
