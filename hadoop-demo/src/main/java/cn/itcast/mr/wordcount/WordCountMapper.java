package cn.itcast.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 继承mapper类,有四个泛型
 *
 * @author: Liyuxin wechat:13011800146. @Title: WordCountMapper @ProjectName hadoop-parent
 * @date: 2019/2/25 23:31
 * @description:
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  /**
   * 重写map方法
   *
   * @param key K1
   * @param value V1
   * @param context 上下文对象
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    // 按照逗号切割,得到每一个单词
    String[] words = value.toString().split(",");
    for (String word : words) {
      // 循环遍历每一个单词,将数据通过context对象发送
      context.write(new Text(word), new IntWritable(1));
    }
  }
}
