package cn.itcast.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author: Liyuxin wechat:13011800146. @Title: WordCountReducer @ProjectName hadoop-parent
 * @date: 2019/2/25 23:45
 * @description:
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
  /**
   * 重写reducer方法
   *
   * @param key k2
   * @param values v2
   * @param context 上下文对象
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  protected void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
    Integer i = new Integer(0);
    for (IntWritable value : values) {
      // 将每个单词出现的次数计数求和
      i += value.get();
    }

    //将我们计算的结果给写出去
    context.write(key,new IntWritable(i));
  }
}
