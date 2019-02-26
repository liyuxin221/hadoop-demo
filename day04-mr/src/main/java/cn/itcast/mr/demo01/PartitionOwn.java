package cn.itcast.mr.demo01;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
/**
 * @author: Liyuxin wechat:13011800146. @Title: Partitioner @ProjectName hadoop-parent
 * @date: 2019/2/26 19:31
 * @description:
 */
public class PartitionOwn extends Partitioner<Text, NullWritable> {

  /**
   * 覆写getPartition方法,决定数据的分组
   *
   * @param text k2,一行数据
   * @param nullWritable v2,空
   * @param i reduceTask的数目
   * @return
   */
  @Override
  public int getPartition(Text text, NullWritable nullWritable, int i) {
    String[] splits = text.toString().split("\t");
    String split = splits[5];

    //根据return值来决定找哪个reduceTask
    if (split != null && split!="") {
      if (Integer.parseInt(split) >= 15) {
        return 1;
      } else {
        return 0;
      }
    }

    return 0;
  }
}
