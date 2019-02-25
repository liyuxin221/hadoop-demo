package cn.itcast.hadoop_demo;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author: Liyuxin wechat:13011800146. @Title: HdfsStudy @ProjectName hadoop-parent
 * @date: 2019/2/25 21:08
 * @description:
 */
public class HdfsStudy {

  /** 合并本地小文件,变成一个大文件,放到hdfs上. */
  @Test
  public void mergeSmallFile() throws IOException, URISyntaxException, InterruptedException {
    // 第一步:读取本地文件系统

    // 上传到hdfs的一个输出流里面去

    // 获取hdfs的输出流
    FileSystem fileSystem =
        FileSystem.get(new URI("hdfs://node-1:8020"), new Configuration(), "root");

    // 打开hdfs上面的一个输出流
    FSDataOutputStream fos = fileSystem.create(new Path("hdfs://node-1:8020/bigfile.xml"));

    // 获取本地文件系统
    LocalFileSystem local = FileSystem.getLocal(new Configuration());

    // 获取某个路径下面所有文件的filestatus
    FileStatus[] fileStatuses =
        local.listStatus(
            new Path(
                "file:///D:\\001 JavaWeb\\00 itheima\\04 就业班\\day53-hadoop day03\\离线阶段第三天课\\第三天的资料与视频\\3、大数据离线第三天\\上传小文件合并"));
    for (FileStatus fileStatus : fileStatuses) {
      // 获取本地每一个文件的路径
      Path path = fileStatus.getPath();
      // 获取每一个本地文件的输入流
      FSDataInputStream fis = local.open(path);

      // 将输入流拷贝到输出流
      IOUtils.copy(fis, fos);
      IOUtils.closeQuietly(fis);
    }

    IOUtils.closeQuietly(fos);
    // 关闭文件系统
    local.close();
    fileSystem.close();
  }
}
