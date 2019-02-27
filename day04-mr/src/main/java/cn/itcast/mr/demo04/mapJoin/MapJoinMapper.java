package cn.itcast.mr.demo04.mapJoin;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @author: Liyuxin wechat:13011800146. @Title: MapJoinMapper @ProjectName hadoop-parent
 * @date: 2019/2/27 21:43
 * @description:
 */
public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	private Map<String, String> map;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		map = new HashMap<String, String>();

		Configuration configuration = context.getConfiguration();

		URI[] uris = DistributedCache.getCacheFiles(configuration);

		URI cacheFile = uris[0];

		FileSystem fileSystem = FileSystem.get(cacheFile, configuration);

		FSDataInputStream fis = fileSystem.open(new Path(cacheFile));

		BufferedReader reader = new BufferedReader(new InputStreamReader(fis));

		/**
		 p0001,xiaomi,1000,2
		 p0002,appale,1000,3
		 p0003,samsung,1000,4
		 */

		String line = null;
		while ((line = reader.readLine()) != null) {
			String[] split = line.split(",");
			map.put(split[0], line);
		}

		// 关闭资源
		reader.close();
		IOUtils.closeQuietly(fis);
		fileSystem.close();
	}

	/**
	 * 将商品表数据从分布式缓存中读取出来,读取订单表的每一行数据,两者进行拼接
	 *
	 * @param key
	 * @param value
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] split = value.toString().split(",");

		String produceId = split[2];

		String productInfo = map.get(produceId);

		context.write(new Text(value.toString() + "\t" + productInfo), NullWritable.get());

	}
}
