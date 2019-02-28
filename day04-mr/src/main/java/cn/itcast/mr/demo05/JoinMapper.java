package cn.itcast.mr.demo05;

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

public class JoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	//key 是商品id
	//value 是商品信息
	Map<String, String> productMap = new HashMap<String, String>();

	//初始化的时候，执行一次

	/**
	 * @param context 上下文对象，里面有configuration
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		//从上下文对象里面获得configuration
		Configuration configuration = context.getConfiguration();
		URI[] cacheFiles = DistributedCache.getCacheFiles(configuration);//获取多个缓存文件
		URI cacheFile = cacheFiles[0];//获得我们的缓存文件pdts.txt
		FileSystem fileSystem = FileSystem.get(cacheFile, configuration);
		//获得一个输入流
		FSDataInputStream fsDataInputStream = fileSystem.open(new Path(cacheFile));
		//获得文件的缓存流
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream));
		String line = null;

		while ((line = bufferedReader.readLine()) != null) {
			//将读到的数据，存放到map里面去，key 是商品id,value是 一行数据
			String[] split = line.split(",");
			String pid = split[0]; //截取的到商品id
			productMap.put(pid, line);
		}
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String order = value.toString();
		String[] split = order.split(",");
		String pid = split[2];
		//商品表的数据和 订单表的数据 拼接
		if (productMap.containsKey(pid)) {
			String order_product = pid + "\t" + order + "\t" + productMap.get(pid);
			context.write(new Text(order_product), NullWritable.get());
		} else {
			context.write(value, NullWritable.get());
		}
	}
}
