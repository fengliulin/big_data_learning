package cc.chengheng.casus.mapjoin;

import cc.chengheng.casus.flow3.FlowCountPartition;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import javax.lang.model.SourceVersion;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Set;

public class JobMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        // 1、获取job对象
        Job job = Job.getInstance(getConf(), "map_join_job");

        job.setJarByClass(JobMain.class); /* 如果打包运行出错，则需要加这个配置 */

        // 2、设置job对象(将小表放在分布式缓存中)
//        DistributedCache.addCacheFile("hdfs://192.168.1.100:8020/cache_file/product.txt"); 2.0之前，老的不用了
        // 将小表放在分布式缓存中
        job.addCacheFile(new URI("hdfs://192.168.1.100:8020/cache_file/product.txt"));

        // 第一步：设置输入类和输入的路径
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("file:///Users/fengliulin/Documents/map_join_input"));

        // 第二步：设置Mapper类和数据类型
        job.setMapperClass(MapJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 第三 分区
        // 第四 排序
        // 五、规约
        // 六 分组

        // 第七步：没有

        //region 第八步：设置输出类和输出路径
        job.setOutputFormatClass(TextOutputFormat.class);
        getConf().set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        // 设置输出的路径
        Path path = new Path("file:///Users/fengliulin/Documents/map_join_out");
        TextOutputFormat.setOutputPath(job, path);

        // 判断目录是否存在，存在就删除
        FileSystem fileSystem = FileSystem.get(new URI("file:///Users/fengliulin/Documents/map_join_out"), new Configuration());
        boolean exists = fileSystem.exists(path);
        if (exists) {
            // 如果目录存在就删除
            fileSystem.delete(path, true);
        }
        //endregion

        // 等待任务结束
        boolean b = job.waitForCompletion(true);

        return b ? 1 : 0;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        System.out.println("3233");
        // 启动job任务
        ToolRunner.run(configuration, new JobMain(), args);

        int run = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(run);
    }
}
