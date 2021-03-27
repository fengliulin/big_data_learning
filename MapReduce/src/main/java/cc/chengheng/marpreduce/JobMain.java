package cc.chengheng.marpreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class JobMain extends Configured implements Tool {

    /**
     * 该方法用于指定一个job任务
     *
     * @param args
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        // 1、创建一个job任务对象
        Job job = Job.getInstance(getConf(), "wordcount");

        job.setJarByClass(JobMain.class); /* 如果打包运行出错，则需要加这个配置 */

        // 2、配置job任务对象(八个步骤)

        // 第一步：指定文件的读取方式和读取路径
        job.setInputFormatClass(TextInputFormat.class);
//        TextInputFormat.addInputPath(job, new Path("hdfs://192.168.1.100:8020/wordcount"));
        TextInputFormat.addInputPath(job, new Path("file:///Users/fengliulin/Documents/MapReduce"));

        // 第二步：指定map阶段的处理方式
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class); // 设置Map阶段的 k2 类型
        job.setMapOutputValueClass(LongWritable.class); // 设置Map阶段的 v2 类型

        // 第三、四、五、六 分区 排序 规约 分组 采用 默认方式

        // 第七步：指定Reduce阶段的处理方式和数据类型
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class); // 设置k3的类型
        job.setOutputValueClass(LongWritable.class); // 设置v3的类型

        // 第八步：设置输出类型
        job.setOutputFormatClass(TextOutputFormat.class);
        getConf().set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        // 设置输出的路径
//        Path path = new Path("hdfs://192.168.1.100:8020/wordcount_out");
        Path path = new Path("file:///Users/fengliulin/Documents/wordcount_out");
        TextOutputFormat.setOutputPath(job, path);

        // 判断目录是否存在，存在就删除
        FileSystem fileSystem = FileSystem.get(new URI("file:///Users/fengliulin/Documents/wordcount_out"), new Configuration());
        boolean exists = fileSystem.exists(path);
        if (exists) {
            // 如果目录存在就删除
            fileSystem.delete(path, true);
        }

        // 等待任务结束
        boolean b = job.waitForCompletion(true);


        return b ? 1 : 0;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        // 启动job任务
        ToolRunner.run(configuration, new JobMain(), args);

        int run = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(run);
    }
}
