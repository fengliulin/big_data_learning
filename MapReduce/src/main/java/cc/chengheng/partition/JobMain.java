package cc.chengheng.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        // 1、创建job任务对象
        Job job = Job.getInstance(getConf(), "partition_mapreduce");

        job.setJarByClass(cc.chengheng.marpreduce.JobMain.class); /* 如果打包运行出错，则需要加这个配置 */

        // 2、对job任务进行配置（八个步骤）

        // 第一步：设置输入类和输入的路径
        job.setInputFormatClass(TextInputFormat.class);
//        TextInputFormat.addInputPath(job, new Path("hdfs://192.168.1.100:8020/input"));
        TextInputFormat.addInputPath(job, new Path("file:///Users/fengliulin/Documents/input")); // 本地运行

        // 第二步：设置Mapper类的数据类型(k2和v2)
        job.setMapperClass(PartitionMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 第三步：指定分区类
        job.setPartitionerClass(MyPartitioner.class);

        // 四、五、六步  排序 规约 分组    采用默认方式

        // 第七步：指定Reducer类和数据类型(k3和v3)
        job.setReducerClass(PartitionerReducer.class);
        job.setOutputValueClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(2); // 设置ReduceTask的个数

        // 第八步：指定输出类和输出路径
        job.setOutputFormatClass(TextOutputFormat.class);
//        TextOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.100:8020/out/partition_out"));
        TextOutputFormat.setOutputPath(job, new Path("file:///Users/fengliulin/Documents/out/partition_out")); //本地运行

        // 3、等待任务结束
        boolean b = job.waitForCompletion(true);
        return b ? 1 : 0;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        // 启动job任务
        int run = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(run);
    }
}
