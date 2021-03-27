package cc.chengheng.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobMain extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        // 1、创建job对象
        Job job = Job.getInstance(getConf(), "mapreduce_sort");

        job.setJarByClass(cc.chengheng.marpreduce.JobMain.class); /* 如果打包运行出错，则需要加这个配置 */

        // 2、配置job任务（八个步骤）

        // 第一步：设置输入类和输入的路径
        job.setInputFormatClass(TextInputFormat.class);
//        TextInputFormat.addInputPath(job, new Path("hdfs://192.168.1.100:8020/sort_input"));
        TextInputFormat.addInputPath(job, new Path("file:///Users/fengliulin/Documents/sort_input")); // 本地运行


        // 第二步：设置Mapper类和数据类型
        job.setMapperClass(SortMapper.class);
        job.setMapOutputKeyClass(SortBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 第三、四、五、六 分区 排序(只要写了接口的compareTo，自动就排序了) 规约 分组

        // 第七步：设置和值Reducer类和类型
        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(SortBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 第八步：设置输出类和输出的路径
        job.setOutputFormatClass(TextOutputFormat.class);
//        TextOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.100:8020/out/sort_input"));
        TextOutputFormat.setOutputPath(job, new Path("file:///Users/fengliulin/Documents/out")); //本地运行


        // 3、等待任务的结束
        boolean b = job.waitForCompletion(true);

        return b ? 1 : 0;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(run);
    }
}
