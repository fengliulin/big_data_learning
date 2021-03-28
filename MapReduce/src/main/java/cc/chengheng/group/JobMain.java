package cc.chengheng.group;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class JobMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        // 1、获取job对象
        Job job = Job.getInstance(getConf(), "myGroup_job");

        job.setJarByClass(JobMain.class); /* 如果打包运行出错，则需要加这个配置 */

        // 2、设置job任务
        // 第一步：设置输入类和输入路径
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("file:///Users/fengliulin/Documents/myGroup_input"));

        // 第二步：设置Mapper类和数据类型
        job.setMapperClass(GroupMapper.class);
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(Text.class);

        // 第三、分区
        job.setPartitionerClass(OrderPartition.class); // 不设置job.setNumReduceTasks(2)，这个没什么用

        // 四、排序
        // 五、规约

        // 六 分组
        job.setGroupingComparatorClass(OrderGroupComparator.class);

        // 第七步：设置Reduce类和数据类型
        job.setReducerClass(GroupReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(2); // 设置这个才能断点到OrderPartition

        // 第八步：设置输出类和输出的路径
        job.setOutputFormatClass(TextOutputFormat.class);
        Path path = new Path("file:///Users/fengliulin/Documents/myGroup_out");
        TextOutputFormat.setOutputPath(job, path);

        // 判断目录是否存在，存在就删除
        directoryExists(path);

        // 等待任务结束
        boolean b = job.waitForCompletion(true);

        return b ? 1 : 0;
    }

    /**
     * 判断目录是否存在，存在就删除
     *
     * @param path
     * @throws IOException
     * @throws URISyntaxException
     */
    private void directoryExists(Path path) throws IOException, URISyntaxException {
        FileSystem fileSystem = FileSystem.get(new URI("file:///Users/fengliulin/Documents/myGroup_out"), new Configuration());
        boolean exists = fileSystem.exists(path);
        if (exists) {
            // 如果目录存在就删除
            fileSystem.delete(path, true);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        // 启动job任务
        ToolRunner.run(configuration, new JobMain(), args);

        int run = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(run);
    }
}
