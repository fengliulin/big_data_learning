package cc.chengheng.coustom_outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyOutputFormat extends FileOutputFormat<Text, NullWritable> {
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        // 1、获取目标文件的输出流(两个)
        FileSystem fileSystem = FileSystem.get(job.getConfiguration());
        FSDataOutputStream goodCommentsOutputStream = fileSystem.create(new Path("file:///Users/fengliulin/Documents/good_comments/good_comments.txt"));
        FSDataOutputStream badCommentsOutputStream = fileSystem.create(new Path("file:///Users/fengliulin/Documents/bad_comments/bad_comments.txt"));

        // 2、将输出流传给MyRecordWriter
        return new MyRecordWriter(goodCommentsOutputStream, badCommentsOutputStream);
    }
}
