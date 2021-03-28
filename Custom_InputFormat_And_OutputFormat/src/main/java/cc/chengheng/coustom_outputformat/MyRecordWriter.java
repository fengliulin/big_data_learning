package cc.chengheng.coustom_outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class MyRecordWriter extends RecordWriter<Text, NullWritable> {

    FSDataOutputStream goodCommentsOutputStream;
    FSDataOutputStream badCommentsOutputStream;

    public MyRecordWriter() {
    }

    public MyRecordWriter(FSDataOutputStream goodCommentsOutputStream, FSDataOutputStream badCommentsOutputStream) {
        this.goodCommentsOutputStream = goodCommentsOutputStream;
        this.badCommentsOutputStream = badCommentsOutputStream;
    }

    /**
     *
     * @param key   行文本内容
     * @param value
     * @throws InterruptedException
     */
    @Override
    public void write(Text key, NullWritable value) throws IOException {
        // 1、从行文本数据中获取第9个字段
        String[] split = key.toString().split("\t");
        String numStr = split[7];

        // 2、根据字段的值，判断评论的类型，然后将对应的数据写入不同的文件夹文件中
        if (Integer.parseInt(numStr) <= 1) {
            // 好评或中评
            goodCommentsOutputStream.write(key.toString().getBytes(StandardCharsets.UTF_8));
            goodCommentsOutputStream.write("\n".getBytes(StandardCharsets.UTF_8));
        } else {
            // 差评
            badCommentsOutputStream.write(key.toString().getBytes(StandardCharsets.UTF_8));
            badCommentsOutputStream.write("\n".getBytes(StandardCharsets.UTF_8));
        }
    }

    @Override
    public void close(TaskAttemptContext context) {
        IOUtils.closeStream(goodCommentsOutputStream);
        IOUtils.closeStream(badCommentsOutputStream);
    }
}
