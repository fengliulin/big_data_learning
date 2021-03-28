package cc.chengheng.coustom_inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class MyRecordReader extends RecordReader<NullWritable, BytesWritable> {

    private Configuration configuration;
    private FileSplit fileSplit;
    private boolean processed = false;
    private BytesWritable bytesWritable;
    private FileSystem fileSystem;
    private FSDataInputStream inputStream;

    /**
     * 进行初始化工作
     *
     * @param split
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        // 获取文件的切片
        fileSplit = (FileSplit) split;

        // 获取Configuration对象
        configuration = context.getConfiguration();
    }

    /**
     * 获取k1和v1
     * k1: NullWritable
     * V1: BytesWritable
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!processed) {
            // 1、获取源文件的字节输入流
            // 获取源文件的文件系统
            fileSystem = FileSystem.get(configuration);
            // 获取文件字节输入流
            inputStream = fileSystem.open(fileSplit.getPath());

            // 2、读取源文件数据到普通的字节数组（byte[]）
            byte[] bytes = new byte[(int) fileSplit.getLength()];
            IOUtils.readFully(inputStream, bytes, 0, (int) fileSplit.getLength());

            // 3、将字节数组中的数据封装到BytesWritable, 得到v1
            bytesWritable = new BytesWritable();
            bytesWritable.set(bytes, 0, (int) fileSplit.getLength());

            processed = true;
            return true;
        }

        return false;
    }

    /**
     * 返回k1
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    /**
     * 返回v1
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return bytesWritable;
    }

    /**
     * 获取文件读取进度
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    /**
     * 进行资源释放
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        inputStream.close();
        fileSystem.close();
    }
}
