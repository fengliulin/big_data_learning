package cc.chengheng.marpreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 * KEYIN:       k1的类型
 * VALUEIN:     v1的类型
 * KEYOUT:      k2的类型
 * VALUEOUT:    v2的类型
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    /**
     * map方法就是将k1和v1转为k2和v2 <br>
     * 如何将k1和v1 转为 k2和v2
     * k1           v1
     * 0       hello,world,hadoop
     * 15      hdfs,hive,hello
     * -------------------------------
     * k2          v2
     * hello       1
     * world       1
     * hdfs        1
     * hadoop      1
     * hello       1
     *
     * @param key     k1 行偏移量
     * @param value   v1 每一行的文本数据
     * @param context 表示上下文对象
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text text = new Text();
        LongWritable longWritable = new LongWritable();

        // 1、将一行的文本数据进行拆分
        String[] split = value.toString().split(",");

        // 2、遍历数组，组装 k2 和 v2
        for (String word : split) {
            // 3、将k2 和 v2 写入上下文
            text.set(word);
            longWritable.set(1);
            context.write(text, longWritable);
        }
    }
}
