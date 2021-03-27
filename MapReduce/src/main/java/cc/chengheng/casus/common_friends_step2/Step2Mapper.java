package cc.chengheng.casus.common_friends_step2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class Step2Mapper extends Mapper<LongWritable, Text, Text, Text> {
    /**
     * I-K-B-G-F-H-O-C-D-	A
     * A-F-C-J-E-	B
     * E-A-H-B-F-G-K-	C
     * K-E-C-L-A-F-H-G-	D
     * F-M-L-H-G-D-C-B-A-	E
     * G-D-A-M-L-	F
     * M-	G
     * O-	H
     * C-O-	I
     * O-	J
     * B-	K
     * D-E-	L
     * E-F-	M
     * J-I-H-A-F-	O
     * ------------------
     * k1           v2
     * 0        A-F-C-J-E-  B
     *-------上面数据，编程下面的格式----------
     * k2           v2
     * A-C          B
     * A-E          B
     * A-F          B
     * C-E          B
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1、拆分行文本数据，结果第二部分可以得到v2
        String[] split = value.toString().split("\t");
        String friendStr = split[1];

        // 2、继续以"-"为分隔符拆分行文本数据第一部分，得到数组
        String[] userArray = split[0].split("-");

        // 3、对数组做一个排序
        Arrays.sort(userArray);

        // 4、对数组中的元素进行两两合并，得到k2
        // A-C-E-排序之后A C E
        // 两两组合， ac ae ce
        /*
         * A     C     E
         *    A     C     E
         */
        for (int i = 0; i < userArray.length - 1; i++) {
            for (int j = i + 1; j < userArray.length; j++) {
                // 5、将k2和v2写入上下文中
                String k2 = userArray[i] + "-" + userArray[j];
                context.write(new Text(k2), new Text(friendStr)); // 重复的k，会把v放入Iterable<Text> values中
            }
        }
    }
}
