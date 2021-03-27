package cc.chengheng.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SortBean implements WritableComparable<SortBean> {

    private String word;
    private int num;

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    @Override
    public String toString() {
        return word + num ;
    }

    /**
     * 实现比较器，指定排序的规则
     * 第一列(word)按照字典顺序进行排列
     * 第一列相同的时候，第二列(num)按照升序进行排列
     *
     * @param sortBean
     * @return
     */
    @Override
    public int compareTo(SortBean sortBean) {
        // 先对第一列排序
        int result = word.compareTo(sortBean.word);

        // 第一列相同的时候，按照第二列进行排列
        if (result == 0) {
            return num - sortBean.num;
        }
        return result;
    }

    /**
     * 实现序列化
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(word);
        out.writeInt(num);
    }

    /**
     * 实现反序列化
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        word = in.readUTF();
        num = in.readInt();
    }
}
