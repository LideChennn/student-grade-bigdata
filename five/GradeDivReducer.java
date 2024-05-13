package cn.edu.gdpu.lastwork.five;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 本科线60%
 * 输入
 * 省份  总分（一个学生）
 *
 * 输出
 * 省份 本科线
 */
public class GradeDivReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final IntWritable outValue = new IntWritable();
    private double undergraduateRate;

    @Override
    protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        //配置文件中读取本科率
        Configuration conf = context.getConfiguration();
        undergraduateRate = Double.parseDouble(conf.get("undergraduateRate"));
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        //key省份，values：所有的分数
        List<Integer> list = new ArrayList<>();
        int count = 0;
        for (IntWritable value : values) {
            list.add(value.get());
            count++;
        }
        //排序
        list.sort((o1, o2) -> Integer.compare(o2, o1));
        // 计算总人数的60%
        int thresholdIndex = (int) Math.ceil(count * undergraduateRate);
        //本科线
        int undergraduate = list.get(thresholdIndex) - 1;

        outValue.set(undergraduate);
        context.write(key, outValue);
    }
}