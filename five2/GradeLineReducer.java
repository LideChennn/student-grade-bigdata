package cn.edu.gdpu.lastwork.five2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 输入
 * 广东  455 411
 * ...
 *
 * 江苏 582 412
 * ...
 *
 * 浙江 562 452
 *
 * 输出
 * 广东 本科线
 * ...
 */
public class GradeLineReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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
        List<Integer> list = new ArrayList<>();
        int count = 0;
        for (IntWritable value : values) {
            count++;
            System.out.print(key + ": "  + value.get() + " ");
            list.add(value.get());
        }
        System.out.println();

        // 计算总人数的60%
        int thresholdIndex = (int) Math.ceil(count * undergraduateRate);

        //本科线 排名第60%的人的分数就是本科线
        int undergraduate = list.get(thresholdIndex) - 1;

        outValue.set(undergraduate);
        context.write(key, outValue);
    }
}
