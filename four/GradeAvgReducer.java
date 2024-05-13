package cn.edu.gdpu.lastwork.four;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class GradeAvgReducer extends Reducer<Text, SumAndCountBean, Text, DoubleWritable> {
    private final DoubleWritable outValue = new DoubleWritable();
    @Override
    protected void reduce(Text key, Iterable<SumAndCountBean> values, Reducer<Text, SumAndCountBean, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        int count = 0;//统计有多少条记录
        //语文  --- {sum = 470 ， count = 10}, {sum = 400, count = 9}
        for (SumAndCountBean value : values) {
            sum += value.getSum();
            count += value.getCount();
        }
        double average = sum /( 1.0 * count);

        // 四舍五入到两位小数
        double roundedAverage = Math.round(average * 100.0) / 100.0;
        outValue.set(roundedAverage);
        context.write(key, outValue);
    }
}
