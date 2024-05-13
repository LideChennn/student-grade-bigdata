package cn.edu.gdpu.lastwork.four;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 输入 ：某一分区的一部分数据
 * 语文 (分数 总数)
 *
 *  环形缓冲区溢出数据后，对溢出的数据进行分区排序，然后进入combiner预聚合
 *  溢出的数据并不是全部，combiner预聚合并不是所有的数据，聚合了一部分
 * 设置SumAndCountBean的原因是：reducer不知道combiner传了是合并了多少条数据
 *
 * combiner运行完后，进行归并排序，对所有同一个分区进行总排序，进入reducer的才是全部数据
 */
public class GradeCombiner extends Reducer<Text, SumAndCountBean, Text, SumAndCountBean> {
    private final SumAndCountBean outValue = new SumAndCountBean();

    @Override
    protected void reduce(Text key, Iterable<SumAndCountBean> values, Reducer<Text, SumAndCountBean, Text, SumAndCountBean>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        int count = 0;
        ////语文  --- {sum = 70 ， count = 1}, {sum = 60, count = 1}
        for (SumAndCountBean value : values) {
            sum += value.getSum();
            count += value.getCount();
        }

        outValue.setCount(count);
        outValue.setSum(sum);
        context.write(key, outValue);
    }
}
