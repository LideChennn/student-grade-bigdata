package cn.edu.gdpu.lastwork.two;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import java.io.IOException;

/**
 *  直接把输入当输出
 */
public class GradeSumDecReducer extends Reducer<GradeBean, NullWritable, GradeBean, NullWritable> {

    private MultipleOutputs<GradeBean, NullWritable> multipleOutputs;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(GradeBean key, Iterable<NullWritable> values, Reducer<GradeBean, NullWritable, GradeBean, NullWritable>.Context context) throws IOException, InterruptedException {
        multipleOutputs.write(key, NullWritable.get(),  key.getProvince());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }

}
