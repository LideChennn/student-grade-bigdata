package cn.edu.gdpu.lastwork.sive;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class  UnderGraduatePartitioner extends Partitioner<GradeBean, NullWritable> {
    @Override
    public int getPartition(GradeBean gradeBean, NullWritable nullWritable, int i) {
        String province = gradeBean.getProvince();
        int part = 0;
        switch (province) {
            case "广东":
                break;
            case "江苏":
                part = 1;
                break;
            case "浙江":
                part = 2;
                break;
        }
        return part;
    }
}
