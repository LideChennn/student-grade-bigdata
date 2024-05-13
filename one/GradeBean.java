package cn.edu.gdpu.lastwork.one;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GradeBean implements WritableComparable<GradeBean> {
    private String examId;
    private String name;
    private int totalScore;
    private String province;

    @Override
    public int compareTo(GradeBean o) {
        //按成绩降序排序，成绩相等则按考试id排序
        int compare = Integer.compare(o.totalScore, this.totalScore);
        return compare == 0 ? this.examId.compareTo(o.examId) : compare;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(examId);
        dataOutput.writeUTF(name);
        dataOutput.writeInt(totalScore);
        dataOutput.writeUTF(province);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.examId = dataInput.readUTF();
        this.name = dataInput.readUTF();
        this.totalScore = dataInput.readInt();
        this.province = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return examId + "\t" + name + "\t" + totalScore + "\t" + province;
    }

    public String getExamId() {
        return examId;
    }

    public void setExamId(String examId) {
        this.examId = examId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getTotalScore() {
        return totalScore;
    }

    public void setTotalScore(int totalScore) {
        this.totalScore = totalScore;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }
}
