import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.WritableComparable;

public class ClassTermPair implements WritableComparable<ClassTermPair>{
    private String className;
    private String term;

    public ClassTermPair() {
        // TODO Auto-generated constructor stub
    }

    public ClassTermPair(String className, String term)
    {
        this.className = className;
        this.term = term;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClassTermPair ctp = (ClassTermPair)o;

        return Objects.equals(className, ctp.className) && Objects.equals(term, ctp.term);
    }

    @Override
    public int hashCode() {
        return (className + term).hashCode();
    }

    @Override
    public void write(DataOutput out) throws IOException {  //序列化
        out.writeBytes(className + "\n");
        out.writeBytes(term + "\n");
    }
    @Override
    public void readFields(DataInput in) throws IOException {   //反序列化
        this.className = in.readLine();
        this.term = in.readLine();
    }
    @Override
    public int compareTo(ClassTermPair o) { //自定义比较函数
        int c1 = this.className.compareTo(o.className);
        if (c1 == 0) {
            return this.term.compareTo(o.term);
        } else {
            return c1;
        }
    }

    @Override
    public String toString() {
        return this.className + "\t" + this.term;
    }

    public String getClassName() {
        return className;
    }

    public void set(ClassTermPair o) {
        this.setClassName(o.className);
        this.setTerm(o.term);
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public void setTerm(String term) {
        this.term = term;
    }
}
