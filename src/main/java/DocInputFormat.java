import java.io.*;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * A {@link FileInputFormat} implementation that passes the class name and term as the key
 * and 1 as the value. Generates one map task per file
 */
public class DocInputFormat extends FileInputFormat<Text, Text> {

    @Override
    public boolean isSplitable(JobContext context, Path p) {
        return false;
    }

    @Override
    public DocRecordReader createRecordReader(
            InputSplit arg0, TaskAttemptContext arg1) throws IOException,
            InterruptedException {
        return new DocRecordReader();
    }

    public static class DocRecordReader extends
            RecordReader<Text, Text> {

//        private ClassTermPair key = new ClassTermPair();
//        private IntWritable value = new IntWritable(1);
        private Text key = new Text();
        private Text value = new Text();
        private boolean read = false;
        private FSDataInputStream in;

        @Override
        public void close() throws IOException {
            if (in != null) {
                in.close();
            }
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException,
                InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return read ? 1 : 0;
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context)
                throws IOException, InterruptedException {
            read = false;

            FileSplit fSplit = (FileSplit) split;

            if (fSplit.getLength() > Integer.MAX_VALUE) {
                throw new IOException("Size of file is larger than max integer");
            }

            FileSystem fs = FileSystem.get(context.getConfiguration());
            String fileName = fs.makeQualified(fSplit.getPath()).getName();
//            String className = fileName.substring(0, fileName.indexOf('_'));
            this.key.set(fileName);

            this.in = fs.open(fSplit.getPath());
            byte[] bytes = new byte[(int)fSplit.getLength()];
            in.readFully(bytes);
            this.value.set(bytes);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (!read) {
                read = true;
                return true;
            } else {
                return false;
            }
        }
    }
}

