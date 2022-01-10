import java.io.*;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
public class TermInputFormat extends FileInputFormat<ClassTermPair, IntWritable> {

    @Override
    public boolean isSplitable(JobContext context, Path p) {
        return false;
    }

    @Override
    public TermRecordReader createRecordReader(
            InputSplit arg0, TaskAttemptContext arg1) throws IOException,
            InterruptedException {
        return new TermRecordReader();
    }

    public static class TermRecordReader extends
            RecordReader<ClassTermPair, IntWritable> {

        private ClassTermPair key = new ClassTermPair();
        private IntWritable value = new IntWritable(1);
        private boolean read = false;
        private BufferedReader br = null;

        @Override
        public void close() throws IOException {
            // nothing to do here
        }

        @Override
        public ClassTermPair getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public IntWritable getCurrentValue() throws IOException,
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
            String className = fileName.substring(0, fileName.indexOf('_'));
            this.key.setClassName(className);

            FSDataInputStream in = fs.open(fSplit.getPath());
            br = new BufferedReader(new InputStreamReader(in));
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            String term;
            // 逐行读单词
            if ((term = this.br.readLine()) != null) {
                this.key.setTerm(term);
                return true;
            } else {
                read = true;
                return false;
            }
        }
    }
}

