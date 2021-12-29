import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * A {@link FileInputFormat} implementation that passes the file name as the key
 * and 1 as the value. Generates one map task per file
 */
public class FileNameInputFormat extends FileInputFormat<Text, IntWritable> {

    @Override
    public boolean isSplitable(JobContext context, Path p) {
        return false;
    }

    @Override
    public RecordReader<Text, IntWritable> createRecordReader(
            InputSplit arg0, TaskAttemptContext arg1) throws IOException,
            InterruptedException {
        return new FileNameRecordReader();
    }

    public static class FileNameRecordReader extends
            RecordReader<Text, IntWritable> {

        private Text key = new Text();
        private IntWritable value = new IntWritable(1);
        private boolean read = false;
        private FileSystem fs = null;
        private FileSplit fSplit = null;

        @Override
        public void close() throws IOException {
            // nothing to do here
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
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

            fSplit = (FileSplit) split;

            if (fSplit.getLength() > Integer.MAX_VALUE) {
                throw new IOException("Size of file is larger than max integer");
            }

            fs = FileSystem.get(context.getConfiguration());
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (!read) {

                // set the key to the fully qualified path
                String fileName = fs.makeQualified(fSplit.getPath()).getName();
                String className = fileName.substring(0, fileName.indexOf('_'));
                key.set(className);

                read = true;
                return true;
            } else {
                return false;
            }
        }
    }
}

