package cs435.hbaseImport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

/**
 * Created by Alec on 4/15/2017.
 * custom record reader
 */
public class WikiHBaseRecordReader extends RecordReader<LongWritable, Text> {
	private long start;
	private long pos;
	private long end;
	private LineReader in;
	private int maxLineLength;
	private LongWritable key = new LongWritable();
	private Text value = new Text();

	@Override
	public boolean nextKeyValue() throws IOException {

		// Current offset is the key
		key.set(pos);
		value.set("");
		int size = 0;


		while (true) {

			Text temp = new Text();
			int maxBytes = Math.max((int) Math.min(Integer.MAX_VALUE, end - pos), maxLineLength);
			size = in.readLine(temp, maxLineLength, maxBytes);

			if (size == 0) { // end of split
				break;
			}

			if (temp.toString().trim().equals("")) { // end of record
				break;
			} else {
				value.set(value.toString() + temp.toString() + "&&");
			}

			pos += size;

		}

		//System.out.println(value.toString());
		//System.out.println("==============================================================================");
		if (size == 0) {// end of Split
			key = null;
			value = null;
			return false;
		} else {
			return true;
		}

	}

	// all same as default
	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {

		FileSplit split = (FileSplit) genericSplit;

		Configuration job = context.getConfiguration();
		this.maxLineLength = job.getInt(
				"mapred.linerecordreader.maxlength",
				Integer.MAX_VALUE);

		start = split.getStart();
		end = start + split.getLength();


		final Path file = split.getPath();
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(split.getPath());

		boolean skipFirstLine = false;
		if (start != 0) {
			skipFirstLine = true;
			--start;
			fileIn.seek(start);
		}

		in = new LineReader(fileIn, job);

		if (skipFirstLine) {
			Text dummy = new Text();
			start += in.readLine(dummy, 0, (int) Math.min((long) Integer.MAX_VALUE, end - start));
		}


		this.pos = start;

	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}

	@Override
	public void close() throws IOException {
		if (in != null) {
			in.close();
		}
	}
}
