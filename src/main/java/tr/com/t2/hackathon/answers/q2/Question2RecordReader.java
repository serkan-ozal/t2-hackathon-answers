/*
 * Copyright (c) 2014, "SkyKeeper Team". All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY 
 * KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A
 * PARTICULAR PURPOSE.
 */

package tr.com.t2.hackathon.answers.q2;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

/**
 * @author Serkan OZAL
 */
public class Question2RecordReader extends RecordReader<LongWritable, Question2Data> {
	
	private static final Logger logger = Logger.getLogger(Question2RecordReader.class); 
	
	private long start;
	private long pos;
	private long end;
	private Question2DataReader reader;
	private FSDataInputStream fileIn;
	private Seekable filePosition;
	private LongWritable key;
	private Question2Data value;
	private int dataSize;
	private long fileSize;
	
	public Question2RecordReader() {
		
	}

	public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
	    FileSplit split = (FileSplit) genericSplit;
	    Configuration job = context.getConfiguration();
	    start = split.getStart();
	    end = start + split.getLength();
	    final Path file = split.getPath();

	    // open the file and seek to the start of the split
	    final FileSystem fs = file.getFileSystem(job);
	    fileIn = fs.open(file);
	    fileSize = fs.getFileStatus(file).getLen();
	    reader = new Question2DataReader(fileIn);
	    dataSize = reader.dataSize();
	    filePosition = fileIn;
	    
	    // If this is not the first split, we always throw away first record
	    // because we always (except the last split) read one extra record in next() method.
	    if (start != 0) {
	    	start += dataSize;
	    	long i = start % dataSize;
	    	if (i != 0) {
	    		start -= i;
	    	}
	    }
	    pos = start;
	    fileIn.seek(start);
	}
	  
	private long getFilePosition() throws IOException {
		if (null != filePosition) {
			return filePosition.getPos();
		} 
		else {
			return pos;
	    }
	}

	public boolean nextKeyValue() throws IOException {
		try {
		    if (key == null) {
		    	key = new LongWritable();
		    }
		    key.set(pos);
		    long filePos = getFilePosition();
		    // We always read one extra record, which lies outside the upper split limit i.e. (end - 1)
		    if (filePos <= end && filePos < fileSize) {
		    	value = reader.read();
		    	pos += dataSize;
		    	return true;
		    }
		    else {
		    	key = null;
		    	value = null;
		    	return false;
		    }
		}
		catch (Throwable t) {
			logger.error("Error occured while reading data at position " + pos, t);
			return false;
		}
	}

	@Override
	public LongWritable getCurrentKey() {
		return key;
	}

	@Override
	public Question2Data getCurrentValue() {
	    return value;
	}

	public float getProgress() throws IOException {
		if (start == end) {
			return 0.0f;
		} 
		else {
			return Math.min(1.0f, (getFilePosition() - start) / (float)(end - start));
		}
	}
	  
	public synchronized void close() throws IOException {
		if (reader != null) {
			reader.close();
		}
	}
	  
	class Question2DataReader implements Closeable {
			
		final static int DATA_SIZE = 2 * (Long.SIZE / 8);
		
		DataInputStream dis;
			
		Question2DataReader(InputStream is) {
			this.dis = new DataInputStream(is);
		}
			
		Question2Data read() throws IOException {
			long groupId = dis.readLong();
			long amount = dis.readLong();
			return new Question2Data(groupId, amount);
		}
		
		int dataSize() {
			return DATA_SIZE;
		}

		@Override
		public void close() throws IOException {
			dis.close();
		}
			
	}
	  
}
