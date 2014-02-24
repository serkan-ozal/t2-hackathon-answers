/*
 * Copyright (c) 2014, "SkyKeeper Team". All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY 
 * KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A
 * PARTICULAR PURPOSE.
 */

package tr.com.t2.hackathon.answers.io;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

/**
 * @author Serkan OZAL
 */
public class LineRecordReader extends org.apache.hadoop.mapreduce.lib.input.LineRecordReader {

    protected final Logger logger = Logger.getLogger(getClass());
    
    private Path file;
    
    public LineRecordReader() {
        
    }

    public LineRecordReader(byte[] recordDelimiter) {
        super(recordDelimiter);
    }
    
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
        super.initialize(genericSplit, context);
        file = ((FileSplit)genericSplit).getPath();
    }   
        
    public boolean nextKeyValue() throws IOException {
        try {
            return super.nextKeyValue();
        }
        catch (Throwable t) {
            logger.error("Error while getting next key-value for file " + file.getName(), t);
            throw new IOException("Error while getting next key-value for file " + file.getName(), t);
        }
    }
    
}
