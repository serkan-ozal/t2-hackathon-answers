/*
 * Copyright (c) 2014, "SkyKeeper Team". All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY 
 * KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A
 * PARTICULAR PURPOSE.
 */

package tr.com.t2.hackathon.answers.q9.j2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import tr.com.t2.hackathon.answers.Answers.BaseMapper;

/**
 * @author Serkan OZAL
 */
public class Question9Mapper2 extends BaseMapper<LongWritable, Text, LongWritable, LongWritable> {

	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
        	String line = value.toString();
        	String[] lineParts = line.split("\\s+");
        	// First split is id of user
        	long userId = Long.parseLong(lineParts[0]);
        	// Second split is count of first level connections
        	int connectionCount = Integer.parseInt(lineParts[1]);
        	long[] connectionIds = new long[connectionCount];
        	// Read first level connections and emit them again 
        	for (int i = 0; i < connectionCount; i++) {
        		connectionIds[i] = Long.parseLong(lineParts[2 + i]);
        		context.write(new LongWritable(userId), new LongWritable(connectionIds[i]));
        	}
        	// Emit first level connections to each other to help them for finding their second level connections 
        	for (int i = 0; i < connectionCount; i++) {
        		for (int j = 0; j < connectionCount; j++) {
        			if (i != j) {
        				context.write(new LongWritable(connectionIds[i]), new LongWritable(connectionIds[j]));
        			}	
        		}
        	}
        }
        catch (Throwable t) {
        	logger.error("Error occured while executing map function of Mapper", t);
        }
    }

}
