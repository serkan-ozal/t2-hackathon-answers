/*
 * Copyright (c) 2014, "SkyKeeper Team". All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY 
 * KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A
 * PARTICULAR PURPOSE.
 */

package tr.com.t2.hackathon.answers.q9.j1;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import tr.com.t2.hackathon.answers.Answers.BaseReducer;

/**
 * @author Serkan OZAL
 */
public class Question9Reducer1 extends BaseReducer<LongWritable, LongWritable, LongWritable, Text> {

    protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		try {
			Iterator<LongWritable> i = values.iterator();
			Set<Long> idSet = new HashSet<Long>();
			// Find unique first level connections
			while (i.hasNext()) {
				idSet.add(i.next().get());
			}
			int count = idSet.size();
			StringBuilder sb = new StringBuilder(count * 10); // Id can contain 9-10 digits at maximum
			// Build all first level connection ids
			for (Long id : idSet) {
				sb.append(id).append(" ");
			}
			// Emit all first level connection ids with their count
			context.write(key, new Text(count + " " + sb.toString()));
		}
        catch (Throwable t) {
        	logger.error("Error occured while executing reduce function of Reducer", t);
        }    
    }

}
