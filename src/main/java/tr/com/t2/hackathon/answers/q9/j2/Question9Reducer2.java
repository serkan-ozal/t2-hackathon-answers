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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;

import tr.com.t2.hackathon.answers.Answers.BaseReducer;

/**
 * @author Serkan OZAL
 */
public class Question9Reducer2 extends BaseReducer<LongWritable, LongWritable, LongWritable, LongWritable> {

	private Long maxConnection;
	private Long maxConnectionUserId;
	
    protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		try {
			long userId = key.get();
			Iterator<LongWritable> i = values.iterator();
			Set<Long> idSet = new HashSet<Long>();
			// Find all unique connections
			while (i.hasNext()) {
				long id = i.next().get();
				// If connection doesn't point to current user
				if (userId != id) {
					idSet.add(id);
				}	
			}
			long connectionCount = idSet.size();
			if (maxConnectionUserId == null) {
				maxConnectionUserId = userId;
				maxConnection = connectionCount;
			}
			else if (connectionCount > maxConnection) {
				maxConnectionUserId = userId;
				maxConnection = connectionCount;
			}
			else if (connectionCount == maxConnection && userId > maxConnectionUserId) {
				maxConnectionUserId = userId;
				maxConnection = connectionCount;
			}
		}
        catch (Throwable t) {
        	logger.error("Error occured while executing reduce function of Reducer", t);
        }    
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	// Emit local user with maximum connection count
    	if (maxConnectionUserId != null) {
    		context.write(new LongWritable(maxConnectionUserId), new LongWritable(maxConnection));
    	}	
    }

}
