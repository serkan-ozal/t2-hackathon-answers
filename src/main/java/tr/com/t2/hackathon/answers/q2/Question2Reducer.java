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

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;

import tr.com.t2.hackathon.answers.Answers.BaseReducer;

/**
 * @author Serkan OZAL
 */
public class Question2Reducer extends BaseReducer<LongWritable, LongWritable, LongWritable, LongWritable> {

    protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		try {
			long sum = 0;
			Iterator<LongWritable> i = values.iterator();
			// Calculate amounts of specific group
			while (i.hasNext()) {
				sum += i.next().get();
			}
			// Emit group id as key and its total amount as value
			context.write(key, new LongWritable(sum));
		}
        catch (Throwable t) {
        	logger.error("Error occured while executing reduce function of Reducer", t);
        }    
    }

}
