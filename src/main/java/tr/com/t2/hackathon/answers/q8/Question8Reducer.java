/*
 * Copyright (c) 2014, "SkyKeeper Team". All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY 
 * KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A
 * PARTICULAR PURPOSE.
 */

package tr.com.t2.hackathon.answers.q8;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import tr.com.t2.hackathon.answers.Answers.BaseReducer;

/**
 * @author Serkan OZAL
 */
public class Question8Reducer extends BaseReducer<NullWritable, LongWritable, NullWritable, LongWritable> {

    protected void reduce(NullWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		try {
			long total = 0;
			Iterator<LongWritable> i = values.iterator();
			// Calculate count of tweets
			while (i.hasNext()) {
				total += i.next().get();
			}
			// Emit count of tweets without key
			context.write(NullWritable.get(), new LongWritable(total));
		}
        catch (Throwable t) {
        	logger.error("Error occured while executing reduce function of Reducer", t);
        }    
    }

}
