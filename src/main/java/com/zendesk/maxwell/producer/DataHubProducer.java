package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;

import java.util.concurrent.LinkedBlockingQueue;

public class DataHubProducer  extends AbstractProducer {

	public LinkedBlockingQueue<String> queue;

	public DataHubProducer(MaxwellContext context, LinkedBlockingQueue<String> queue) {
		super(context);
		this.queue = queue;
	}

	@Override
	public void push(RowMap r) throws Exception {

		// write queue
		String output = r.toJSON(outputConfig);
		if (output != null && !output.equalsIgnoreCase("null")) {
			queue.put(output);
		}
		context.setPosition(r);
	}
}
