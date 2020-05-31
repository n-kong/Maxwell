package com.zendesk.maxwell.producer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.model.*;
import com.zendesk.maxwell.MaxwellConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DataHubConsumer extends Thread {

	public LinkedBlockingQueue<String> queue;
	private DatahubClient client;
	private MaxwellConfig config;
	private String projectName;
	private String topicName;
	private Map<String, DataHubNode> map = new ConcurrentHashMap<>();
	private static Logger LOGGER = LoggerFactory.getLogger(DataHubConsumer.class);
	private JSONObject rowJson;

	public DataHubConsumer(LinkedBlockingQueue<String> queue, DatahubClient client, MaxwellConfig config) {
		this.queue = queue;
		this.client = client;
		this.config = config;
	}

	@Override
	public void run() {

		while (true) {
			try {
				// 从队列取数据，队列为空，等待2秒
				String row = queue.poll(2, TimeUnit.SECONDS);

				if (null != row) {
					rowJson = sortField(row);
					LOGGER.info("the result is: {}", rowJson.toJSONString());
					// check topic is exist
					cheakTopic();
					// 解析数据
					RecordEntry recordEntry = parseRow();
					map.get(topicName).getList().add(recordEntry);
				} else {
					LOGGER.info("Queue is empty. Sleep 2 second!");
				}

				putRecord();

			} catch (Exception e) {
				LOGGER.error("Error message is: {}", e.getMessage());
			}
		}

	}

	/**
	 * write to datahub
	 */
	private void putRecord() {

		Set<Map.Entry<String, DataHubNode>> entries = map.entrySet();
		for (Map.Entry<String, DataHubNode> entry : entries) {
			String topicName = entry.getKey();
			DataHubNode node = entry.getValue();
			long lastUpdateTime = node.getTime();
			List<RecordEntry> recordEntries = node.getList();
			int size = recordEntries.size();
			long newTime = System.currentTimeMillis() / 1000;
			String pName = config.projectName;
			if (0 != size && (size > Integer.parseInt(config.outputDataHubSize) || newTime - lastUpdateTime > Integer.parseInt(config.outputDataHubInterval))) {
				PutRecordsResult putRecordsResult = client.putRecords(pName, topicName, recordEntries);
				int failedCount = putRecordsResult.getFailedRecordCount();
				if (0 != failedCount) {
					LOGGER.error("write to {}.{} failed num = {}", pName, topicName, failedCount);
				}
				LOGGER.info("write to {}.{} success num = {}", pName, topicName, size - failedCount);
				node.setTime(newTime);
				recordEntries.clear();
			}

		}

	}

	/**
	 * parse data
	 * @return datahub record
	 */
	private RecordEntry parseRow() {
		RecordEntry recordEntry = new RecordEntry();
		try {
			RecordSchema recordSchema = map.get(topicName).getRecordSchema();
			TupleRecordData recordData = new TupleRecordData(recordSchema);
			Set<Map.Entry<String, Object>> entries = rowJson.getJSONObject("data").entrySet();
			for (Map.Entry<String, Object> entry : entries) {
				recordData.setField(entry.getKey(), replaceBlank(String.valueOf(entry.getValue())));
			}
			recordEntry.setRecordData(recordData);
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
		}
		return recordEntry;
	}

	/**
	 * replace \t \r \n
	 * @param inStr source data
	 * @return target data
	 */
	public String replaceBlank(String inStr) {
		if (null != inStr && !"null".equalsIgnoreCase(inStr)) {
			Pattern p = Pattern.compile("\t|\r|\n");
			Matcher m = p.matcher(inStr);
			inStr = m.replaceAll("");
		}
		return inStr;
	}

	/**
	 * sort data by key
	 * @param row source data
	 * @return target data
	 */
	private JSONObject sortField(String row) {
		// 将数据转换为有序的JSON格式
		LinkedHashMap linkedHashMap = JSON.parseObject(row, LinkedHashMap.class, Feature.OrderedField);
		JSONObject r = new JSONObject(true);
		r.putAll(linkedHashMap);
		return r;
	}

	/**
	 * check datahub topic is exists
	 */
	private void cheakTopic() {
		long time = System.currentTimeMillis() / 1000;
		projectName = config.projectName;
		topicName = rowJson.getString("table");
		if (!map.containsKey(topicName)) {
			createTopic();
			RecordSchema recordSchema = client.getTopic(projectName, topicName).getRecordSchema();
			map.put(topicName, new DataHubNode(time, recordSchema));
		}
	}

	/**
	 * get datahub topic schema for row data
	 * @return
	 */
	private RecordSchema getSchema() {
		Set<String> cols = rowJson.getJSONObject("data").keySet();
		RecordSchema recordSchema = new RecordSchema();
		for (String col : cols) {
			recordSchema.addField(new Field(col, FieldType.STRING));
		}
		return recordSchema;
	}

	/**
	 * create datahub topic
	 */
	private void createTopic() {
		RecordSchema schema = getSchema();
		if (client.listTopic(projectName).getTopicNames().contains(topicName)) {
			LOGGER.info("the topic:{} is exists!", topicName);
			return;
		}
		int shardCount = Integer.parseInt(config.shardCount);
		int lifeCycle = Integer.parseInt(config.lifeCycle);
		client.createTopic(projectName, topicName, shardCount, lifeCycle, RecordType.TUPLE, schema, topicName);
		client.waitForShardReady(projectName, topicName);
	}

}


class DataHubNode {

	// time  out
	private long time;
	// Record list
	private List<RecordEntry> list;

	RecordSchema recordSchema;

	public DataHubNode(long time, RecordSchema recordSchema) {
		this.time = time;
		this.recordSchema = recordSchema;
		initList();
	}

	public List getList() {
		return list;
	}

	public void setList(List list) {
		this.list = list;
	}

	public RecordSchema getRecordSchema() {
		return recordSchema;
	}

	public void setRecordSchema(RecordSchema recordSchema) {
		this.recordSchema = recordSchema;
	}

	public void initList() {
		list = new ArrayList();
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}


}
