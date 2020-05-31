package com.zendesk.maxwell.producer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import com.zendesk.maxwell.Maxwell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Author: nkong
 * @Date: 2020/4/17 14:07
 * @Version 1.0
 */

public class FileConsumer extends Thread {
	public LinkedBlockingQueue<String> queue;
	private String SEP = "\t";
	private String interval;
	private String size;
	private Map<String, Node> map = new ConcurrentHashMap<>();
	String filePath;
	String fileTmpPath;
	static final Logger LOGGER = LoggerFactory.getLogger(FileConsumer.class);

	public FileConsumer(String interval, String size, String path, String tmpPath, LinkedBlockingQueue<String> queue) throws IOException {
		this.queue = queue;
		this.filePath = path;
		this.fileTmpPath = tmpPath;
		this.interval = interval;
		this.size = size;
	}

	@Override
	public void run() {
		while (true) {
			try {
				// 从队列取数据，队列为空，等待2秒
				String row = queue.poll(2, TimeUnit.SECONDS);
				long time = System.currentTimeMillis() / 1000;
				if (null != row) {
					// 将数据转换为有序的JSON格式
					LinkedHashMap linkedHashMap = JSON.parseObject(row, LinkedHashMap.class, Feature.OrderedField);
					JSONObject jsonObject = new JSONObject(true);
					jsonObject.putAll(linkedHashMap);
					// 表名
					String tableName = jsonObject.getString("table");
					Node node = map.get(tableName);
					// 如果有新表或已有流死亡，则初始化流节点
					if (null == node || !node.isWriteIsActive()) {
						String pathTmp = fileTmpPath + "/" + tableName + "_" + System.currentTimeMillis() + ".nb";
						map.put(tableName, new Node(pathTmp, time));
					}
					// 解析数据
					String result = parse(jsonObject);
					// 获取当前数据流节点
					Node node2 = map.get(tableName);
					// 为流节点绑定最新时间
					node2.setTime(time);
					// 获取流对象，并把数据写入流，进行缓存
					BufferedWriter writer = node2.getWriter();
					writer.write(result);
					// 流数据计数器加1
					node2.addOneNum();
				} else {
					LOGGER.info("Queue is empty!");
				}
				// 遍历所有对象，当有流数据满足输出条件则输出
				Set<Map.Entry<String, Node>> entries = map.entrySet();
				for (Map.Entry<String, Node> entry : entries) {
					// 获取输出流节点
					Node node1 = entry.getValue();
					// 获取流内数据计数器
					int messageTotal = node1.getNum();
					// 流内数据不为空并且满足数量上限或时间间隔，则输出
					if (messageTotal != 0 && (messageTotal >= Integer.parseInt(size) || time - node1.getTime() > Long.valueOf(interval))) {
						// 获取表对应的输出流
						BufferedWriter outputWrite = node1.getWriter();
						outputWrite.flush();
						outputWrite.close();
						// 输出表名
						String tableName = entry.getKey();
						LOGGER.info("----");
						LOGGER.info("Table:{}, Write success num: {}", tableName, messageTotal);
						// 流关闭之后，存活状态改为false，下次使用需要再初始化
						node1.setWriteIsActive(false);
						// 数量计数器置0
						node1.initNum();
						// 获取文件输出临时路径
						String tmpPath = node1.getTmpPath();
						// 文件输出临时路径更改为最终路径
						File file = new File(tmpPath);
						file.renameTo(new File(filePath + "/" + file.getName()));
					}
				}
			} catch (Exception e) {
				LOGGER.error("Error message is: {}", e.getMessage());
			}
		}

	}

	/**
	 * 解析数据
	 *
	 * @param jsonObject 数据对象
	 * @return 返回\t分割数据
	 */
	public String parse(JSONObject jsonObject) {
		StringBuilder sb = new StringBuilder();
		try {
			// 获取数据
			JSONObject datas = jsonObject.getJSONObject("data");
			LOGGER.info("Result is: {}", jsonObject.toJSONString());
			Set<Map.Entry<String, Object>> entries = datas.entrySet();
			Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
			while (iterator.hasNext()) {
				Map.Entry<String, Object> next = iterator.next();
				sb.append(replaceBlank(String.valueOf(next.getValue())));
				if (iterator.hasNext()) {
					sb.append(SEP);
				}
			}
			sb.append("\n");
		} catch (Exception e) {
			LOGGER.error("Error  message is: {}", e.getMessage());
			LOGGER.error("The data is: {}", jsonObject.toJSONString());
		}
		return sb.toString();
	}

	public String replaceBlank(String inStr) {
		String outStr = "";
		if (null != inStr && !"null".equalsIgnoreCase(inStr)) {
			Pattern p = Pattern.compile("\t|\r|\n");
			Matcher m = p.matcher(inStr);
			outStr = m.replaceAll("");
		}
		return outStr;
	}

}


/**
 * 流节点，绑定输出流相关参数
 */
class Node {

	// 流存活标志
	private boolean writeIsActive;
	// 临时文件路径
	private String tmpPath;
	// 输出流
	private BufferedWriter writer;
	// 流存活时间
	private long time;
	// 流内数据计数器
	private int num;

	public Node(String tmpPath, long time) {
		this.writeIsActive = true;
		this.tmpPath = tmpPath;
		this.time = time;
		this.num = 0;
		initFileWrite();
	}

	public String getTmpPath() {
		return tmpPath;
	}

	public void setTmpPath(String tmpPath) {
		this.tmpPath = tmpPath;
	}

	public int getNum() {
		return num;
	}

	public void addOneNum() {
		this.num += 1;
	}

	public void initFileWrite() {
		try {
			writer = new BufferedWriter(new FileWriter(tmpPath));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public BufferedWriter getWriter() {
		return writer;
	}

	public void initNum() {
		this.num = 0;
	}

	public boolean isWriteIsActive() {
		return writeIsActive;
	}

	public void setWriteIsActive(boolean writeIsActive) {
		this.writeIsActive = writeIsActive;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}


}
