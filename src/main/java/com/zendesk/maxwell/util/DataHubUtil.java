package com.zendesk.maxwell.util;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.zendesk.maxwell.MaxwellConfig;

public class DataHubUtil {

	private MaxwellConfig config;

	public DataHubUtil() {
	}

	public static DataHubUtil newBuilder() {
		return new DataHubUtil();
	}

	public DataHubUtil setConfig(MaxwellConfig config) {
		this.config = config;
		return this;
	}

	public DatahubClient build() {
		AliyunAccount aliyunAccount = new AliyunAccount(this.config.accessId, this.config.accessKey);
		DatahubConfig datahubConfig = new DatahubConfig(this.config.datahubEndPoint, aliyunAccount);
		DatahubClient client = DatahubClientBuilder.newBuilder().setDatahubConfig(datahubConfig).build();
		return client;
	}

	public void createTopic() {

	}


}
