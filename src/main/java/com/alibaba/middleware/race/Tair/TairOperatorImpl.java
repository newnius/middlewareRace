package com.alibaba.middleware.race.Tair;

import com.alibaba.middleware.race.RaceConfig;
import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 读写tair所需要的集群信息，如masterConfigServer/slaveConfigServer地址/ group
 * 、namespace我们都会在正式提交代码前告知选手
 */
public class TairOperatorImpl {

	private DefaultTairManager tairManager;
	private Logger logger;

	public TairOperatorImpl(String masterConfigServer, String slaveConfigServer, String groupName, int namespace) {
		// 创建config server列表
		List<String> confServers = new ArrayList<String>();
		confServers.add(masterConfigServer);
		//confServers.add(slaveConfigServer); // 可选

		// 创建客户端实例
		tairManager = new DefaultTairManager();
		tairManager.setConfigServerList(confServers);

		// 设置组名
		tairManager.setGroupName(groupName);
		// 初始化客户端
		tairManager.init();

		logger = LoggerFactory.getLogger(getClass());
	}

	public boolean write(Serializable key, Serializable value) {
		logger.info("Tair set " + key);
		// 第一个参数是namespace，第二个是key，第三是value，第四个是版本，第五个是有效时间
		ResultCode result = tairManager.put(RaceConfig.TairNamespace, key, value);
		return result.isSuccess();
	}

	public Object get(Serializable key) {
		logger.info("Tair get " + key);
		// 第一个参数是namespce，第二个是key
		Result<DataEntry> result = tairManager.get(RaceConfig.TairNamespace, key);
		if (result.isSuccess()) {
			DataEntry entry = result.getValue();
			if (entry != null) {
				// 数据存在
				logger.info("value is " + entry.getValue().toString());
				return entry.getValue();
			} else {
				// 数据不存在
				logger.info("Tair key " + key + " doesn't exist.");
			}
		} else {
			// 异常处理
			logger.warn(result.getRc().getMessage());
		}

		return null;
	}

	public boolean remove(Serializable key) {
		return false;
	}

	public void close() {
	}

}
