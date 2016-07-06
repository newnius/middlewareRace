package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.jstorm.bolts.Counter;
import com.alibaba.middleware.race.jstorm.bolts.OrderGetter;
import com.alibaba.middleware.race.jstorm.bolts.OrderSaver;
import com.alibaba.middleware.race.jstorm.bolts.ResultWriter;
import com.alibaba.middleware.race.jstorm.spouts.MqReader;

/**
 * 这是一个很简单的例子
 * 选手的拓扑提交到集群，我们是有超时设置的。每个选手的拓扑最多跑20分钟，一旦超过这个时间
 * 我们会将选手拓扑杀掉。
 */

/**
 * 选手拓扑入口类，我们定义必须是com.alibaba.middleware.race.jstorm.RaceTopology
 * 因为我们后台对选手的git进行下载打包，拓扑运行的入口类默认是com.alibaba.middleware.race.jstorm.
 * RaceTopology； 所以这个主类路径一定要正确
 */
public class RaceTopology {

	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		int spout_Parallelism_hint = 1;
		int order_saver_Parallelism_hint = 3;
		int order_getter_Parallelism_hint = 4;
		int count_Parallelism_hint = 1;
		int tair_write_Parallelism_hint = 3;

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("mq-reader", new MqReader(), spout_Parallelism_hint);

		//builder.setBolt("order-saver", new OrderSaver(), order_saver_Parallelism_hint).shuffleGrouping("mq-reader", "tb-order").shuffleGrouping("mq-reader", "tm-order");
		//builder.setBolt("order-getter", new OrderGetter(), order_getter_Parallelism_hint).shuffleGrouping("mq-reader","payment");

		builder.setBolt("counter", new Counter(), count_Parallelism_hint).fieldsGrouping("mq-reader",
				"payment", new Fields("minuteTime")).allGrouping("mq-reader", "flush");
		builder.setBolt("result-writer", new ResultWriter(), tair_write_Parallelism_hint).fieldsGrouping("counter", new Fields("key"));
		String topologyName = RaceConfig.JstormTopologyName;

		try {
			StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}