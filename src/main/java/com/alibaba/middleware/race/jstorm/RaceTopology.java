package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.jstorm.bolts.OrderSaver;
import com.alibaba.middleware.race.jstorm.spouts.TBOrderReader;



/**
 * 这是一个很简单的例子
 * 选手的拓扑提交到集群，我们是有超时设置的。每个选手的拓扑最多跑20分钟，一旦超过这个时间
 * 我们会将选手拓扑杀掉。
 */

/**
 * 选手拓扑入口类，我们定义必须是com.alibaba.middleware.race.jstorm.RaceTopology
 * 因为我们后台对选手的git进行下载打包，拓扑运行的入口类默认是com.alibaba.middleware.race.jstorm.RaceTopology；
 * 所以这个主类路径一定要正确
 */
public class RaceTopology {

    private static org.apache.log4j.Logger LOG = org.apache.log4j.Logger.getLogger(RaceTopology.class);

    public static void main(String[] args) throws Exception {
    	
    	
    	java.util.logging.Logger logger = java.util.logging.Logger.getLogger(RaceTopology.class.getName());
    	logger.warning("This is java.util.logging.Logger");
    	LOG.warn("This is org.apache.log4j.Logger");
    	
    	Logger log = LoggerFactory.getLogger(RaceTopology.class);
    	log.warn("This is org.slf4j.Logger");
    	
    	System.out.println("THis it sysout");
    	
    	LOG.info("successfully call main");
        Config conf = new Config();
        int spout_Parallelism_hint = 1;
        int split_Parallelism_hint = 2;
        //int count_Parallelism_hint = 2;

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("order-spout", new TBOrderReader(), spout_Parallelism_hint);
        builder.setBolt("order-saver", new OrderSaver(), split_Parallelism_hint).shuffleGrouping("order-spout");
        
        //builder.setBolt("tb-count", new TBCounter(), split_Parallelism_hint).shuffleGrouping("tb-spout");
        //builder.setBolt("count", new WordCount(), count_Parallelism_hint).fieldsGrouping("split", new Fields("word"));
        String topologyName = RaceConfig.JstormTopologyName;

        try {
            StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}