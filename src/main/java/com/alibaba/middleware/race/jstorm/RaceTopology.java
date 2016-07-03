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
import com.alibaba.middleware.race.jstorm.spouts.PaymentReader;
import com.alibaba.middleware.race.jstorm.spouts.TBOrderReader;
import com.alibaba.middleware.race.jstorm.spouts.TMOrderReader;



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

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        int spout_Parallelism_hint = 1;
        int order_saver_Parallelism_hint = 2;
        int order_getter_Parallelism_hint = 2;
        int count_Parallelism_hint = 2;
        int tair_write_Parallelism_hint = 1;

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("tb-order-reader", new TBOrderReader(), spout_Parallelism_hint);
        builder.setSpout("tm-order-reader", new TMOrderReader(), spout_Parallelism_hint);
        builder.setSpout("payment-spout", new PaymentReader(), spout_Parallelism_hint);
        
        builder.setBolt("order-saver", new OrderSaver(), order_saver_Parallelism_hint).shuffleGrouping("tb-order-reader").shuffleGrouping("tm-order-reader");
        builder.setBolt("order-getter", new OrderGetter(), order_getter_Parallelism_hint).shuffleGrouping("payment-spout");
        
        builder.setBolt("counter", new Counter(), count_Parallelism_hint).fieldsGrouping("order-getter", new Fields("minuteTime"));
        builder.setBolt("result-writer", new ResultWriter(), tair_write_Parallelism_hint).shuffleGrouping("counter");
        String topologyName = RaceConfig.JstormTopologyName;

        try {
            StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}