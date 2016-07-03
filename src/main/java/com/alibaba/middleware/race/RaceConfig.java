package com.alibaba.middleware.race;

import java.io.Serializable;

public class RaceConfig implements Serializable {

    /**
	 * 
	 */
	private static final long serialVersionUID = 7160717176287541054L;
	//这些是写tair key的前缀
    public static String prex_tmall = "platformTmall_";
    public static String prex_taobao = "platformTaobao_";
    public static String prex_ratio = "ratio_";
    
    public static String prex_order = "order_";


    //这些jstorm/rocketMq/tair 的集群配置信息，这些配置信息在正式提交代码前会被公布
    public static String JstormTopologyName = "41530gvxko";
    public static String MetaConsumerGroup = "41530gvxko";
    public static String MqPayTopic = "MiddlewareRaceTestData_Pay";
    public static String MqTmallTradeTopic = "MiddlewareRaceTestData_TMOrder";
    public static String MqTaobaoTradeTopic = "MiddlewareRaceTestData_TBOrder";
    public static String TairSalveConfigServer = "10.101.72.128:5198";
    
    //remote
    public static String TairConfigServer = "10.101.72.127:5198";
    public static String TairGroup = "group_tianchi";
    public static Integer TairNamespace = 16332;
    public static final String RocketMqAddr = "";
    
    //local test
//    public static Integer TairNamespace = 0;
//    public static final String RocketMqAddr = "192.168.56.104:9876";
//    public static String TairGroup = "group_1";
//    public static String TairConfigServer = "192.168.56.102:5198";
}
