package com.alibaba.middleware.race.jstorm.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

public class TMOrderReader implements IRichSpout {
	/**
	 * 
	 */
	private static final long serialVersionUID = 7527748428170112375L;
	private DefaultMQPushConsumer consumer;
	private LinkedBlockingQueue<OrderMessage> orderMessages;

	private static Logger LOG = LoggerFactory.getLogger(TMOrderReader.class);
	private SpoutOutputCollector _collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		orderMessages = new LinkedBlockingQueue<>();
		consumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup);
		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
		// consumer.setNamesrvAddr("127.0.0.1:9876");
		try {
			consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");
		} catch (MQClientException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		consumer.registerMessageListener(new MessageListenerConcurrently() {

			@Override
			public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
				for (MessageExt msg : msgs) {

					byte[] body = msg.getBody();
					if (body.length == 2 && body[0] == 0 && body[1] == 0) {
						// Info: 生产者停止生成数据, 并不意味着马上结束
						System.out.println("Got the end signal");
/*						try {
							orderMessages.put(null);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}*/
						continue;
					}

					OrderMessage orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
					System.out.println(orderMessage);
					try {
						orderMessages.put(orderMessage);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
			}
		});

		try {
			consumer.start();
		} catch (MQClientException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void nextTuple() {
		
		while (1 > 0) {
			Utils.sleep(10);
			try {
				OrderMessage orderMessage = orderMessages.take();
				_collector.emit(new Values(orderMessage));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}
	}

	@Override
	public void ack(Object id) {
		// Ignored
	}

	@Override
	public void fail(Object id) {
		_collector.emit(new Values(id), id);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("TMOrder"));
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}