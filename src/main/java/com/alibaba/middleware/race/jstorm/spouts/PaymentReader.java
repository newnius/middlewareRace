package com.alibaba.middleware.race.jstorm.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.Payment;
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

import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class PaymentReader implements IRichSpout {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8132576557859500158L;
	/**
	 * 
	 */
	private DefaultMQPushConsumer consumer;
	private LinkedBlockingQueue<Payment> payments;

	private static Logger LOG = LoggerFactory.getLogger(PaymentReader.class);
	private SpoutOutputCollector _collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		payments = new LinkedBlockingQueue<>();
		consumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup);
		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
		//
		if (RaceConfig.RocketMqAddr.length() > 0)
			consumer.setNamesrvAddr(RaceConfig.RocketMqAddr);
		try {
			consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");
			consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");
			consumer.subscribe(RaceConfig.MqPayTopic, "*");
		} catch (MQClientException ex) {
			ex.printStackTrace();
			LOG.error(ex.getErrorMessage());
		}

		consumer.registerMessageListener(new MessageListenerConcurrently() {

			@Override
			public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
				LOG.info("registerMessageListener called.");
				for (MessageExt msg : msgs) {
					LOG.info(msg.getTopic());
					if (msg.getTopic().equals(RaceConfig.MqPayTopic)) {
						byte[] body = msg.getBody();
						if (body.length == 2 && body[0] == 0 && body[1] == 0) {
							// Info: 生产者停止生成数据, 并不意味着马上结束
							LOG.info("Got the end signal of MqPayTopic");
							try {
								// orderMessages.put(null);
							} catch (Exception e) {
								e.printStackTrace();
								LOG.error(e.getMessage());
							}
							continue;
						}

						LOG.info("This is payment");
						PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);
						Payment payment = new Payment(paymentMessage);
						LOG.info("end of parse");
						LOG.info("OrderId:" + payment.getOrderId());
						LOG.info(payment.toString());
						try {
							payments.put(payment);
							LOG.info("after put, Total paymentss : " + payments.size());
						} catch (InterruptedException e) {
							e.printStackTrace();
							LOG.error(e.getMessage());
						}
					}
				}
				return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
			}
		});

		try {
			consumer.start();
		} catch (MQClientException e) {
			e.printStackTrace();
			LOG.error(e.getErrorMessage());
		}

	}

	@Override
	public void nextTuple() {
		LOG.info("before get, Total payments : " + payments.size());

		Payment payment = null;
		do {
			payment = payments.poll();
			if (payment != null) {
				_collector.emit(new Values(payment));
			} else {
				Utils.sleep(20);
			}
		} while (payment != null);
	}

	@Override
	public void ack(Object id) {
		LOG.info("ack " + id);
	}

	@Override
	public void fail(Object id) {
		LOG.info("fail " + id);
		_collector.emit(new Values(id), id);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("payment"));
	}

	@Override
	public void close() {

	}

	@Override
	public void activate() {

	}

	@Override
	public void deactivate() {

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}