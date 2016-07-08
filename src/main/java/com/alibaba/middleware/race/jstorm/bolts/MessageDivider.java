package com.alibaba.middleware.race.jstorm.bolts;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.common.message.MessageExt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MessageDivider implements IRichBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = 4201243201976307440L;
	
	
	private OutputCollector collector;
	
//	private LinkedBlockingQueue<OrderMessage> TBorders;
//	private ConcurrentHashMap<Long, OrderMessage> waitingTBOrders;
//	private LinkedBlockingQueue<OrderMessage> TMorders;
//	private ConcurrentHashMap<Long, OrderMessage> waitingTMOrders;
//	private LinkedBlockingQueue<PaymentMessage> payments;
//	private Map<Long, PaymentMessage> waitingPayments;
//	
	private Logger LOG;
	

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		
//		waitingTBOrders = new ConcurrentHashMap<>();
//		TBorders = new LinkedBlockingQueue<>();
//		waitingTMOrders = new ConcurrentHashMap<>();
//		TMorders = new LinkedBlockingQueue<>();
//		payments = new LinkedBlockingQueue<>();
//		waitingPayments = new ConcurrentHashMap<>();
//		
		this.LOG = LoggerFactory.getLogger(getClass());
		
		
	}

	@Override
	public void execute(Tuple input) {
		if("flush".equals(input.getSourceStreamId())){
			collector.emit("flush", new Values());
			collector.ack(input);
			return ;
		}
		
		
		@SuppressWarnings("unchecked")
		List<MessageExt> msgs = (List<MessageExt>) input.getValueByField("msgs");
		
		for (MessageExt msg : msgs) {
			byte[] body = msg.getBody();
			OrderMessage orderMessage;

			switch (msg.getTopic()) {
			case RaceConfig.MqTaobaoTradeTopic:

				if (body.length == 2 && body[0] == 0 && body[1] == 0) {
					// Info: 生产者停止生成数据, 并不意味着马上结束
					LOG.info("Got the end signal of TBOrderMessage");
					continue;
				}

				orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
				collector.emit("tb-order", new Values(orderMessage.getOrderId(), orderMessage));
				//LOG.info("emit tb-order " + orderMessage.getOrderId());
				break;

			case RaceConfig.MqTmallTradeTopic:
				if (body.length == 2 && body[0] == 0 && body[1] == 0) {
					// Info: 生产者停止生成数据, 并不意味着马上结束
					LOG.info("Got the end signal of TMOrderMessage");
					continue;
				}

				orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
				collector.emit("tm-order", new Values(orderMessage.getOrderId(), orderMessage));
				//LOG.info("emit tm-order " + orderMessage.getOrderId());
				break;

			case RaceConfig.MqPayTopic:
				if (body.length == 2 && body[0] == 0 && body[1] == 0) {
					// Info: 生产者停止生成数据, 并不意味着马上结束
					// LOG.info("Got the end signal of payments");
					continue;
				}
				PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);
				collector.emit("payment", new Values(paymentMessage.getOrderId(), paymentMessage));
				//LOG.info("emit payment " + paymentMessage.getOrderId());
				break;
			}

		}
		collector.ack(input);
	}

	@Override
	public void cleanup() {
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("tb-order", new Fields("orderId", "order"));
		declarer.declareStream("tm-order", new Fields("orderId", "order"));
		declarer.declareStream("payment", new Fields("orderId", "payment"));
		declarer.declareStream("flush", new Fields());
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
