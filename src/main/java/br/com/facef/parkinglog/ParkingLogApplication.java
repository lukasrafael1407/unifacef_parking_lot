package br.com.facef.parkinglog;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

import java.util.List;
import java.util.Map;

@SpringBootApplication
public class ParkingLogApplication {

	private static final String ORIGINAL_QUEUE = "order-messages-queue";

	private static final String DLQ = ORIGINAL_QUEUE + ".dlq";

	private static final String PARKING_LOT = ORIGINAL_QUEUE + ".parkingLot";

	private static final String X_DEATH_HEADER = "x-death";

	private static final String X_RETRIES_HEADER = "x-retries";

	public static void main(String[] args) throws Exception {
		ConfigurableApplicationContext context = SpringApplication.run(ParkingLogApplication.class, args);
		System.out.println("Hit enter to terminate");
		System.in.read();
		context.close();
	}

	@Autowired
	private RabbitTemplate rabbitTemplate;

	@SuppressWarnings("unchecked")
	@RabbitListener(queues = DLQ)
	public void rePublish(Message failedMessage) {
		Map<String, Object> headers = failedMessage.getMessageProperties().getHeaders();
		Integer retriesHeader = (Integer) headers.get(X_RETRIES_HEADER);
		if (retriesHeader == null) {
			retriesHeader = Integer.valueOf(0);
		}
		if (retriesHeader < 3) {
			headers.put(X_RETRIES_HEADER, retriesHeader + 1);
			List<Map<String, ?>> xDeath = (List<Map<String, ?>>) headers.get(X_DEATH_HEADER);
			String exchange = (String) xDeath.get(0).get("exchange");
			List<String> routingKeys = (List<String>) xDeath.get(0).get("routing-keys");
			this.rabbitTemplate.send(exchange, routingKeys.get(0), failedMessage);
		}
		else {
			this.rabbitTemplate.send(PARKING_LOT, failedMessage);
		}
	}

	@Bean
	public Queue parkingLot() {
		return new Queue(PARKING_LOT);
	}

}

