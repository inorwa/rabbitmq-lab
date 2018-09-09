import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

public class PublishSubscribe {

	private static final String EXCHANGE_NAME = "logs";

	public static Thread createEmitter(List<String> messages) {
		Runnable task = new Runnable() {
			@Override
			public void run() {
				ConnectionFactory factory = new ConnectionFactory();
				factory.setHost("localhost");
				try {
					Connection connection = factory.newConnection();
					Channel channel = connection.createChannel();


					channel.exchangeDeclare(EXCHANGE_NAME, "fanout");


					for (String message : messages) {
						channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes());
						System.out.println(" [x] Sent '" + message + "'");
					}

					channel.close();
					connection.close();

				} catch (IOException | TimeoutException e) {
					e.printStackTrace();
				}

			}
		};
		return new Thread(task);
	}

	public static Thread createReceiver() {
		Runnable task = new Runnable() {
			@Override
			public void run() {
				ConnectionFactory factory = new ConnectionFactory();
				factory.setHost("localhost");
				try {
					Connection connection = factory.newConnection();
					Channel channel = connection.createChannel();

					channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
					String queueName = channel.queueDeclare().getQueue();
					channel.queueBind(queueName, EXCHANGE_NAME, "");

					System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

					Consumer consumer = new DefaultConsumer(channel) {
						@Override
						public void handleDelivery(String consumerTag, Envelope envelope,
						                           AMQP.BasicProperties properties, byte[] body) throws IOException {
							String message = new String(body, "UTF-8");
							System.out.println(" [x] Received '" + message + "'");
						}
					};

					channel.basicConsume(queueName, true, consumer);
				} catch (IOException e) {
					e.printStackTrace();
				} catch (TimeoutException e) {
					e.printStackTrace();
				}
			}
		};
		return new Thread(task);
	}

	public static void main(String[] args) {

		Thread emitter = createEmitter(IntStream.range(0, 10).mapToObj(i -> "message " + i).collect(toList()));
		Thread receiver = createReceiver();

		receiver.start();
		emitter.start();


		try {
			TimeUnit.SECONDS.sleep(5);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.exit(1);
	}
}
