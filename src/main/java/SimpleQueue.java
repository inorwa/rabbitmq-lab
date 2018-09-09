import com.rabbitmq.client.*;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

public class SimpleQueue {

	private final static String QUEUE_NAME = "hello";

	public static void main(String[] args) {
		System.out.println("START");


		Runnable senderTask = () -> {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost("localhost");
			try {

				Connection connection = factory.newConnection();
				Channel channel = connection.createChannel();
				channel.queueDeclare(QUEUE_NAME, false, false, false, null);
				for (int i = 0; i < 10; i++) {
					String message = "Hello world " + LocalDateTime.now();
					channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
					System.out.println(" [x] Sent '" + message + "'");

					Thread.sleep(100);
				}
				channel.close();
				connection.close();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (TimeoutException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		};

		Runnable receiverTask = () -> {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost("localhost");
			try {
				Connection connection = factory.newConnection();
				Channel channel = connection.createChannel();

				channel.queueDeclare(QUEUE_NAME, false, false, false, null);
				System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

				Consumer consumer = new DefaultConsumer(channel) {
					@Override
					public void handleDelivery(String consumerTag, Envelope envelope,
					                           AMQP.BasicProperties properties, byte[] body)
							throws IOException {
						String message = new String(body, "UTF-8");
						System.out.println(" [x] Received '" + message + "'");
					}
				};

				channel.basicConsume(QUEUE_NAME, true, consumer);


			} catch (IOException e) {
				e.printStackTrace();
			} catch (TimeoutException e) {
				e.printStackTrace();
			}

		};

		Thread senderThread = new Thread(senderTask);
		Thread receiverThread = new Thread(receiverTask);
		senderThread.start();
		receiverThread.start();

		try {
			TimeUnit.SECONDS.sleep(2);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		try {
			System.out.println("try stop");
			senderThread.join();
			System.out.println("joined");
			receiverThread.stop();
			System.out.println("interrupted");
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.exit(-1);

	}
}
