import com.rabbitmq.client.*;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

public class WorkQueues {

	private final static String QUEUE_NAME = "hello";

	public static Thread createPublisher() {
		Runnable task = () -> {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost("localhost");
			try {

				Connection connection = factory.newConnection();
				Channel channel = connection.createChannel();
				channel.queueDeclare(QUEUE_NAME, true, false, false, null);

				for (int i = 0; i < 10; i++) {
					String message = "Hello world " + i;

					channel.basicPublish("",
							QUEUE_NAME,
							MessageProperties.PERSISTENT_TEXT_PLAIN,
							message.getBytes());

					System.out.println(" [x] Sent '" + message + "'");
				}
				channel.close();
				connection.close();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (TimeoutException e) {
				e.printStackTrace();
			}
		};
		return new Thread(task);
	}

	public static Thread createConsumer() {
		Runnable task = () -> {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost("localhost");
			try {
				Connection connection = factory.newConnection();
				Channel channel = connection.createChannel();


				channel.queueDeclare(QUEUE_NAME, true, false, false, null);
				System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

				channel.basicQos(1);

				Consumer consumer = new DefaultConsumer(channel) {
					@Override
					public void handleDelivery(String consumerTag, Envelope envelope,
					                           AMQP.BasicProperties properties, byte[] body)
							throws IOException {
						String message = new String(body, "UTF-8");
						System.out.println("Thread " + Thread.currentThread().getName() + " [x] Received '" + message + "'");
						try {
							Thread.sleep(10);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}finally {
							channel.basicAck(envelope.getDeliveryTag(),false);
						}
					}
				};

				channel.basicConsume(QUEUE_NAME, false, consumer);


			} catch (IOException e) {
				e.printStackTrace();
			} catch (TimeoutException e) {
				e.printStackTrace();
			} finally {

			}

		};
		return new Thread(task);
	}

	public static void main(String[] args) {
		System.out.println("START");

		Thread senderThread = createPublisher();
		Thread receiverThread1 = createConsumer();
		Thread receiverThread2 = createConsumer();
		senderThread.start();
		receiverThread1.start();
		receiverThread2.start();

		try {
			TimeUnit.SECONDS.sleep(2);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		try {
			System.out.println("try stop");
			senderThread.join();
			System.out.println("joined");
			receiverThread1.stop();
			receiverThread2.stop();
			System.out.println("interrupted");
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.exit(-1);

	}
}
