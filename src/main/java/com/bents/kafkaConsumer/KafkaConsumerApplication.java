package com.bents.kafkaConsumer;

import org.apache.kafka.clients.consumer.Consumer;

import com.bents.kafkaConsumer.constants.IKafkaConstants;
import com.bents.kafkaConsumer.dto.KafkaMessageDTO;
import com.bents.kafkaConsumer.factory.ConsumerFactory;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaConsumerApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(KafkaConsumerApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		Consumer<Long, KafkaMessageDTO> consumer = ConsumerFactory.createConsumer();

		int noMessageToFetch = 0;

		while (true) {
			final ConsumerRecords<Long, KafkaMessageDTO> consumerRecords = consumer.poll(1000);
			if (consumerRecords.count() == 0) {
				noMessageToFetch++;
				if (noMessageToFetch > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
					break;
				else
					continue;
			}

			consumerRecords.forEach(record -> {
				System.out.println("Record Key " + record.key());
				System.out.println("Record value " + record.value());
				System.out.println("Record partition " + record.partition());
				System.out.println("Record offset " + record.offset());
			});
			consumer.commitAsync();
		}
		consumer.close();
	}
}
