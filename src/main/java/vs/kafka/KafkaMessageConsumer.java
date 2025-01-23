package vs.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.Logger;

import akhil.DataUnlimited.util.LogStackTrace;
import vs.util.StringOps;

public class KafkaMessageConsumer {

	private final Logger LOGGER = LogManager.getLogger(KafkaMessageConsumer.class.getName());
	private Consumer<String, String> consumer;
	private List<Boolean> interrupted;
	private ExecutorService es = Executors.newFixedThreadPool(1);

	private String name;
	private OnMessageTester tester;

	public OnMessageTester getTester() {
		return tester;
	}

	public void close() {
		interrupted.remove(0);
		interrupted.add(false);
		if (consumer != null)
			consumer.wakeup();
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
		}
		tester.shutdown();
		es.shutdown();
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
		}
		es.shutdownNow();
		tester.shutdownNow();
	}

	public void start() {
		this.interrupted.add(true);
		Runnable r = () -> {
			try {
				while (interrupted.get(0)) {
					if (consumer != null) {
						ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
						for (ConsumerRecord<String, String> consumedRecord : records) {
							if (interrupted.get(0)) {
								tester.processAsync(consumedRecord.value());
								if (tester.getPending() > KafkaSVMap.getBatchSize())
									Thread.sleep(KafkaSVMap.getWaitPerMessage());
							} else
								break;
						}
						consumer.commitAsync();
					} else {
						LOGGER.error("{}...Consumer is null...\n", new Object[] { name });
						consumer.close();
						break;
					}
				}
			} catch (WakeupException e) {
				LOGGER.error("{}...Wakeup exception Consumer...\n", new Object[] { name });
			} catch (Exception e) {
				LOGGER.error("{}...Exception Consumer...\n {}", new Object[] { name, LogStackTrace.get(e) });
			} finally {
				consumer.close();
				LOGGER.error("{}...Consumer closed finally...\n", new Object[] { name });
				close();
			}
		};
		es.submit(r);
	}

	public String setKafkaMessageConsumer(String name, String epws, String brokerUrl, String oauthTokenUrl,
			String oauthClientId, String oauthClientSecret, String oauthScope, String topicName, String groupId,
			boolean isGroupIdRandomized, boolean isEnablePost, boolean isEnableRequestLog, boolean isEnableResponseLog,
			boolean isEnablePostResponseLog, String status) {

		// synchronized (KafkaCallProcessor.lockSystemProperty) {
		try {
			this.name = name;
			this.interrupted = new ArrayList<>();
			tester = new OnMessageTester(name);
			tester.enablePost(isEnablePost);
			tester.enableRequestLog(isEnableRequestLog);
			tester.enableResponseLog(isEnableResponseLog);
			tester.enablePostResponseLog(isEnablePostResponseLog);
			if (status.equals("Paused"))
				tester.pause(true);
			else
				tester.pause(false);
			String responseString;
			if (epws != null)
				responseString = tester.setEPWs(epws);
			else
				responseString = "SUCCESS";

			if (responseString.contains("SUCCESS")) {

				// System.setProperty("oauth.token.endpoint.uri", oauthTokenUrl);
				// System.setProperty("oauth.client.id", oauthClientId);
				// System.setProperty("oauth.client.secret", oauthClientSecret);
				// System.setProperty("oauth.scope", oauthScope);

				String sasl_jaas_config = StringOps.append(
						"org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required",
						" oauth.token.endpoint.uri=\"", oauthTokenUrl, "\" oauth.client.id=\"", oauthClientId,
						"\" oauth.client.secret=\"", oauthClientSecret, "\"");

				if (!oauthScope.equalsIgnoreCase("null") && !oauthScope.equalsIgnoreCase("not set"))
					sasl_jaas_config = StringOps.append(sasl_jaas_config, " oauth.scope=\"", oauthScope, "\";");
				else
					sasl_jaas_config = StringOps.append(sasl_jaas_config, ";");

				// Connection
				Properties props = new Properties();
				props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);

				LOGGER.info("sasl_jaas_config to create the consumer for service {}: {}\n",
						new Object[] { name, sasl_jaas_config });

				if (isGroupIdRandomized)
					groupId = StringOps.append(groupId, "T", String.valueOf(new Date().getTime()));

				props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

				props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

				// Auth
				props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
				props.put(SaslConfigs.SASL_MECHANISM, "OAUTHBEARER");
				// props.put(SaslConfigs.SASL_JAAS_CONFIG,
				// "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule
				// required;");
				props.put(SaslConfigs.SASL_JAAS_CONFIG, sasl_jaas_config);

				props.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
						"io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler");

				// Serialization / Deserialization
				props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
						"org.apache.kafka.common.serialization.StringDeserializer");
				props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
						"org.apache.kafka.common.serialization.StringDeserializer");

				// Consumption
				props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
				props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

				// TODO: If using a VPC Endpoint to connect to NSP then these settings will need
				// to be applied
				// props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "JKS");
				// props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, JKS_LOCATION);

				consumer = new KafkaConsumer<>(props);
				consumer.subscribe(Collections.singletonList(topicName));

				consumer.poll(Duration.ofMillis(5000));
				consumer.seekToEnd(consumer.assignment());
				consumer.commitSync();

				// System.clearProperty("oauth.scope");

			} else
				responseString = StringOps.append("<dmsv-status>DMSV-ERROR: In EPW Setup.\n", responseString,
						"</dmsv-status>");

			return responseString;

		} catch (Exception e) {
			LOGGER.error("{}...Exception Consumer...\n {}", new Object[] { name, LogStackTrace.get(e) });
			return "<dmsv-status>DMSV-ERROR: In consumer creation</dmsv-status>";
		}
		// }
	}

}
