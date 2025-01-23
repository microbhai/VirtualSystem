package vs.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.logging.log4j.LogManager;

import org.apache.logging.log4j.Logger;

import akhil.DataUnlimited.util.LogStackTrace;
import vs.util.StringOps;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaMessageProducer {

	private Producer<String, String> producer;

	public void close() {
		producer.close();
	}

	static int suffix = 0;

	private static synchronized int getSuffix() {
		return suffix++;
	}

	public RecordMetadata send(ProducerRecord<String, String> record) {

		try {
			return producer.send(record).get();
		} catch (InterruptedException e) {
			String msg = StringOps.append(this.brokerUrl, ":", this.oauthClientId, ":", this.oauthClientSecret, ":",
					this.oauthTokenUrl, ":", this.compression, "\n");
			LOGGER.error(StringOps.append("DMSV-ERROR: InterruptedException in posting message:", msg, LogStackTrace.get(e),
					"\n"));
			return null;
		} catch (ExecutionException e) {
			String msg = StringOps.append(this.brokerUrl, ":", this.oauthClientId, ":", this.oauthClientSecret, ":",
					this.oauthTokenUrl, ":" + this.compression, "\n");
			LOGGER.error(
					StringOps.append("DMSV-ERROR: ExecutionException in posting message:", msg, LogStackTrace.get(e), "\n"));
			return null;
		}
	}

	private String brokerUrl;
	private String oauthTokenUrl;
	private String oauthClientId;
	private String oauthClientSecret;
	private String compression;

	public KafkaMessageProducer(String brokerUrl, String oauthTokenUrl, String oauthClientId, String oauthClientSecret,
			String oauthScope, String compression) {
		this.brokerUrl = brokerUrl;
		this.oauthTokenUrl = oauthTokenUrl;
		this.oauthClientId = oauthClientId;
		this.oauthClientSecret = oauthClientSecret;
		this.compression = compression;

		// System.setProperty("oauth.token.endpoint.uri", oauthTokenUrl);
		// System.setProperty("oauth.client.id", oauthClientId);
		// System.setProperty("oauth.client.secret", oauthClientSecret);

		String sasl_jaas_config = StringOps.append(
				"org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required",
				" oauth.token.endpoint.uri=\"", oauthTokenUrl, "\" oauth.client.id=\"", oauthClientId,
				"\" oauth.client.secret=\"", oauthClientSecret, "\";");

		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, oauthClientId + "_" + getSuffix());
		if (getSuffix() > 1000000)
			suffix = 0;
		props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "" + 1024 * 1024 * 10);

		props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
		props.put(SaslConfigs.SASL_MECHANISM, "OAUTHBEARER");
		// props.put(SaslConfigs.SASL_JAAS_CONFIG,
		// "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule
		// required;");
		props.put(SaslConfigs.SASL_JAAS_CONFIG, sasl_jaas_config);
		props.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
				"io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler");

		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");

		if (!compression.equals("none"))
			props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression);

		producer = new KafkaProducer<>(props);

	}

	private static Logger LOGGER = LogManager.getLogger((KafkaMessageProducer.class).getName());

}
