package vs.kafka;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import akhil.DataUnlimited.util.FileOperation;
import vs.util.OauthCredentials;
import vs.util.OauthIDSecret;
import vs.util.SQLite;
import vs.util.SchemaValidator;
import vs.util.StringOps;
import vs.web.WebSVMap;

public class KafkaCallProcessor {

	// private static final Logger LOGGER =
	// LogManager.getLogger(KafkaCallProcessor.class.getName());

	private List<String> responses = new ArrayList<>();
	private static ExecutorService esfw = Executors.newFixedThreadPool(5);
	private ExecutorService es;
	private static Object lock = new Object();
	// public static Object lockSystemProperty = new Object();

	public String getResponse() {
		String resp;
		synchronized (responses) {
			resp = String.join("</message-id><message-id>", responses);
			responses.clear();
		}
		if (resp != null)
			return StringOps.append("<message-id>",resp,"</message-id>");
		else
			return "";
	}
	public void remove(String processorId)
	{
		FileOperation.deleteFile(StringOps.append(WebSVMap.getTempDataGenDir() , File.separator ,  processorId), "");
	}
	public String getResponse(String messageId, String processorId) {
		String filename = StringOps.append(KafkaSVMap.getTempDataGenDir() , File.separator ,  processorId, File.separator , messageId);
		return FileOperation.getFileContentAsString(filename);
	}
	private void addResponse(String r) {
		synchronized (this.responses) {
			responses.add(r);
		}
	}

	private void writeFileAsync(ExecutorService esfw, String filepath, String filename, String towrite,
			boolean append) {
		Runnable r = () -> {
			if (lock != null)
				synchronized (lock) {
					akhil.DataUnlimited.util.FileOperation.writeFile(filepath, filename, towrite, append);
				}
			else
				akhil.DataUnlimited.util.FileOperation.writeFile(filepath, filename, towrite, append);
		};
		esfw.submit(r);
	}

	private static void pace(ArrayList<Long> testTimetaken, double tpm, int thread) {
		double timeAvg = testTimetaken.stream().mapToDouble(d -> d).average().getAsDouble();
		long wait = Math.round((60 / (tpm * 1.03) - timeAvg / 1000) * 1000);
		wait = wait / thread;
		if (wait < 0) {
			wait = 0;
			System.out.println(
					"WARNING: Test may not be able to achieve required transaction per minute. Increase number of users or test transactions should response faster. Average time to run test steps is (ms): "
							+ timeAvg);
		}
		try {
			Thread.sleep(wait);
		} catch (InterruptedException e) {
		}
	}

	public void processDataGen(String kafkaName, String data, boolean isDataFolderPath) {

		ArrayList<Long> testTimetaken = new ArrayList<>();

		String querySelect = "select details from kafkacalldetails where name = ?";
		List<List<String>> result = SQLite.getInstance().selectQuery(querySelect, false, kafkaName);
		if (result.isEmpty())
			addResponse(StringOps.append("DMSV-ERROR: No Kafka Call Details found for name: ", kafkaName));
		else {
			String kafkaDetails = result.get(0).get(0);
			List<String> url = StringOps.getInBetweenFast(kafkaDetails, "<broker-url>", "</broker-url>", true, true);
			List<String> oauthtokenurl = StringOps.getInBetweenFast(kafkaDetails, "<okta-url>", "</okta-url>", true,
					true);
			List<String> compressionL = StringOps.getInBetweenFast(kafkaDetails, "<compression>", "</compression>",
					true, true);
			String compression = "none";
			if (!compressionL.isEmpty())
				compression = compressionL.get(0);
			List<String> topicName = StringOps.getInBetweenFast(kafkaDetails, "<topic>", "</topic>", true, true);
			List<String> oauthcredsname = StringOps.getInBetweenFast(kafkaDetails, "<okta-creds-name>",
					"</okta-creds-name>", true, true);

			List<String> tpms = StringOps.getInBetweenFast(kafkaDetails, "<tpm>", "</tpm>", true, true);

			List<String> threads = StringOps.getInBetweenFast(kafkaDetails, "<threads>", "</threads>", true, true);

			double tpm = 60;
			if (!tpms.isEmpty())
				tpm = Double.parseDouble(tpms.get(0));

			int thread = 1;
			if (!threads.isEmpty())
				thread = Integer.parseInt(threads.get(0));

			if (url.isEmpty())
				addResponse("DMSV-ERROR: Broker URL missing");
			else if (oauthtokenurl.isEmpty())
				addResponse("DMSV-ERROR: Oauth-token-url missing");
			else if (topicName.isEmpty())
				addResponse("DMSV-RROR: Topic name missing");
			else if (oauthcredsname.isEmpty())
				addResponse("DMSV-ERROR: oauth-creds-name name missing");
			else {

				OauthIDSecret os = OauthCredentials.getInstance().getCreds(oauthcredsname.get(0));

				if (os != null) {

					String id = os.getOauthClientID();
					String secret = os.getOauthClientSecret();
					String scope = os.getOauthScope();

					KafkaMessageProducer producer;
					// synchronized (lockSystemProperty) {
					producer = new KafkaMessageProducer(url.get(0), oauthtokenurl.get(0), id, secret, scope,
							compression);
					// }

					if (isDataFolderPath) {
						es = Executors.newFixedThreadPool(thread);
						List<String> files = FileOperation.getListofFiles(data, true, false);
						String responseDir = data;//.replace(KafkaSVMap.getTempDataGenDir(), WebSVMap.getSVTempDir());
						for (String file : files) {
							String payload = FileOperation.getFileContentAsString(file);
							final double tpmx = tpm;
							final int threadx = thread;
							Runnable r = () -> {
								StringBuilder sb = new StringBuilder();
								sb.append("<data>\n");
								sb.append(payload);
								sb.append("\n</data>");
								sb.append("\n<response>");
								ProducerRecord<String, String> record = new ProducerRecord<>(topicName.get(0), payload);
								long st = new Date().getTime();
								RecordMetadata res = producer.send(record);

								if (res != null) {
									sb.append("Data posted to the topic");
								} else
									sb.append("DMSV-ERROR: Message post result to Kafka is null");
								sb.append("\n</response>");
								//String uuid = UUID.randomUUID().toString();
								String filename = file.substring(file.lastIndexOf(File.separator)+1, file.length());
								writeFileAsync(esfw, responseDir, filename, sb.toString(), false);
								addResponse(filename);
								long ed = new Date().getTime();
								testTimetaken.add(ed - st);
								pace(testTimetaken, tpmx, threadx);
							};
							es.submit(r);
							try {
								Thread.sleep(500);
							} catch (InterruptedException e) {
							}
						}
						es.shutdown();
						try {
							es.awaitTermination(3600, TimeUnit.HOURS);
						} catch (InterruptedException e) {
						}
						//FileOperation.deleteFile(data, "txt");

					} else {

						if (data.contains("<data-file>") && data.contains("</data-file>")) {
							es = Executors.newFixedThreadPool(thread);
							List<String> dataFiles = StringOps.getInBetweenFast(data, "<data-file>", "</data-file>",
									true, false);

							for (String file : dataFiles) {

								final double tpmx = tpm;
								final int threadx = thread;
								Runnable r = () -> {

									ProducerRecord<String, String> recordPrd = new ProducerRecord<>(topicName.get(0),
											file);
									long st = new Date().getTime();
									RecordMetadata res = producer.send(recordPrd);
									long ed = new Date().getTime();
									testTimetaken.add(ed - st);
									StringBuilder sb = new StringBuilder();
									sb.append("<data>\n");
									sb.append(file);
									sb.append("\n</data>");
									sb.append("\n<response>");
									if (res != null) {
										sb.append("Data posted to the topic");
									} else
										sb.append("DMSV-ERROR: Message post result to Kafka is null");
									sb.append("\n</response>");
									addResponse(sb.toString());
									pace(testTimetaken, tpmx, threadx);
								};
								es.submit(r);
								try {
									Thread.sleep(500);
								} catch (InterruptedException e) {
								}

							}
							es.shutdown();
							try {
								es.awaitTermination(3600, TimeUnit.HOURS);
							} catch (InterruptedException e) {
							}

						} else {
							ProducerRecord<String, String> record = new ProducerRecord<>(topicName.get(0), data);

							//RecordMetadata res = producer.send(record);
							producer.send(record);
							StringBuilder sb = new StringBuilder();
							sb.append("<data>\n");
							sb.append(data);
							sb.append("\n</data>\n");
							sb.append("<response>Data posted to the topic<response>");
							addResponse(sb.toString());
						}
					}
					producer.close();

				} else
					addResponse("<response>DMSV-ERROR: oauth creds not found</response>");
			}
		}
	}

	public static String process(String kafkaName, String data) {

		String querySelect = "select details from kafkacalldetails where name = ?";
		List<List<String>> result = SQLite.getInstance().selectQuery(querySelect, false, kafkaName);
		if (result.isEmpty())
			return "DMSV-ERROR: No Kafka Call Details found for name: " + kafkaName;
		else {
			String kafkaDetails = result.get(0).get(0);
			String url = StringOps.getInBetweenFast(kafkaDetails, "<broker-url>", "</broker-url>", true, false).get(0);
			String oauthtokenurl = StringOps.getInBetweenFast(kafkaDetails, "<okta-url>", "</okta-url>", true, false)
					.get(0);
			List<String> compressionL = StringOps.getInBetweenFast(kafkaDetails, "<compression>", "</compression>",
					true, false);
			String compression = "none";
			if (compressionL != null)
				compression = compressionL.get(0);
			String topicName = StringOps.getInBetweenFast(kafkaDetails, "<topic>", "</topic>", true, false).get(0);
			String oauthcredsname = StringOps
					.getInBetweenFast(kafkaDetails, "<okta-creds-name>", "</okta-creds-name>", true, false).get(0);

			List<String> schema = StringOps.getInBetweenFast(kafkaDetails, "<dmsv-response-schema>",
					"</dmsv-response-schema>", true, true);
			String schemaStr = null;
			if (!schema.isEmpty()) {
				schemaStr = schema.get(0).trim();
			}

			boolean isValidated = SchemaValidator.validate(data, schemaStr, false);
			if (isValidated) {
				OauthIDSecret os = OauthCredentials.getInstance().getCreds(oauthcredsname);

				if (os != null) {

					String id = os.getOauthClientID();
					String secret = os.getOauthClientSecret();
					String scope = os.getOauthScope();

					KafkaMessageProducer producer;
					// synchronized (lockSystemProperty) {
					producer = new KafkaMessageProducer(url, oauthtokenurl, id, secret, scope, compression);
					// }

					StringBuilder sb = new StringBuilder();

					ProducerRecord<String, String> recordPrd = new ProducerRecord<>(topicName, data);
					RecordMetadata res = producer.send(recordPrd);
					if (res != null) {
						sb.append("Message posted to Kafka: partition,offset = ");
						sb.append(res.partition());
						sb.append(",");
						sb.append(res.offset());
					} else
						sb.append("DMSV-ERROR: Message post result to Kafka is null\n");

					producer.close();
					return sb.toString();
				} else
					return "DMSV-ERROR: oauth creds not found";
			} else
				return "DMSV-ERROR: Schema validation failed";
		}
	}

	public static String deleteKafkaCall(String name) {
		String queryDelete = "delete from kafkacalldetails where name = ?";
		Integer i = SQLite.getInstance().dmlQuery(queryDelete, name);
		if (i != null) {
			return StringOps.append("DMSV-SUCCESS: Deleted ", String.valueOf(i), " rows...");
		} else {
			return "DMSV-ERROR: in delete process... check logs";
		}
	}

	public static String getKafkaSearchResults(String tname, String searchtype) {

		String query;
		List<List<String>> qr;
		if (tname != null) {
			if (searchtype != null && searchtype.equals("name")) {
				query = "select name, description from kafkacalldetails where name = ?";
				qr = SQLite.getInstance().selectQuery(query, false, tname);
			} else if (searchtype != null && searchtype.equals("namepartial")) {
				query = "select name, description from kafkacalldetails where name like ? order by name";
				qr = SQLite.getInstance().selectQuery(query, false, "%" + tname + "%");
			} else if (searchtype != null && searchtype.equals("namedescriptionpartial")) {
				query = "select name, description from kafkacalldetails where name like ? or DESCRIPTION like ? order by name";
				qr = SQLite.getInstance().selectQuery(query, false, "%" + tname + "%", "%" + tname + "%");
			} else if (searchtype != null && searchtype.equals("descriptionpartial")) {
				query = "select name, description from kafkacalldetails where DESCRIPTION like ? order by name";
				qr = SQLite.getInstance().selectQuery(query, false, "%" + tname + "%");
			} else {
				query = "select name, description, details from kafkacalldetails where name = ?";
				qr = SQLite.getInstance().selectQuery(query, false, tname);
			}
		} else {
			query = "select NAME, description, details from kafkacalldetails order by NAME";
			qr = SQLite.getInstance().selectQuery(query, false);
		}

		StringBuilder sb = new StringBuilder();
		sb.append("<dmsv-kafka-search-results>");

		for (List<String> ls : qr) {
			sb.append("<dmsv-kafka-search-result>");
			sb.append("<dmsv-kafka-search-name>" + ls.get(0) + "</dmsv-kafka-search-name>");
			sb.append("<dmsv-kafka-search-description>" + ls.get(1) + "</dmsv-kafka-search-description>");
			if (ls.size() == 3) {
				sb.append("<dmsv-kafka-search-details>");
				sb.append(ls.get(2));
				sb.append("</dmsv-kafka-search-details>");
			}
			sb.append("</dmsv-kafka-search-result>");

		}
		sb.append("</dmsv-kafka-search-results>");

		return sb.toString();

	}

	public static String updateKafkaCall(String name, String details, String description) {

		Integer i;
		if (description.isEmpty()) {
			String query = "update kafkacalldetails set details = ? where name = ?";
			i = SQLite.getInstance().dmlQuery(query, details, name);
		} else if (details.isEmpty()) {
			String query = "update kafkacalldetails set description = ? where name = ?";
			i = SQLite.getInstance().dmlQuery(query, description, name);
		} else {
			String query = "update kafkacalldetails set details = ?, description = ? where name = ?";
			i = SQLite.getInstance().dmlQuery(query, details, description, name);
		}
		if (i != null) {
			return StringOps.append("DMSV-SUCCESS: Updated ", String.valueOf(i), " rows...");
		} else {
			return "DMSV-ERROR: Failed update... check logs";
		}
	}

	public static String saveKafkaCall(String name, String details, String description) {
		String queryInsert = "insert into kafkacalldetails (name, description, details) values (?, ?, ?)";
		Integer i = SQLite.getInstance().dmlQuery(queryInsert, name, description, details);
		if (i != null) {
			return StringOps.append("DMSV-SUCCESS: Inserted ", String.valueOf(i), " rows...");
		} else {
			return "DMSV-ERROR: Failed insert... check logs";
		}
	}
}
