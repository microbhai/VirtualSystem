package vs.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import vs.util.LogStackTrace;
import vs.util.OauthCredentials;
import vs.util.SQLite;
import vs.util.StringOps;

public class KafkaVirtualService {

	private final Logger LOGGER = LogManager.getLogger(KafkaVirtualService.class.getName());
	private String name;
	private String description;
	private AtomicLong reqServed = new AtomicLong();
	private String sub;
	private String epws;
	private KafkaMessageConsumer consumer;
	private List<String> status = new ArrayList<>();
	private AtomicBoolean locked = new AtomicBoolean();
	private ScheduledExecutorService es;
	private String lockKey;
	private boolean isEnablePost;
	private boolean isEnableRequestLog;
	private boolean isEnableResponseLog;
	private boolean isEnablePostResponseLog;

	public boolean getEnablePost() {
		return isEnablePost;
	}

	public boolean getEnableRequestLog() {
		return isEnableRequestLog;
	}

	public boolean getEnableResponseLog() {
		return isEnableResponseLog;
	}

	public boolean getEnablePostResponseLog() {
		return isEnablePostResponseLog;
	}

	public long incCount() {
		return reqServed.incrementAndGet();
	}

	public String getCount() {
		return reqServed.toString();
	}

	public int getPending() {
		return consumer.getTester().getPending();
	}

	public boolean getLockedStatus() {
		return locked.get();
	}

	public String setLockKey() {
		lockKey = UUID.randomUUID().toString();
		return lockKey;
	}

	public boolean unlockService(String lockKey) {
		if (this.lockKey != null && (this.lockKey.equals(lockKey) || "adminsmasterkey".equals(lockKey))) {
			locked = new AtomicBoolean(false);
			this.lockKey = null;
		}

		return locked.get();

	}

	public String lockService() {
		locked = new AtomicBoolean(true);
		return setLockKey();
	}

	public String getStatus() {
		return status.get(0);
	}

	public String getDescription() {
		return description;
	}

	public synchronized void setStatus(String action, boolean isInitialize) {
		if (!status.isEmpty())
			status.clear();
		status.add(action);
		if (!isInitialize) {
			String query = "update VS_KAFKA_DMSSCRIPT set STATUS = ? where VS_NAME = ?";
			SQLite.getInstance().dmlQuery(query, action, name);
		}
	}

	public synchronized void pause() {
		consumer.getTester().pause(true);
		setStatus("Paused", false);
	}

	public synchronized void unpause() {
		consumer.getTester().pause(false);
		setStatus("Running", false);
	}

	public synchronized void enablePost(boolean b) {
		consumer.getTester().enablePost(b);
		String setting;
		if (b)
			setting = "true";
		else
			setting = "false";
		String query = "update VS_KAFKA_DMSSCRIPT set enablePost = ? where VS_NAME = ?";
		SQLite.getInstance().dmlQuery(query, setting, name);
	}

	public synchronized void enableRequestLog(boolean b) {
		consumer.getTester().enableRequestLog(b);
		String setting;
		if (b)
			setting = "true";
		else
			setting = "false";
		String query = "update VS_KAFKA_DMSSCRIPT set enableRequestLog = ? where VS_NAME = ?";
		SQLite.getInstance().dmlQuery(query, setting, name);
	}

	public synchronized void enableResponseLog(boolean b) {
		consumer.getTester().enableResponseLog(b);
		String setting;
		if (b)
			setting = "true";
		else
			setting = "false";
		String query = "update VS_KAFKA_DMSSCRIPT set enableResponseLog = ? where VS_NAME = ?";
		SQLite.getInstance().dmlQuery(query, setting, name);
	}

	public synchronized void enablePostResponseLog(boolean b) {
		consumer.getTester().enablePostResponseLog(b);
		String setting;
		if (b)
			setting = "true";
		else
			setting = "false";
		String query = "update VS_KAFKA_DMSSCRIPT set enablePostResponseLog = ? where VS_NAME = ?";
		SQLite.getInstance().dmlQuery(query, setting, name);
	}

	public String getEPWs() {
		return consumer.getTester().getEPWs();
	}

	public String getDetails() {
		StringBuilder toreturn = new StringBuilder();
		toreturn.append("<dmsv-name>").append(name).append("</dmsv-name>\n").append("<dmsv-description>")
				.append(description).append("</dmsv-description>\n").append("<dmsv-requestsserved>")
				.append(reqServed.toString()).append("</dmsv-requestsserved>\n").append("<dmsv-locked>")
				.append(locked.get()).append("</dmsv-locked>\n").append("<dmsv-subscriber-detail>").append(sub)
				.append("</dmsv-subscriber-detail>\n").append("<dmsv-epws-detail>\n");
		if (consumer != null)
			toreturn.append(consumer.getTester().getEPWs());
		toreturn.append("\n</dmsv-epws-detail>\n");
		return toreturn.toString();
	}

	public String getFilterValues() {
		return consumer.getTester().getFilterValues();
	}

	public String getPersistentFilterValues() {
		return consumer.getTester().getPersistentFilterValues();
	}

	public String getPersistentFilterValuesOR() {
		return consumer.getTester().getPersistentFilterValuesOR();
	}

	public String getUsedFilterValues() {
		return consumer.getTester().getUsedFilterValues();
	}

	public synchronized void clearFilter(String type, String filterValue) {
		consumer.getTester().clearFilter(type, filterValue);
	}

	public synchronized void clearFilter(String type) {
		consumer.getTester().clearFilter(type);
	}

	public synchronized void addFilter(String value, String type) {
		consumer.getTester().setFilterValue(StringOps.fastSplit(value, "<DMSDELIM>"), type);
	}

	private void updateCount() {
		if (getStatus().equals("Running")) {
			String query = "update VS_KAFKA_DMSSCRIPT set REQSERVED = ? where VS_NAME = ?";
			SQLite.getInstance().dmlQuery(query, KafkaSVMap.getCount(name), name);
		}
	}

	public synchronized String startService(boolean isEnablePost, boolean isEnableRequestLog,
			boolean isEnableResponseLog, boolean isEnablePostResponseLog) {
		es = Executors.newScheduledThreadPool(1);
		Runnable r = () -> {
			updateCount();
		};
		es.scheduleAtFixedRate(r, 300000, 10000, TimeUnit.MILLISECONDS);

		return startConsumer(this.name, this.sub, this.epws);
	}

	public String setMandatoryFilter(String setMandatory, String type) {
		if (setMandatory.trim().equalsIgnoreCase("true"))
			return consumer.getTester().setMandatoryFilter(true, type);
		else
			return consumer.getTester().setMandatoryFilter(false, type);
	}

	public synchronized String startService(String name, String description, String sub, String epws, String reqserved,
			String status, String enablePost, String enableRequestLog, String enableResponseLog,
			String enablePostResponseLog) {

		this.name = name;
		this.description = description;
		this.sub = sub;
		this.epws = epws;
		this.reqServed.set(Long.parseLong(reqserved));
		setStatus(status, true);
		this.isEnablePost = enablePost.equalsIgnoreCase("true");
		this.isEnableRequestLog = enableRequestLog.equalsIgnoreCase("true");
		this.isEnableResponseLog = enableResponseLog.equalsIgnoreCase("true");
		this.isEnablePostResponseLog = enablePostResponseLog.equalsIgnoreCase("true");

		if (getStatus().equals("Stopped")) {
			LOGGER.info("Service {} is stopped...", name);
			return StringOps.append("<dmsv-status>DMSV-SUCCESS: Service ", name,
					" created with stopped status</dmsv-status>");
		} else {
			LOGGER.info("Starting service ...{}", name);
			return startService(this.isEnablePost, this.isEnableRequestLog, this.isEnableResponseLog,
					this.isEnablePostResponseLog);
		}
	}

	public synchronized String startService() {
		setStatus("Paused", false);
		return startService(this.isEnablePost, this.isEnableRequestLog, this.isEnableResponseLog,
				this.isEnablePostResponseLog);
	}

	public synchronized void interrupt(boolean isContextDestroyed) {
		updateCount();
		if (!isContextDestroyed)
			setStatus("Stopped", false);

		if (consumer != null)
			consumer.close();
		consumer = null;
		if (es != null) {
			es.shutdown();
			es.shutdownNow();
		}

	}

	public synchronized String startConsumer(String name, String sub, String epws) {

		String credsName = "oauth";

		List<String> subType = StringOps.getInBetweenFast(sub, "<sub-type>", "</sub-type>", true, true);
		List<String> subName = StringOps.getInBetweenFast(sub, "<sub-name>", "</sub-name>", true, true);

		if (subType.isEmpty() && subName.isEmpty()) {
			return StringOps.append("DMSV-ERROR: Subscriber name and type missing for name: ", subName.get(0));
		} else {
			if (subType.get(0).equals("KAFKA")) {
				String querySelect = "select details from kafkacalldetails where name = ?";
				List<List<String>> result = SQLite.getInstance().selectQuery(querySelect, false, subName.get(0));
				if (result.isEmpty())
					return StringOps.append("DMSV-ERROR: No Kafka Call Details found for name: ", subName.get(0));
				else {
					sub = result.get(0).get(0);
					final List<String> brokerUrlList = StringOps.getInBetweenFast(sub, "<broker-url>", "</broker-url>",
							true, true);

					final List<String> oauthTokenUrlList = StringOps.getInBetweenFast(sub, "<okta-url>", "</okta-url>",
							true, true);

					final List<String> topicNameList = StringOps.getInBetweenFast(sub, "<topic>", "</topic>", true,
							true);

					final List<String> persistentAndList = StringOps.getInBetweenFast(sub, "<persistent-and>",
							"</persistent-and>", true, true);
					final List<String> persistentOrList = StringOps.getInBetweenFast(sub, "<persistent-or>",
							"</persistent-or>", true, true);

					final List<String> oauthcredsname = StringOps.getInBetweenFast(sub, "<okta-creds-name>",
							"</okta-creds-name>", true, true);

					final List<String> groupId = StringOps.getInBetweenFast(sub, "<group-id>", "</group-id>", true,
							true);

					final List<String> groupIdRandomized = StringOps.getInBetweenFast(sub, "<is-groupid-randomized>",
							"</is-groupid-randomized>", true, true);

					if (!oauthcredsname.isEmpty())
						credsName = oauthcredsname.get(0).trim();

					if (!brokerUrlList.isEmpty() && !oauthTokenUrlList.isEmpty() && !topicNameList.isEmpty()) {

						try {
							String persistentAnd = "";
							List<String> persistentOr = null;
							if (!persistentAndList.isEmpty())
								persistentAnd = persistentAndList.get(0).trim();
							if (!persistentOrList.isEmpty())
								persistentOr = persistentOrList;
							consumer = new KafkaMessageConsumer();
							consumer.setKafkaMessageConsumer(name, epws, brokerUrlList.get(0).trim(),
									oauthTokenUrlList.get(0).trim(),
									OauthCredentials.getInstance().getCreds(credsName).getOauthClientID(),
									OauthCredentials.getInstance().getCreds(credsName).getOauthClientSecret(),
									OauthCredentials.getInstance().getCreds(credsName).getOauthScope(),
									topicNameList.get(0).trim(), groupId.get(0).trim(),
									Boolean.parseBoolean(groupIdRandomized.get(0).trim()), isEnablePost,
									isEnableRequestLog, isEnableResponseLog, isEnablePostResponseLog, getStatus());
							consumer.getTester().setFilterValue(StringOps.fastSplit(persistentAnd, "<DMSDELIM>"),
									"persistent-and");
							if (persistentOr != null)
								for (String orFilter : persistentOr) {
									consumer.getTester().setFilterValue(StringOps.fastSplit(orFilter, "<DMSDELIM>"),
											"persistent-or");
								}
							consumer.start();

							return "SUCCESS";
						} catch (Exception e) {
							LOGGER.error("DMSV-ERROR: Starting service ...{}\n{}",
									new Object[] { name, LogStackTrace.get(e) });
							return StringOps.append("<dmsv-status>DMSV-ERROR: Starting service. Check logs.", name,
									"</dmsv-status>");
						}
					} else {
						return StringOps.append("<dmsv-status>DMSV-ERROR: Subscriber details missing for ", name, "\n", sub,
								"\n</dmsv-status>");
					}
				}
			} else
				return StringOps.append("<dmsv-status>DMSV-ERROR: Only subscriber supported is of type KAFKA...",
						subType.get(0), " is not supported.</dmsv-status>");
		}
	}
}