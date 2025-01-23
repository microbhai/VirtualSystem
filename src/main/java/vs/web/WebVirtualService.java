package vs.web;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import akhil.DataUnlimited.DataUnlimitedApi;
import akhil.DataUnlimited.util.LogStackTrace;
import vs.kafka.KafkaCallProcessor;
import vs.kafka.KafkaSVMap;
import vs.util.EPW;
import vs.util.SQLite;
import vs.util.StringOps;

public class WebVirtualService {
	private final static Logger LOGGER = LogManager.getLogger(WebVirtualService.class.getName());

	private AtomicLong reqServed = new AtomicLong(0);
	private AtomicBoolean locked = new AtomicBoolean();
	private String lockKey;
	private String key;
	private boolean isPostEnabled;
	private boolean isRequestLogEnabled;
	private boolean isResponseLogEnabled;
	private boolean isPostResponseLogEnabled;
	private boolean isWaitFirst = false;
	private static String slash = "DMS-SLASH";

	public boolean getLockedStatus() {
		return locked.get();
	}

	public WebVirtualService(String key, String enablePost, String enableRequestLog, String enableResponseLog,
			String enablePostResponseLog) {
		this.key = key;
		this.isPostEnabled = enablePost.equalsIgnoreCase("true");
		this.isRequestLogEnabled = enableRequestLog.equalsIgnoreCase("true");
		this.isResponseLogEnabled = enableResponseLog.equalsIgnoreCase("true");
		this.isPostResponseLogEnabled = enablePostResponseLog.equalsIgnoreCase("true");
		start();
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

	public void setReqServed(String reqserved) {
		this.reqServed.set(Long.parseLong(reqserved));
	}

	public String getCount() {
		return reqServed.toString();
	}

	public String lockService() {
		locked = new AtomicBoolean(true);
		return setLockKey();
	}

	private Map<String, String> hmDmsvScript = new HashMap<>();
	private Map<String, List<EPW>> hmEPW = new HashMap<>();

	private String responseCode = "not set";

	public void setResponseCode(String responseCode) {
		this.responseCode = responseCode;
	}

	public String getResponseCode() {
		return this.responseCode;
	}

	public String getResponseCodes() {
		return String.join(",", hmDmsvScript.keySet());
	}

	public Set<String> getResponseCodeAsSet() {
		return hmDmsvScript.keySet();
	}

	private String delim = "+";

	private ExecutorService es;
	private ScheduledExecutorService esx;
	private ExecutorService esfw;
	private ExecutorService esrfw;

	public void incCount() {
		reqServed.incrementAndGet();
	}

	private void updateCount() {
		String query = "update VS_URL_DMSSCRIPT set REQSERVED = ? where VS_KEY like ?";
		SQLite.getInstance().dmlQuery(query, String.valueOf(reqServed),
				StringOps.append(key, delim, responseCode, "%"));
	}

	public void deleteResponseCode(String rco) {
		hmEPW.remove(rco);
		hmDmsvScript.remove(rco);

		if (responseCode.equals(rco)) {
			if (!hmDmsvScript.isEmpty())
				responseCode = hmDmsvScript.keySet().stream().findFirst().get();
			else
				responseCode = "not set";
		}
	}

	public void processAsync(String request, String keyUT) {

		request = request.trim();
		keyUT = keyUT.replace(slash, "_");
		if (!request.isEmpty()) {

			String key = StringOps.append(keyUT, delim, responseCode);

			final String requestx = request;
			Runnable r = () -> {

				String id = UUID.randomUUID().toString();
				String requestVar = null;
				// if (requestx.startsWith("{") && requestx.endsWith("}") &&
				// FormatConversion.isJSONValid(requestx))
				// requestVar = FormatConversion.jsonToXML(requestx);
				// else
				requestVar = requestx;

				String filedir = StringOps.append(WebSVMap.getLogDirRequests(), File.separator,
						new SimpleDateFormat("yyyy-MM-dd").format(new Date()), File.separator, key);
				String filename = StringOps.append(id, ".txt");

				akhil.DataUnlimited.util.FileOperation.writeFile(filedir, filename, requestVar, false);
				final String filenamex = StringOps.append(filedir, File.separator, filename);

				final String keyx = key;
				Runnable r1 = () -> processEPWs(hmEPW.get(responseCode), id, filenamex, keyx, 1);

				es.submit(r1);

			};
			esrfw.submit(r);
		}
	}

	public void stop() {
		updateCount();
		shutdown();
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
		}
		shutdownNow();
	}

	public void start() {
		es = Executors.newCachedThreadPool();
		esfw = Executors.newFixedThreadPool(10);
		esrfw = Executors.newFixedThreadPool(10);
		esx = Executors.newScheduledThreadPool(1);
		Runnable r = () -> updateCount();
		esx.scheduleAtFixedRate(r, 10000, 10000, TimeUnit.MILLISECONDS);

	}

	public void shutdown() {
		es.shutdown();
		esx.shutdown();
		esfw.shutdown();
		esrfw.shutdown();
	}

	public void shutdownNow() {
		es.shutdownNow();
		esx.shutdownNow();
		esrfw.shutdownNow();
		esfw.shutdownNow();

	}

	private String getRequestData(String filename, String id) {
		return StringOps.append(akhil.DataUnlimited.util.FileOperation.getContentAsString(filename, "UTF-8"),
				"\n<DMSV-MESSAGE-ID>", id, "</DMSV-MESSAGE-ID>");
	}

	private void addResponseToRequestPayload(String filename, String id, String postResponse, int count) {
		String origRequest = getRequestData(filename, id);

		StringBuilder sb = new StringBuilder();
		sb.append(origRequest);
		sb.append("\n<epw-");
		sb.append(count);
		sb.append(">\n");
		sb.append(postResponse);
		sb.append("\n</epw-");
		sb.append(count);
		sb.append(">");

		String data = sb.toString();

		final String requestx = data;

		akhil.DataUnlimited.util.FileOperation.writeFile("", filename, requestx, false);

	}

	public void waitFunc(long wait, String id, String filename, String serviceName, int epWcounter, boolean isWaitFirst,
			int epwCount) {
		String epwCounter = String.valueOf(epWcounter);
		String epwCountString = String.valueOf(epwCount);
		String isWaitFirstString = String.valueOf(isWaitFirst);

		WebSVMap.addWaitingMessageId(id, filename, serviceName, epwCounter, isWaitFirstString, epwCountString);

		long wait10Sec = wait / 10000;

		long count = 0;

		while (count < wait10Sec) {
			if (WebSVMap.shouldMessageWait(id))
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
				}
			else
				break;

			count++;
		}

		WebSVMap.removeWaitingMessageId(id);

	}

	public void epwProcessing(String id, String filename, String key, EPW e, int counter) {
		String response = "DMSV-ERROR:";
		String virtualResponse = null;
		virtualResponse = new DataUnlimitedApi().getVirtualResponse(getRequestData(filename, id), e.getDmsScript(), "", true,
				true, String.valueOf(counter));

		if (virtualResponse != null && !virtualResponse.isEmpty())
			response = virtualResponse;

		if (response.trim().isEmpty())
			return;
		else if (!response.startsWith("DMSV-ERROR:")) {
			if (e.isGVF()) {
				new DataUnlimitedApi().createGlobalVirtualFileParam(response, true);
				LOGGER.info("{}...{}... Processing complete.", new Object[] { id, key });
			} else {

				List<String> postResponseA = new ArrayList<>();

				if (isResponseLogEnabled) {
					writeFileAsync(
							StringOps.append(WebSVMap.getLogDirResponses(), File.separator,
									new SimpleDateFormat("yyyy-MM-dd").format(new Date()), File.separator, key),
							StringOps.append(id, new SimpleDateFormat("HH-mm-ss-SSS").format(new Date()), ".txt"),
							response.trim(), false, null);
				}
				if (isPostEnabled) {
					for (int i = 0; i < e.getPubSize(); i++) {
						if (e.getPubType(i).equalsIgnoreCase("API")) {
							postResponseA
									.add(ApiCallProcessor.process(e.getPubName(i), null, null, null, response, false));
						} else if (e.getPubType(i).equalsIgnoreCase("KAFKA")) {
							postResponseA.add(KafkaCallProcessor.process(e.getPubName(i), response));
						} else {

						}
					}
				}
				for (String postResponse : postResponseA) {
					if (postResponse != null) {
						if (postResponse.contains("DMSV-ERROR: Schema validation failed")) {
							LOGGER.error("{}... DMSV-ERROR: Virtual Response Schema Validation Failed", new Object[] { id });
							writeFileAsync(
									StringOps.append(KafkaSVMap.getLogDirSchema(), File.separator,
											new SimpleDateFormat("yyyy-MM-dd").format(new Date()), File.separator, key),
									StringOps.append(id, new SimpleDateFormat("HH-mm-ss-SSS").format(new Date()),
											".txt"),
									response.trim(), false, null);
						} else if (postResponse.contains("DMSV-ERROR: No API Call Details found for name")) {
							LOGGER.error("{}... DMSV-ERROR: No API Call Details found for name", new Object[] { id });
						} else if (postResponse.contains("DMSV-ERROR: Message post result to Kafka is null")) {
							LOGGER.error("{}... DMSV-ERROR: Kafka post failed", new Object[] { id });
						} else if (postResponse.contains("DMSV-ERROR: oauth creds not found")) {
							LOGGER.error("{}... DMSV-ERROR: Kafka oauth creds not found failed", new Object[] { id });
						} else if (postResponse.contains("Message posted to Kafka")) {
							WebSVMap.incCount(key);
						} else {
							WebSVMap.incCount(key);
							addResponseToRequestPayload(filename, id, postResponse, counter);

							if (isPostResponseLogEnabled && postResponse.length() > 0)
								writeFileAsync(StringOps.append(KafkaSVMap.getLogDirPostHttp(), File.separator,
										new SimpleDateFormat("yyyy-MM-dd").format(new Date()), File.separator, key),
										StringOps.append(id, new SimpleDateFormat("HH-mm-ss-SSS").format(new Date()),
												".txt"),
										postResponse.trim(), false, null);
							LOGGER.info("ID {}... Name {}...EPW count {} Message posted.",
									new Object[] { id, key, counter });
						}
					} else
						LOGGER.info("{}... INFO: Message not posted. Posting is disabled.", new Object[] { id });
				}
			}

		} else {
			LOGGER.error("{}... DMSV-ERROR: Virtual Response Not created due to response being null or empty: {}",
					new Object[] { id, virtualResponse });
		}
		response = null;
	}

	protected void processEPWs(List<EPW> epws, String id, String filename, String key, int epwCount) {

		int counter = 0;
		for (EPW e : epws) {

			counter++;
			if (counter < epwCount)
				continue;

			if (isWaitFirst)
				waitFunc(e.getWait(), id, filename, key, counter, isWaitFirst, epwCount);
			try {
				WebSVMap.getSemaphore().acquire();
				epwProcessing(id, filename, key, e, counter);
			} catch (Exception ex) {
				LOGGER.error("{}... {}...\n Error in processing EPWs: {}\n",
						new Object[] { id, key, LogStackTrace.get(ex) });
			} finally {
				WebSVMap.getSemaphore().release();
			}
			if (!isWaitFirst)
				waitFunc(e.getWait(), id, filename, key, counter, isWaitFirst, epwCount);
		}
	}

	private void writeFileAsync(String filepath, String filename, String towrite, boolean append, Object lock) {
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

	public void enablePost(boolean b) {

		isPostEnabled = b;
	}

	public void enableRequestLog(boolean b) {

		isRequestLogEnabled = b;
	}

	public void enableResponseLog(boolean b) {

		isResponseLogEnabled = b;
	}

	public void enablePostResponseLog(boolean b) {

		isPostResponseLogEnabled = b;
	}

	public List<EPW> getEPWsList(String rco) {
		return hmEPW.get(rco);
	}

	public String getEPWs(String rco) {
		StringBuilder sb = new StringBuilder();

		if (rco == null)
			rco = responseCode;

		for (EPW e : hmEPW.get(rco)) {
			StringBuilder pub = new StringBuilder();
			for (int i = 0; i < e.getPubSize(); i++) {
				pub.append(StringOps.append("<pub><pub-type>", e.getPubType(i), "</pub-type><pub-name>",
						e.getPubName(i), "</pub-name></pub>\n"));
			}
			sb.append(StringOps.append("<isWaitFirst>", String.valueOf(isWaitFirst), "</isWaitFirst>\n",
					"<epw>\n<dmsv-script>", e.getDmsScriptSetting(), "</dmsv-script>\n", pub.toString(), "\n<wait>",
					String.valueOf(e.getWait()), "</wait>\n</epw>\n"));
		}
		sb.append(StringOps.append("<postEnabled>", String.valueOf(isPostEnabled), "</postEnabled>\n<requestLog>",
				String.valueOf(isRequestLogEnabled), "</requestLog>\n<responseLog>",
				String.valueOf(isResponseLogEnabled), "</responseLog>\n<postResponseLog>",
				String.valueOf(isPostResponseLogEnabled), "</postResponseLog>\n"));
		return sb.toString();
	}

	public String setEPWs(String rco, String epws) {

		List<EPW> epwList = new ArrayList<>();

		long wait = 0;

		try {
			List<String> waitFirstL = StringOps.getInBetweenFast(epws, "<isWaitFirst>", "</isWaitFirst>", true, true);
			if (!waitFirstL.isEmpty() && waitFirstL.get(0).equalsIgnoreCase("TRUE"))
				this.isWaitFirst = true;

			List<String> epwsList = StringOps.getInBetweenFast(epws, "<epw>", "</epw>", true, true);
			boolean flag = false;
			if (!epwsList.isEmpty()) {
				for (String f : epwsList) {

					List<String> checkContent = StringOps.getInBetweenFast(f, "<dms-script-content>",
							"</dms-script-content>", true, true);
					List<String> checkTemplate = StringOps.getInBetweenFast(f, "<dms-script-template>",
							"</dms-script-template>", true, true);

					if (checkContent.isEmpty() && checkTemplate.isEmpty()) {
						LOGGER.error(
								"DMSV-ERROR: Improper or missing <dms-script-content>/<dms-script-template> ...\n {} \n ",
								new Object[] { f });
						flag = true;
					}

					String dmsScript;
					boolean isDmsScriptTemplate = checkContent.isEmpty();
					if (checkTemplate.isEmpty())
						dmsScript = checkContent.get(0).trim();
					else
						dmsScript = checkTemplate.get(0).trim();

					List<String> check = StringOps.getInBetweenFast(f, "<pub>", "</pub>", true, true);
					if (check.isEmpty()) {
						LOGGER.error("No <pub> content/script found in EPW ...\n {} \n ", new Object[] { f });
						flag = true;
					}
					List<String> pubConfig = check;

					check = StringOps.getInBetweenFast(f, "<wait>", "</wait>", true, true);
					if (check.isEmpty()) {
						LOGGER.error("No <wait> found in EPW ...\n {} \n ", new Object[] { f });
						flag = true;
					}
					wait = Long.valueOf(check.get(0).trim());

					epwList.add(new EPW(dmsScript, pubConfig, wait, isDmsScriptTemplate));
				}

				hmEPW.put(rco, epwList);
				if (flag)
					return "ERROR";
				else
					return "SUCCESS";
			} else
				return "DMSV-ERROR: EPWs not found <epw></epw>";

		} catch (Exception e) {
			LOGGER.error(StringOps.append("<dmsv-status>DMSV-ERROR: EPW setup failed due to exception.\n",
					LogStackTrace.get(e), "</dmsv-status>"));
			return "DMSV-ERROR: EPW setup failed. Check logs...";
		}
	}

	public String getDMSScriptTemplate(String rco) {

		if (rco == null)
			return hmDmsvScript.get(responseCode);
		else
			return hmDmsvScript.get(rco);

	}

	public void setDMSScriptTemplate(String rco, String dmsvScriptTemplate) {
		hmDmsvScript.put(rco, dmsvScriptTemplate);
	}

	public List<EPW> getEPWList(String rco) {

		return hmEPW.get(rco);

	}

	public void updateRCode(String rco) {
		this.responseCode = rco;
	}

	public String getRCode() {
		return responseCode;
	}

}
