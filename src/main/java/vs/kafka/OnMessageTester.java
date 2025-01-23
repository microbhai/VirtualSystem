package vs.kafka;

import vs.util.EPW;
import vs.util.StringOps;
import vs.web.ApiCallProcessor;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import akhil.DataUnlimited.DataUnlimitedApi;
import akhil.DataUnlimited.util.FormatConversion;
import akhil.DataUnlimited.util.LogStackTrace;

public class OnMessageTester {

	private List<EPW> epws = new ArrayList<>();
	private List<String> filterValue = new ArrayList<>();
	private List<String> persistentFilterValue = new ArrayList<>();
	private List<List<String>> persistentFilterValueOR = new ArrayList<>();
	private String name;
	private boolean isPaused;
	private static final Logger LOGGER = LogManager.getLogger(OnMessageTester.class.getName());
	private List<String> filterStore = new ArrayList<>();
	private boolean isPostEnabled;
	private boolean isRequestLogEnabled;
	private boolean isResponseLogEnabled;
	private boolean isPostResponseLogEnabled;
	private ExecutorService es = Executors.newCachedThreadPool();
	private ExecutorService esfw = Executors.newFixedThreadPool(10);
	private ExecutorService esrfw = Executors.newFixedThreadPool(10);
	private boolean isPersistentOrMandatory = false;
	private boolean isOneTimeMandatory = false;
	private boolean isWaitFirst = false;
	private AtomicInteger pending = new AtomicInteger();

	public int getPending() {
		return pending.get();
	}

	public void setZeroWait() {
		for (EPW e : epws)
			e.setZeroWait();
	}

	public String setMandatoryFilter(boolean b, String filterType) {
		if (filterType.trim().equals("one-time")) {
			this.isOneTimeMandatory = b;
			return "DMSV-SUCCESS: one-time filter mandatory status set to:" + b;
		} else if (filterType.trim().equals("persistent-or")) {
			this.isPersistentOrMandatory = b;
			return "DMSV-SUCCESS: persistent-or filter mandatory status set to:" + b;
		} else if (filterType.trim().equals("persistent-and")) {
			return "DMSV-ERROR: persistent-and filter mandatory status can't be changed";
		} else {
			return "DMSV-ERROR: Improper filter type entered";
		}
	}

	public String getEPWs() {
		StringBuilder sb = new StringBuilder();
		for (EPW e : epws) {
			StringBuilder pub = new StringBuilder();
			for (int i = 0; i < e.getPubSize(); i++) {
				pub.append(StringOps.append("<pub><pub-type>", e.getPubType(i), "</pub-type><pub-name>",
						e.getPubName(i), "</pub-name></pub>\n"));
			}
			sb.append(StringOps.append("<epw>\n<dmsv-script>", e.getDmsScriptSetting(), "</dmsv-script>\n",
					pub.toString(), "<wait>", String.valueOf(e.getWait()), "</wait>\n</epw>\n"));
		}
		sb.append(StringOps.append("<isWaitFirst>", String.valueOf(isWaitFirst), "</isWaitFirst>\n<isPaused>",
				String.valueOf(isPaused), "</isPaused>\n<postEnabled>", String.valueOf(isPostEnabled),
				"</postEnabled>\n<requestLog>", String.valueOf(isRequestLogEnabled), "</requestLog>\n<responseLog>",
				String.valueOf(isResponseLogEnabled), "</responseLog>\n<postResponseLog>",
				String.valueOf(isPostResponseLogEnabled), "</postResponseLog>\n<filters>\n<one-time>",
				getFilterValues(), "</one-time>\n<persistent-and>", getPersistentFilterValues(),
				"</persistent-and>\n<persistent-or>", getPersistentFilterValuesOR(), "</persistent-or>\n<used>",
				getUsedFilterValues(), "</used>\n</filters>\n<isOneTimeMandatory>", String.valueOf(isOneTimeMandatory),
				"</isOneTimeMandatory>\n<isPersistentOrMandatory>", String.valueOf(isPersistentOrMandatory),
				"</isPersistentOrMandatory>\n"));
		return sb.toString();
	}

	public void shutdown() {
		es.shutdown();
		esfw.shutdown();
		esrfw.shutdown();
	}

	public void shutdownNow() {
		es.shutdownNow();
		esfw.shutdownNow();
		esrfw.shutdownNow();
	}

	public void pause(boolean b) {
		isPaused = b;
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

	public String getFilterValues() {
		return String.join("<DMSDELIM>", filterValue);
	}

	public String getPersistentFilterValues() {
		return String.join("<DMSDELIM>", persistentFilterValue);
	}

	public String getPersistentFilterValuesOR() {
		StringBuilder sb = new StringBuilder();
		for (List<String> as : persistentFilterValueOR) {
			sb.append("<persistent-or>").append(String.join("<DMSDELIM>", as)).append("</persistent-or>");
		}
		return sb.toString();
	}

	public String getUsedFilterValues() {
		return String.join("<DMSDELIM>", filterStore);
	}

	public void setFilterValue(List<String> filterValue, String type) {
		synchronized (this.filterValue) {
			if (type.equals("persistent-and")) {
				this.persistentFilterValue.addAll(filterValue);
				this.persistentFilterValue = new ArrayList<>(new HashSet<>(this.persistentFilterValue));
			} else if (type.equals("persistent-or")) {
				ArrayList<String> filt = new ArrayList<>(new HashSet<>(filterValue));
				this.persistentFilterValueOR.add(filt);
			} else {
				this.filterValue.addAll(filterValue);
				this.filterValue = new ArrayList<>(new HashSet<>(this.filterValue));
			}
		}
	}

	public void clearFilter(String type) {

		synchronized (this.filterValue) {
			if (type.equals("persistent-and")) {
				persistentFilterValue.clear();
			} else if (type.equals("persistent-or")) {
				persistentFilterValueOR.clear();
			} else if (type.equals("one-time")) {
				this.filterValue.clear();
			} else
				filterStore.clear();
			LOGGER.info("clearning filter values...");
		}
	}

	public void clearFilter(String type, String filterValue) {
		synchronized (this.filterValue) {
			if (type.equals("persistent-and")) {
				persistentFilterValue.remove(filterValue);
			} else if (type.equals("persistent-or")) {
				for (List<String> as : persistentFilterValueOR)
					as.remove(filterValue);
			} else if (type.equals("one-time")) {
				this.filterValue.remove(filterValue);
			} else
				filterStore.remove(filterValue);
			LOGGER.info("clearning filter value...{}", filterValue);
		}
	}

	public OnMessageTester(String name) {
		LOGGER.info("Name settingin OnMessageTester...{}", name);
		this.name = name;
	}

	public String setEPWs(String epws) {
		long wait = 0;

		try {
			List<String> waitFirstL = StringOps.getInBetweenFast(epws, "<isWaitFirst>", "</isWaitFirst>", true, true);
			if (!waitFirstL.isEmpty() && waitFirstL.get(0).equalsIgnoreCase("TRUE"))
				this.isWaitFirst = true;

			List<String> epwsList = StringOps.getInBetweenFast(epws, "<epw>", "</epw>", true, true);
			if (epwsList.isEmpty())
				return "<dmsv-status>DMSV-ERROR: EPW Size is Zero</dmsv-status>";

			for (String f : epwsList) {
				List<String> checkContent = StringOps.getInBetweenFast(f, "<dms-script-content>",
						"</dms-script-content>", true, true);
				List<String> checkTemplate = StringOps.getInBetweenFast(f, "<dms-script-template>",
						"</dms-script-template>", true, true);

				if (checkContent.isEmpty() && checkTemplate.isEmpty())
					return "<dmsv-status>DMSV-ERROR: Improper or missing <dms-script-content> </dms-script-content></dmsv-status>";

				String dmsScript;
				boolean isDmsScriptTemplate = checkContent.isEmpty();
				if (checkTemplate.isEmpty())
					dmsScript = checkContent.get(0).trim();
				else
					dmsScript = checkTemplate.get(0).trim();

				List<String> check = StringOps.getInBetweenFast(f, "<pub>", "</pub>", true, true);
				if (check.isEmpty())
					return "<dmsv-status>DMSV-ERROR: Improper or missing <pub> </pub></dmsv-status>";
				List<String> pubConfig = check;

				check = StringOps.getInBetweenFast(f, "<wait>", "</wait>", true, true);
				if (check.isEmpty())
					return "<dmsv-status>DMSV-ERROR: Improper or missing <wait> </wait></dmsv-status>";
				wait = Long.valueOf(check.get(0).trim());

				this.epws.add(new EPW(dmsScript, pubConfig, wait, isDmsScriptTemplate));
			}

			return "SUCCESS";
		} catch (Exception e) {
			return StringOps.append("<dmsv-status>DMSV-ERROR: EPW setup failed due to exception.\n", LogStackTrace.get(e),
					"</dmsv-status>");
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

	private String containedFilterValue(String request) {
		String toReturn = "";
		synchronized (this.filterValue) {
			for (String s : filterValue) {
				if (request.contains(s.trim()))
					toReturn = s.trim();
			}
		}
		return toReturn;
	}

	private boolean containsFilterValue(String request) {
		boolean toReturn = false;
		synchronized (this.filterValue) {
			for (String s : filterValue) {
				if (request.contains(s.trim()))
					toReturn = true;
			}
		}
		return toReturn;
	}

	private boolean containsPersistentORFilterValue(String request) {
		boolean toReturn = true;
		synchronized (this.persistentFilterValueOR) {
			for (List<String> as : persistentFilterValueOR) {
				if (toReturn) {
					boolean eachLineFilterDecision = false;
					for (String s : as) {
						if (request.contains(s.trim())) {
							eachLineFilterDecision = true;
							break;
						}
					}
					if (!eachLineFilterDecision)
						toReturn = eachLineFilterDecision;
				}
			}
		}
		return toReturn;
	}

	private boolean containsPersistentFilterValue(String request) {
		synchronized (persistentFilterValue) {
			if (!persistentFilterValue.isEmpty()) {
				boolean toReturn = true;

				for (String s : persistentFilterValue) {
					if (s.trim().contains("<DMSNOT>")) {
						s = s.trim().replace("<DMSNOT>", "").trim();
						if (request.contains(s)) {
							toReturn = false;
							break;
						}
					} else {
						if (!request.contains(s.trim())) {
							toReturn = false;
							break;
						}
					}
				}

				return toReturn;
			} else
				return true;
		}
	}

	private boolean storeContainsFilterValue() {
		boolean toReturn = false;
		synchronized (this.filterValue) {
			for (String s : filterValue) {
				if (filterStore.contains(s.trim()))
					toReturn = true;
			}
		}
		return toReturn;
	}

	public void processAsync(String request) {
		if (!isPaused) {
			request = request.trim();
			if (!request.isEmpty()) {

				final String requestx = request;
				Runnable r = () -> {
					String id = UUID.randomUUID().toString();
					String requestVar = null;
					if (requestx.startsWith("{") && requestx.endsWith("}") && FormatConversion.isJSONValid(requestx))
						requestVar = FormatConversion.jsonToXML(requestx);
					else
						requestVar = requestx;

					String filedir = StringOps.append(KafkaSVMap.getLogDirRequests(), File.separator,
							new SimpleDateFormat("yyyy-MM-dd").format(new Date()), File.separator, name);
					String filename = StringOps.append(id, ".txt");
					akhil.DataUnlimited.util.FileOperation.writeFile(filedir, filename, requestVar, false);
					final String filenamex = StringOps.append(filedir, File.separator, filename);
					Runnable r1 = () -> {
						onMessage(id, filenamex, 1);

					};
					pending.incrementAndGet();
					es.submit(r1);

				};
				esrfw.submit(r);

			}
		}
	}

	private String getRequestData(String filename, String id) {
		return StringOps.append(akhil.DataUnlimited.util.FileOperation.getContentAsString(filename, "UTF-8"), "\n<DMSV-MESSAGE-ID>", id, "</DMSV-MESSAGE-ID>");
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

		// if (isRequestLogEnabled)
		// writeFileAsync("", filename, requestx, false, null);
	}

	public void epwProcessing(String id, String filename, EPW e, int counter) {
		String response = "ERROR:";
		String virtualResponse = null;
		virtualResponse = new DataUnlimitedApi().getVirtualResponse(getRequestData(filename, id), e.getDmsScript(), "", true, true,
				String.valueOf(counter));

		if (virtualResponse != null && !virtualResponse.isEmpty())
			response = virtualResponse;

		if (response.trim().isEmpty())
			return;
		else if (!response.startsWith("DMSV-ERROR:")) {
			if (e.isGVF()) {
				new DataUnlimitedApi().createGlobalVirtualFileParam(response, true);
				LOGGER.info("{}...{}... Processing complete.", new Object[] { id, name });
			} else {

				List<String> postResponseA = new ArrayList<>();

				if (isResponseLogEnabled) {
					writeFileAsync(
							StringOps.append(KafkaSVMap.getLogDirResponses(), File.separator,
									new SimpleDateFormat("yyyy-MM-dd").format(new Date()), File.separator, name),
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
							writeFileAsync(StringOps.append(KafkaSVMap.getLogDirSchema(), File.separator,
									new SimpleDateFormat("yyyy-MM-dd").format(new Date()), File.separator, name),
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
							KafkaSVMap.incCount(name);
							LOGGER.info("ID {}... Name {}...EPW count {} Message posted to Kafka.",
									new Object[] { id, name, counter });
						} else {
							KafkaSVMap.incCount(name);
							addResponseToRequestPayload(filename, id, postResponse, counter);
							if (isPostResponseLogEnabled && postResponse.length() > 0)
								writeFileAsync(StringOps.append(KafkaSVMap.getLogDirPostHttp(), File.separator,
										new SimpleDateFormat("yyyy-MM-dd").format(new Date()), File.separator, name),
										StringOps.append(id, new SimpleDateFormat("HH-mm-ss-SSS").format(new Date()),
												".txt"),
										postResponse.trim(), false, null);
							LOGGER.info("ID {}... Name {}...EPW count {} Message posted.",
									new Object[] { id, name, counter });
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

	public void waitFunc(long wait, String id, String filename, String serviceName, int epWcounter, boolean isWaitFirst,
			int epwCount) {
		String epwCounter = String.valueOf(epWcounter);
		String epwCountString = String.valueOf(epwCount);
		String isWaitFirstString = String.valueOf(isWaitFirst);
		KafkaSVMap.addWaitingMessageId(id, filename, serviceName, epwCounter, isWaitFirstString, epwCountString);

		long wait10Sec = wait / 10000;

		long count = 0;

		while (count < wait10Sec) {
			if (KafkaSVMap.shouldMessageWait(id))
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
				}
			else
				break;

			count++;
		}

		if (count == wait10Sec)
			KafkaSVMap.removeWaitingMessageId(id);

	}

	public void onMessage(String id, String filename, int epwCount) {

		int counter = 0;

		boolean shouldKeep = false;

		try {
			if (!persistentFilterValue.isEmpty()) {
				if (containsPersistentFilterValue(getRequestData(filename, id))) {

					if (isPersistentOrMandatory && persistentFilterValueOR.isEmpty()) {
						LOGGER.error(
								"{}... DMSV-ERROR: Persistent OR filter is mandatory and can not be empty. Please set the filter values. No action.",
								new Object[] { id });
						akhil.DataUnlimited.util.FileOperation.deleteFile(filename, "");
						return;
					}

					if ((!isPersistentOrMandatory && persistentFilterValueOR.isEmpty())
							|| (!persistentFilterValueOR.isEmpty()
									&& containsPersistentORFilterValue(getRequestData(filename, id)))) {

						int lastEpw = epws.size();
						counter = 0;
						for (EPW e : epws) {
							counter++;
							LOGGER.info("{}... EPW processing starts... {}", new Object[] { id, counter });
							if (counter < epwCount)
								continue;

							boolean shouldProcess = false;

							if (filterValue.isEmpty()) {
								if (isOneTimeMandatory) {
									LOGGER.warn(
											"{}... WARNING: One-time filter is set to be mandatory and is empty. No action will be taken other than wait. Waiting for {}",
											new Object[] { id, String.valueOf(e.getWait()) });
									waitFunc(e.getWait(), id, filename, name, counter, isWaitFirst, epwCount);
									shouldProcess = false;
								} else
									shouldProcess = true;

							} else {
								if (containsFilterValue(getRequestData(filename, id)) && !storeContainsFilterValue()) {
									shouldProcess = true;
									if (lastEpw == counter) {
										String cfv = containedFilterValue(getRequestData(filename, id));
										synchronized (filterValue) {
											filterStore.add(cfv);
											filterValue.remove(cfv);
											if (filterStore.size() > 100)
												filterStore.remove(0);
										}
										LOGGER.info(
												"{}... {}...Filter value: {} matches the incoming file. It will be used only once, clear filter to reuse or to clear used values.",
												new Object[] { id, name, cfv });
									}
								} else {
									shouldProcess = false;
									LOGGER.warn(
											"{}... {}... No match for one-time filter value for EPW count {}. Waiting for {}",
											new Object[] { id, name, String.valueOf(counter),
													String.valueOf(e.getWait()) });
									waitFunc(e.getWait(), id, filename, name, counter, isWaitFirst, epwCount);
								}
							}

							LOGGER.info("{}... SHOULD PROCESS... {}", new Object[] { id, shouldProcess });

							if (shouldProcess) {
								if (isWaitFirst)
									waitFunc(e.getWait(), id, filename, name, counter, isWaitFirst, epwCount);

								try {
									KafkaSVMap.getSemaphore().acquire();
									epwProcessing(id, filename, e, counter);
								} finally {
									KafkaSVMap.getSemaphore().release();
								}

								if (!isWaitFirst)
									waitFunc(e.getWait(), id, filename, name, counter, isWaitFirst, epwCount);

								if (shouldProcess)
									shouldKeep = shouldProcess;
							}
						}

						LOGGER.info("{}...{}... Complete.", new Object[] { id, name });

					} else
						LOGGER.warn("{}... {}... No match with persistent-or filter/message selector",
								new Object[] { id, name });
				} else
					LOGGER.warn("{}... {}... No match with persistent-and filter/message selector",
							new Object[] { id, name });
			} else
				LOGGER.warn("{}... {}... Persistent filter/message selector is empty. No action.",
						new Object[] { id, name });

		} catch (Exception e) {
			LOGGER.error("{}... {}...\nOnMessage Error: {}\n", new Object[] { id, name, LogStackTrace.get(e) });
		}
		pending.decrementAndGet();

		if (isRequestLogEnabled && shouldKeep) {
		} else
			akhil.DataUnlimited.util.FileOperation.deleteFile(filename, "");
	}
}
