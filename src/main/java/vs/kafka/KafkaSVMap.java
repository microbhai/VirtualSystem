package vs.kafka;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import akhil.DataUnlimited.dataextractor.hierarchicaldoc.UtilityFunctions;
import akhil.DataUnlimited.util.FileOperation;
import vs.util.OauthCredentials;
import vs.util.OauthIDSecret;
import vs.util.SQLite;
import vs.util.StringOps;

class SemaPhoreWithReduction extends Semaphore {

	private static final long serialVersionUID = 1L;

	public SemaPhoreWithReduction(int max) {
		super(max, true);
	}

	public void reducePermits(int reduction) {
		super.reducePermits(reduction);
	}

}

public class KafkaSVMap {

	private static final Logger LOGGER = LogManager.getLogger(KafkaSVMap.class.getName());
	private static KafkaSVMap jfsvm;
	private static Map<String, KafkaVirtualService> hm = new HashMap<>();

	private static String logdirrequests;
	private static String logdirresponses;
	private static String logdirschema;
	private static String logdirposthttp;
	private static String tempDataGenDir;

	
	private static int batchSize = 15000;
	private static int waitPerMessageMs = 500;
	
	private static int MAX_NOF_THREADS = 500;
	private static SemaPhoreWithReduction semaphore = new SemaPhoreWithReduction(MAX_NOF_THREADS);

	public static Semaphore getSemaphore() {
		return semaphore;
	}

	public static void addWaitingMessageId(String id, String filename, String serviceName, String epwCounter,
			String isWaitFirst, String epwCount) {
		String query = "insert into waitingmessageid(id, filename, servicename, epwcounter, iswaitfirst, epwCount) values (?, ?, ?, ?, ?, ?)";
		SQLite.getInstance().dmlQuery(query, id, filename, serviceName, epwCounter, isWaitFirst, epwCount);
	}

	public static void removeWaitingMessageId(String id) {
		String query = "delete from waitingmessageid where id = ?";
		SQLite.getInstance().dmlQuery(query, id);
	}

	public static boolean shouldMessageWait(String id) {
		String query = "select id from waitingmessageid where id = ?";
		List<List<String>> qr = SQLite.getInstance().selectQuery(query, false, id);
		return !qr.isEmpty();
	}

	public static int getBatchSize() {
		return batchSize;
	}

	public static void submit(Runnable r) {
		esx.submit(r);
	}

	public static void setMaxParallel(String val) {
		try {
			int newVal = Integer.parseInt(val);
			if (newVal > MAX_NOF_THREADS)
				semaphore.release(newVal - MAX_NOF_THREADS);
			if (newVal < MAX_NOF_THREADS)
				semaphore.reducePermits(MAX_NOF_THREADS - newVal);
			MAX_NOF_THREADS = newVal;
		} catch (NumberFormatException e) {
		}
	}

	public static int getMaxParallel() {
		return MAX_NOF_THREADS;
	}

	public static int getWaitPerMessage() {
		return waitPerMessageMs;
	}

	public static void setWaitPerMessage(String val) {
		try {
			waitPerMessageMs = Integer.parseInt(val);
		} catch (NumberFormatException e) {
		}
	}

	public static void setBatchSize(String val) {
		try {
			batchSize = Integer.parseInt(val);
		} catch (NumberFormatException e) {
		}
	}

	private static ScheduledExecutorService es;
	private static ExecutorService esx;

	//private static String tempRequestDir;
	private static String svTempDir;
	private static int logRetention = 604800000;

	public static void setLogDeleteTime(int time) {
		if (time > 0)
			logRetention = time;

		es.shutdown();
		es.shutdownNow();
		logFileClean();
	}

	public static int getLogDeleteTime() {
		return logRetention;
	}

	public static void incCount(String name) {
		hm.get(name).incCount();
	}

	public static String getCount(String name) {
		return hm.get(name).getCount();
	}

	public static String getSVTempDir() {
		return svTempDir;
	}

	public static void setSVTempDir(String dir) {
		svTempDir = dir;
	}

	public static void stop() {

		es.shutdown();
		esx.shutdown();
		for (Map.Entry<String, KafkaVirtualService> en : hm.entrySet()) {
			en.getValue().interrupt(true);
		}
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
		}
		es.shutdownNow();
		esx.shutdownNow();
	}

	public static String setFilterValue(String name, String filterValue, String type) {
		try { /// to be removed.
			if (hm.containsKey(name)) {
				hm.get(name).addFilter(filterValue, type);
				return StringOps.append("<dmsv-status>DMSV-SUCCESS: Filter value updated for service ", name, " to ",
						filterValue, "</dmsv-status>");
			} else
				return "<dmsv-status>DMSV-ERROR: Service not found</dmsv-status>";
		} catch (Exception e) { // to be removed.
			return StringOps.append("<dmsv-status>DMSV-ERROR: Unable to Update filter file value due to: ",
					Arrays.toString(e.getStackTrace()), " </dmsv-status>");
		}
	}

	public static String setFilterMandatory(String name, String setMandatory, String type) {
		try { /// to be removed.
			if (hm.containsKey(name)) {
				return StringOps.append("<dmsv-status>", hm.get(name).setMandatoryFilter(setMandatory, type),
						"</dmsv-status>");
			} else
				return "<dmsv-status>DMSV-ERROR: Service not found</dmsv-status>";
		} catch (Exception e) { // to be removed.
			return StringOps.append("<dmsv-status>DMSV-ERROR: Unable to Update filter file value due to: ",
					Arrays.toString(e.getStackTrace()), " </dmsv-status>");
		}
	}

	private KafkaSVMap() {
	}

	public static void setLogRequestDir(String ld) {
		logdirrequests = ld;
	}

	public static void setLogResponseDir(String ld) {
		logdirresponses = ld;
	}

	public static void setLogSchemaDir(String ld) {
		logdirschema = ld;
	}

	public static void setLogPostHttpDir(String ld) {
		logdirposthttp = ld;
	}

	public static String getLogDirRequests() {
		return logdirrequests;
	}

	public static String getLogDirResponses() {
		return logdirresponses;
	}

	public static String getLogDirSchema() {
		return logdirschema;
	}

	public static String getLogDirPostHttp() {
		return logdirposthttp;
	}

	public static KafkaSVMap getInstance() {
		if (jfsvm == null) {
			jfsvm = new KafkaSVMap();
		}
		return jfsvm;
	}

	public static String getEPWs(String name) {
		return hm.get(name).getEPWs();
	}

	public static String deleteService(List<String> services) {
		StringBuilder sb = new StringBuilder();

		if (!services.isEmpty()) {
			for (String s : services) {
				if (hm.containsKey(s)) {
					if (hm.get(s).getLockedStatus()) {
						sb.append("<dmsv-status>Service ").append(s).append(" is locked and can't be deleted")
								.append("</dmsv-status>\n");
					} else {
						delete(s);
						sb.append("<dmsv-status>Service ").append(s).append(" interrupted @ ").append(new Date())
								.append("</dmsv-status>\n");
					}
				} else {
					sb.append("<dmsv-status>Service ").append(s)
							.append(" not found in the Available services</dmsv-status>\n");
				}
			}
		} else {
			sb.append("<dmsv-status>No Service/s provided to be stopped</dmsv-status>\n");
		}
		return sb.toString();
	}

	public static String setServiceStatus(List<String> services) {

		StringBuilder sb = new StringBuilder();
		String srvcs = services.get(0);

		List<String> service = UtilityFunctions.getInBetween(srvcs, "<dmsv-service>", "</dmsv-service>", true);

		for (String s : service) {
			String name = UtilityFunctions.getInBetween(s, "<dmsv-name>", "</dmsv-name>", true).get(0);
			String action = UtilityFunctions.getInBetween(s, "<dmsv-action>", "</dmsv-action>", true).get(0);

			if (hm.containsKey(name)) {

				if (action.equals("lock") || action.startsWith("unlock:")) {
					if (action.equals("lock")) {
						if (hm.get(name).getLockedStatus())
							sb.append("<dmsv-status>Service Name: ").append(name)
									.append(" is already locked </dmsv-status>\n");
						else {
							sb.append("<dmsv-status>Service Name: ").append(name).append(" is locked with unlock <key>")
									.append(hm.get(name).lockService())
									.append("</key>. You will need this key to unlock the service </dmsv-status>\n");
						}
					} else {
						if (!hm.get(name).getLockedStatus())
							sb.append("<dmsv-status>Service Name: ").append(name)
									.append(" is already unlocked </dmsv-status>\n");
						else {
							String key = StringOps.fastSplit(action, ":").get(1);
							Boolean lockedStatus = hm.get(name).unlockService(key);
							sb.append("<dmsv-status>Service Name: ").append(name).append(" now has locked status of ")
									.append(lockedStatus)
									.append(". If still \"true\", the unlock key used may be incorrect </dmsv-status>\n");
						}
					}
				} else {
					switch (action.toLowerCase()) {
					case "stop":
						if (!hm.get(name).getStatus().equalsIgnoreCase("Stopped")) {
							hm.get(name).interrupt(false);
							hm.get(name).setStatus("Stopped", false);
							sb.append("<dmsv-status>Service Name: ").append(name)
									.append(" has been stopped </dmsv-status>\n");
						} else
							sb.append("<dmsv-status>Service Name: ").append(name)
									.append(" is already stopped </dmsv-status>\n");
						break;
					case "start":

						if (hm.get(name).getStatus().equalsIgnoreCase("Stopped")) {
							hm.get(name).startService();
							sb.append("<dmsv-status>Service Name: ").append(name)
									.append(" is starting </dmsv-status>\n");
						} else if (hm.get(name).getStatus().equalsIgnoreCase("Paused")) {
							hm.get(name).unpause();
							sb.append("<dmsv-status>Service Name: ").append(name)
									.append(" is starting </dmsv-status>\n");
						} else {
							sb.append("<dmsv-status>Service Name: ").append(name)
									.append(" is already running. Stop it before you can start it. </dmsv-status>\n");
						}
						break;
					case "pause":

						if (hm.get(name).getStatus().equalsIgnoreCase("Running")) {
							hm.get(name).pause();
							sb.append("<dmsv-status>Service Name: ").append(name).append(" is paused </dmsv-status>\n");
						} else if (hm.get(name).getStatus().equalsIgnoreCase("Paused")
								|| hm.get(name).getStatus().equalsIgnoreCase("Stopped")) {
							sb.append("<dmsv-status>Service Name: ").append(name)
									.append(" is already paused/stopped </dmsv-status>\n");
						} else {
							sb.append("<dmsv-status>Service Name: ").append(name)
									.append(" not Running or Paused or Stopped </dmsv-status>\n");
						}
						break;
					case "enable_response_post":

						if (hm.get(name).getStatus().equalsIgnoreCase("Running")) {
							hm.get(name).enablePost(true);
							sb.append("<dmsv-status>Service Name: ").append(name)
									.append(" posting to target system is enabled </dmsv-status>\n");
						} else if (hm.get(name).getStatus().equalsIgnoreCase("Paused")
								|| hm.get(name).getStatus().equalsIgnoreCase("Stopped")) {
							sb.append("<dmsv-status>Service Name: ").append(name)
									.append(" is paused/stopped. Posting won't be enabled </dmsv-status>\n");
						} else {
							sb.append("<dmsv-status>Service Name: ").append(name)
									.append(" not Running or Paused or Stopped </dmsv-status>\n");
						}
						break;
					case "disable_response_post":

						if (hm.get(name).getStatus().equalsIgnoreCase("Running")) {
							hm.get(name).enablePost(false);
							sb.append("<dmsv-status>Service Name: ").append(name)
									.append(" posting to target system is disabled </dmsv-status>\n");
						} else if (hm.get(name).getStatus().equalsIgnoreCase("Paused")
								|| hm.get(name).getStatus().equalsIgnoreCase("Stopped")) {
							sb.append("<dmsv-status>Service Name: ").append(name)
									.append(" is paused/stopped. Posting won't be disabled </dmsv-status>\n");
						} else {
							sb.append("<dmsv-status>Service Name: ").append(name)
									.append(" not Running or Paused or Stopped </dmsv-status>\n");
						}
						break;
					case "enable_request_log":

						if (hm.get(name).getStatus().equalsIgnoreCase("Running")) {
							hm.get(name).enableRequestLog(true);
							sb.append("<dmsv-status>Service Name: ").append(name).append(
									" requests will now be logged under the SVRequestResponseLogs directory </dmsv-status>\n");
						} else if (hm.get(name).getStatus().equalsIgnoreCase("Paused")
								|| hm.get(name).getStatus().equalsIgnoreCase("Stopped")) {
							sb.append("<dmsv-status>Service Name: ").append(name)
									.append(" is paused/stopped. No action can be taken </dmsv-status>\n");
						} else {
							sb.append("<dmsv-status>Service Name: ").append(name)
									.append(" not Running or Paused or Stopped </dmsv-status>\n");
						}
						break;
					case "disable_request_log":

						if (hm.get(name).getStatus().equalsIgnoreCase("Running")) {
							hm.get(name).enableRequestLog(false);
							sb.append("<dmsv-status>Service Name: ").append(name).append(
									" request logging to SVRequestResponseLogs directory disabled </dmsv-status>\n");
						} else if (hm.get(name).getStatus().equalsIgnoreCase("Paused")
								|| hm.get(name).getStatus().equalsIgnoreCase("Stopped")) {
							sb.append("<dmsv-status>Service Name: ").append(name)
									.append(" is paused/stopped. No action can be taken </dmsv-status>\n");
						} else {
							sb.append("<dmsv-status>Service Name: ").append(name)
									.append(" not Running or Paused or Stopped </dmsv-status>\n");
						}
						break;
					case "enable_response_log":

						if (hm.get(name).getStatus().equalsIgnoreCase("Running")) {
							hm.get(name).enableResponseLog(true);
							sb.append("<dmsv-status>Service Name: ").append(name).append(
									" DMSV responses will now be logged under the SVRequestResponseLogs directory </dmsv-status>\n");
						} else if (hm.get(name).getStatus().equalsIgnoreCase("Paused")
								|| hm.get(name).getStatus().equalsIgnoreCase("Stopped")) {
							sb.append("<dmsv-status>Service Name: ").append(name)
									.append(" is paused/stopped. No action can be taken </dmsv-status>\n");
						} else {
							sb.append("<dmsv-status>Service Name: ").append(name)
									.append(" not Running or Paused or Stopped </dmsv-status>\n");
						}
						break;
					case "disable_response_log":

						if (hm.get(name).getStatus().equalsIgnoreCase("Running")) {
							hm.get(name).enableResponseLog(false);
							sb.append("<dmsv-status>Service Name: ").append(name).append(
									" DMSV response logging to SVRequestResponseLogs directory disabled </dmsv-status>\n");
						} else if (hm.get(name).getStatus().equalsIgnoreCase("Paused")
								|| hm.get(name).getStatus().equalsIgnoreCase("Stopped")) {
							sb.append("<dmsv-status>Service Name: ").append(name)
									.append(" is paused/stopped. No action can be taken </dmsv-status>\n");
						} else {
							sb.append("<dmsv-status>Service Name: ").append(name)
									.append(" not Running or Paused or Stopped </dmsv-status>\n");
						}
						break;
					case "enable_post_response_log":

						if (hm.get(name).getStatus().equalsIgnoreCase("Running")) {
							hm.get(name).enablePostResponseLog(true);
							sb.append("<dmsv-status>Service Name: ").append(name).append(
									" post response (HTTP) will now be logged under the SVRequestResponseLogs directory </dmsv-status>\n");
						} else if (hm.get(name).getStatus().equalsIgnoreCase("Paused")
								|| hm.get(name).getStatus().equalsIgnoreCase("Stopped")) {
							sb.append("<dmsv-status>Service Name: ").append(name)
									.append(" is paused/stopped. No action can be taken </dmsv-status>\n");
						} else {
							sb.append("<dmsv-status>Service Name: ").append(name)
									.append(" not Running or Paused or Stopped </dmsv-status>\n");
						}
						break;
					case "disable_post_response_log":

						if (hm.get(name).getStatus().equalsIgnoreCase("Running")) {
							hm.get(name).enablePostResponseLog(false);
							sb.append("<dmsv-status>Service Name: ").append(name).append(
									" post response (HTTP) logging to SVRequestResponseLogs directory disabled </dmsv-status>\n");
						} else if (hm.get(name).getStatus().equalsIgnoreCase("Paused")
								|| hm.get(name).getStatus().equalsIgnoreCase("Stopped")) {
							sb.append("<dmsv-status>Service Name: ").append(name)
									.append(" is paused/stopped. No action can be taken </dmsv-status>\n");
						} else {
							sb.append("<dmsv-status>Service Name: ").append(name)
									.append(" not Running or Paused or Stopped </dmsv-status>\n");
						}
						break;
					default:
						sb.append("<dmsv-status>Service Name: ").append(name).append(
								" doesn't have proper action (only start / stop / pause / enable_request_log / disable_request_log / enable_response_log / disable_response_log / disable_response_post / enable_response_post / lock / unlock:<unlockkey> actions are available) </dmsv-status>\n");
						break;
					}
				}
			}

			else {
				return StringOps.append("<dmsv-status>No Service/s with name ", name,
						" yet available in the SV Server</dmsv-status>\n");
			}

		}

		return sb.toString();

	}
	public static String getTempDataGenDir() {
		return tempDataGenDir;
	}
	public static void setTempDataGenDir(String dir) {
		tempDataGenDir = dir;
	}

	private static void logFileClean() {
		es = Executors.newScheduledThreadPool(1);
		Runnable r = () -> {
			FileOperation.deleteFile(logdirposthttp, true, logRetention);
			FileOperation.deleteFile(logdirrequests, true, logRetention);
			FileOperation.deleteFile(logdirresponses, true, logRetention);
			FileOperation.deleteFile(logdirschema, true, logRetention);
			FileOperation.deleteFile(svTempDir, true, logRetention);
			FileOperation.deleteFile(tempDataGenDir, true, logRetention);

		};
		es.scheduleAtFixedRate(r, 0, 3600000, TimeUnit.MILLISECONDS);
	}

	public static void initialize() {

		String query = "select ID, SECRET, TYPE, OAUTHSCOPE, GRANTTYPE from VS_OKTA_CREDENTIALS";

		List<List<String>> qr1 = SQLite.getInstance().selectQuery(query, false);
		if (!qr1.isEmpty()) {
			for (List<String> ls : qr1) {
				OauthIDSecret o = new OauthIDSecret();
				o.setOauth(ls.get(0), ls.get(1), ls.get(2), ls.get(3), ls.get(4), true);
				OauthCredentials.getInstance().addCreds(ls.get(2), o);
			}
		}

		query = "select VS_NAME, VS_DESCRIPTION, SUB, EPWS, REQSERVED, STATUS, ENABLEPOST, ENABLEREQUESTLOG, ENABLERESPONSELOG, ENABLEPOSTRESPONSELOG from VS_KAFKA_DMSSCRIPT";

		List<List<String>> qr2 = SQLite.getInstance().selectQuery(query, false);
		if (!qr2.isEmpty()) {
			for (List<String> ls : qr2) {
				KafkaVirtualService fvs = new KafkaVirtualService();
				fvs.startService(ls.get(0), ls.get(1), ls.get(2), ls.get(3), ls.get(4), ls.get(5), ls.get(6), ls.get(7),
						ls.get(8), ls.get(9));
				hm.put(ls.get(0), fvs);
			}
		}

		try {
			Thread.sleep(120000);
		} catch (InterruptedException e) {
		}

		query = "select id, filename, servicename, epwcounter, iswaitfirst, epwCount from waitingmessageid";
		List<List<String>> qr3 = SQLite.getInstance().selectQuery(query, false);
		esx = Executors.newCachedThreadPool();
		if (!qr3.isEmpty()) {
			for (List<String> ls : qr3) {
				String id = ls.get(0);
				String filename = ls.get(1);
				String serviceName = ls.get(2);
				int epwCounter = Integer.parseInt(ls.get(3));
				boolean isWaitFirst = Boolean.parseBoolean(ls.get(4));
				int epwCount = Integer.parseInt(ls.get(5));

				removeWaitingMessageId(id);

				if (hm.get(serviceName) != null) {
					OnMessageTester o = new OnMessageTester(serviceName);
					o.enablePost(hm.get(serviceName).getEnablePost());
					o.enableRequestLog(hm.get(serviceName).getEnableRequestLog());
					o.enableResponseLog(hm.get(serviceName).getEnableResponseLog());
					o.enablePostResponseLog(hm.get(serviceName).getEnablePostResponseLog());
					o.pause(false);
					query = "select epws, sub from VS_KAFKA_DMSSCRIPT where VS_NAME = ?";
					List<List<String>> qrx = SQLite.getInstance().selectQuery(query, false, serviceName);
					String epws = null;
					String sub = null;
					if (!qrx.isEmpty()) {
						epws = qrx.get(0).get(0);
						sub = qrx.get(0).get(1);
					}
					String subName = StringOps.getInBetweenFast(sub, "<sub-name>", "</sub-name>", true, true).get(0);
					if (epws != null && sub != null) {
						o.setEPWs(epws);

						query = "select details from kafkacalldetails where name = ?";
						List<List<String>> qry = SQLite.getInstance().selectQuery(query, false, subName);
						if (!qry.isEmpty())
							sub = qry.get(0).get(0);

						List<String> persistentAndList = StringOps.getInBetweenFast(sub, "<persistent-and>",
								"</persistent-and>", true, true);
						List<String> persistentOrList = StringOps.getInBetweenFast(sub, "<persistent-or>",
								"</persistent-or>", true, true);
						String persistentAnd = "";
						List<String> persistentOr = null;
						if (!persistentAndList.isEmpty())
							persistentAnd = persistentAndList.get(0).trim();
						if (!persistentOrList.isEmpty())
							persistentOr = persistentOrList;
						o.setFilterValue(StringOps.fastSplit(persistentAnd, "<DMSDELIM>"), "persistent-and");
						if (persistentOr != null)
							for (String orFilter : persistentOr) {
								o.setFilterValue(StringOps.fastSplit(orFilter, "<DMSDELIM>"), "persistent-or");
							}

						// o.setZeroWait();
						Runnable r1 = () -> {
							if (isWaitFirst) {
								o.onMessage(id, filename, epwCounter);
							} else {
								if (epwCounter < epwCount)
									o.onMessage(id, filename, epwCounter + 1);
							}
							KafkaSVMap.incCount(serviceName);
						};
						esx.submit(r1);
					}
				}
			}
		}

		logFileClean();

	}

	public static String getAvailableService() {
		StringBuilder toreturn = new StringBuilder();
		toreturn.append("<dmsv-servicelist>\n");
		if (!hm.isEmpty()) {
			for (Map.Entry<String, KafkaVirtualService> name : hm.entrySet()) {
				toreturn.append("<dmsv-service>\n").append("<dmsv-name>").append(name.getKey()).append("</dmsv-name>\n")
						.append("<dmsv-status>").append(name.getValue().getStatus()).append("</dmsv-status>\n")
						.append("<dmsv-description>")
						.append(name.getValue().getDescription().replace("&", "&amp;").replace("<", "&lt;")
								.replace(">", "&gt;").replace("'", "&apos;").replace("\"", "&quot;"))
						.append("</dmsv-description>\n").append("<dmsv-requestsserved>")
						.append(name.getValue().getCount()).append("</dmsv-requestsserved>\n").append("<dmsv-locked>")
						.append(name.getValue().getLockedStatus()).append("</dmsv-locked>\n")
						.append("</dmsv-service>\n");
			}
			toreturn.append("</dmsv-servicelist>");
			return toreturn.toString();
		} else {
			toreturn.append("</dmsv-servicelist>");
			return "<dmsv-status>No Service/s Yet Available in the SV Server</dmsv-status>";
		}
	}

	public static String getFilterValues(String name) {
		if (hm.containsKey(name)) {
			return StringOps.append("<dmsv-status>\n<one-time>", hm.get(name).getFilterValues(),
					"</one-time>\n<persistent-and>", hm.get(name).getPersistentFilterValues(),
					"</persistent-and>\n<persistent-or>", hm.get(name).getPersistentFilterValuesOR(),
					"</persistent-or>\n<used>", hm.get(name).getUsedFilterValues(), "</used>\n</dmsv-status>");
		} else
			return StringOps.append("<dmsv-status>DMSV-ERROR: Service ", name, " wasn't found</dmsv-status>");
	}

	public static String clearFilter(String name, String type) {
		if (hm.containsKey(name))
			hm.get(name).clearFilter(type);
		return StringOps.append("<dmsv-status>Type ", type, " filter values cleared for service ", name,
				"</dmsv-status>");
	}

	public static String clearFilter(String name, String type, List<String> filterValue) {
		if (hm.containsKey(name)) {
			for (String fv : filterValue)
				hm.get(name).clearFilter(type, fv);
		}
		return StringOps.append("<dmsv-status>Filter values cleared for service ", name, "</dmsv-status>");
	}

	public static String getAvailableService(String name) {
		if (hm.containsKey(name)) {
			StringBuilder toreturn = new StringBuilder();
			toreturn.append("<dmsv-service-details>\n");
			KafkaVirtualService fm = hm.get(name);
			toreturn.append(fm.getDetails());
			toreturn.append("</dmsv-service-details>");
			return toreturn.toString();
		} else {
			return "<dmsv-status>DMSV-ERROR: No Services available on specified URL</dmsv-status>";
		}

	}

	public String registerService(String name, String description, String overwrite, String sub, String epws) {

		if (hm.containsKey(name)) {
			if (overwrite.equalsIgnoreCase("true") && !hm.get(name).getLockedStatus()) {
				delete(name);
				KafkaVirtualService fvs = new KafkaVirtualService();
				String response = fvs.startService(name, description, sub, epws, "0", "Paused", "false", "true", "true",
						"true");
				LOGGER.info("Response MAP: {}", response);
				if (response.contains("SUCCESS")) {
					hm.put(name, fvs);
					String query = "insert into VS_KAFKA_DMSSCRIPT (VS_NAME, VS_DESCRIPTION, SUB, EPWS, REQSERVED ) values (?, ?, ?, ?, ?)";
					SQLite.getInstance().dmlQuery(query, name, description, sub, epws, "0");

					return StringOps.append("<dmsv-status>DMSV-SUCCESS: Existing Service with Name: ", name,
							" has been updated...</dmsv-status>");
				} else {
					return StringOps.append("<dmsv-status>DMSV-ERROR: Service could not be started successfully due to : ",
							response, "</dmsv-status>");
				}
			} else {
				return StringOps.append("<dmsv-status>DMSV-ERROR: Service already exists with Name: ", name,
						".\nMake sure service is unlocked and use <dmsv-overwrite-existing>true</dmsv-overwrite-existing> to overwrite the existing service...</dmsv-status>");
			}
		} else {
			KafkaVirtualService fvs = new KafkaVirtualService();
			String response = fvs.startService(name, description, sub, epws, "0", "Paused", "false", "true", "true",
					"true");
			if (response.contains("SUCCESS")) {
				hm.put(name, fvs);

				String query = "insert into VS_KAFKA_DMSSCRIPT (VS_NAME, VS_DESCRIPTION, SUB, EPWS, REQSERVED ) values (?, ?, ?, ?, ?)";
				SQLite.getInstance().dmlQuery(query, name, description, sub, epws, "0");

			} else {
				return StringOps.append("<dmsv-status>New Service with Name: ", name, " failed to start due to ",
						response, "</dmsv-status>");
			}
			return StringOps.append("<dmsv-status>New Service with Name: ", name,
					" is ready for use... it is in paused state. Send start service command to start it. </dmsv-status>");
		}
	}

	public static void delete(String name) {
		hm.get(name).interrupt(false);
		hm.remove(name);

		String queryDelete = "delete from VS_KAFKA_DMSSCRIPT where VS_NAME = ?";
		SQLite.getInstance().dmlQuery(queryDelete, name);

	}
}