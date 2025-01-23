package vs.web;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import akhil.DataUnlimited.util.FileOperation;
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

public class WebSVMap {

	private static Map<String, WebVirtualService> hm = new HashMap<>();

	private static String logdirrequests;
	private static String logdirresponses;
	private static String logdirschema;
	private static String logdirposthttp;
	private static String tempDataGenDir;
	private static String svTempDir;

	private static ScheduledExecutorService es;
	private static ExecutorService esx;

	private static String delim = "+";
	private static String slash = "DMS-SLASH";
	private static String regexp = "((?!/).)*?";
	private static int logRetention = 604800000;

	private static int MAX_NOF_THREADS = 500;
	private static SemaPhoreWithReduction semaphore = new SemaPhoreWithReduction(MAX_NOF_THREADS);

	public static Semaphore getSemaphore() {
		return semaphore;
	}

	public static void incCount(String name) {
		List<String> pieces = StringOps.fastSplit(name, "+");
		name = StringOps.append(pieces.get(0), delim, pieces.get(1));
		hm.get(name).incCount();
	}

	public static String getCount(String key) {
		return hm.get(key).getCount();
	}

	public static void submit(Runnable r) {
		esx.submit(r);
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

	public static String getTempDataGenDir() {
		return tempDataGenDir;
	}

	public static String getSVTempDir() {
		return svTempDir;
	}

	public static void setSVTempDir(String dir) {
		svTempDir = dir;
	}

	public static void setTempDataGenDir(String dir) {
		tempDataGenDir = dir;
	}

	public static void addWaitingMessageId(String id, String filename, String serviceName, String epwCounter,
			String isWaitFirst, String epwCount) {
		String query = "insert into waitingmessageidweb(id, filename, servicename, epwcounter, iswaitfirst, epwCount) values (?, ?, ?, ?, ?, ?)";
		SQLite.getInstance().dmlQuery(query, id, filename, serviceName, epwCounter, isWaitFirst, epwCount);
	}

	public static void removeWaitingMessageId(String id) {
		String query = "delete from waitingmessageidweb where id = ?";
		SQLite.getInstance().dmlQuery(query, id);
	}

	public static boolean shouldMessageWait(String id) {
		String query = "select id from waitingmessageidweb where id = ?";
		List<List<String>> qr = SQLite.getInstance().selectQuery(query, false, id);
		return !qr.isEmpty();
	}

	public static void stop() {
		es.shutdown();
		for (WebVirtualService w : hm.values()) {
			w.stop();
		}
		es.shutdownNow();
	}

	public void enablePost(String url, String type, boolean b) {

		url = url.replace("/", slash);
		String key = StringOps.append(url, delim, type);
		hm.get(key).enablePost(b);
		String setting;
		if (b)
			setting = "true";
		else
			setting = "false";
		String query = "update VS_URL_DMSSCRIPT set enablePost = ? where VS_NAME like ?";
		SQLite.getInstance().dmlQuery(query, setting, StringOps.append(key, "%"));
	}

	public void enableRequestLog(String url, String type, boolean b) {

		url = url.replace("/", slash);
		String key = StringOps.append(url, delim, type);
		hm.get(key).enableRequestLog(b);
		String setting;
		if (b)
			setting = "true";
		else
			setting = "false";
		String query = "update VS_URL_DMSSCRIPT set enableRequestLog = ? where VS_NAME like ?";
		SQLite.getInstance().dmlQuery(query, setting, StringOps.append(key, "%"));
	}

	public void enableResponseLog(String url, String type, boolean b) {

		url = url.replace("/", slash);
		String key = StringOps.append(url, delim, type);
		hm.get(key).enableResponseLog(b);
		String setting;
		if (b)
			setting = "true";
		else
			setting = "false";
		String query = "update VS_URL_DMSSCRIPT set enableResponseLog = ? where VS_NAME like ?";
		SQLite.getInstance().dmlQuery(query, setting, StringOps.append(key, "%"));
	}

	public void enablePostResponseLog(String url, String type, boolean b) {

		url = url.replace("/", slash);
		String key = StringOps.append(url, delim, type);
		hm.get(key).enablePostResponseLog(b);
		String setting;
		if (b)
			setting = "true";
		else
			setting = "false";
		String query = "update VS_URL_DMSSCRIPT set enablePostResponseLog = ? where VS_NAME like ?";
		SQLite.getInstance().dmlQuery(query, setting, StringOps.append(key, "%"));
	}

	public static String getAvailableServices() {
		StringBuilder toreturn = new StringBuilder();
		toreturn.append("<dmsv-servicelist>\n");
		for (String urlEtc : hm.keySet()) {
			List<String> pieces = StringOps.fastSplit(urlEtc, delim);
			toreturn.append(StringOps.append("<dmsv-service-key><url>", pieces.get(0), "</url><method>", pieces.get(1),
					"</method><dmsv-locked>", String.valueOf(hm.get(urlEtc).getLockedStatus()),
					"</dmsv-locked><dmsv-requestsserved>", hm.get(urlEtc).getCount(),
					"</dmsv-requestsserved></dmsv-service-key>\n"));
		}
		toreturn.append("</dmsv-servicelist>");
		return toreturn.toString();
	}

	public static WebVirtualService getWVS(String key) {
		return hm.get(key);
	}

	public static String getAvailableServices(String urlEtc) {
		StringBuilder toreturn = new StringBuilder();
		List<String> split = StringOps.fastSplit(urlEtc, delim);
		if (split.size() == 2) {
			if (hm.containsKey(urlEtc)) {
				toreturn.append("<dmsv-service>\n<dmsv-response-codes>\n");
				toreturn.append(hm.get(urlEtc).getResponseCodes());
				toreturn.append("\n</dmsv-response-codes>\n");
				toreturn.append("\n<set-response-code>");
				toreturn.append(hm.get(urlEtc).getResponseCode());
				toreturn.append("</set-response-code>\n");
				toreturn.append("\n</dmsv-service>\n");
				return toreturn.toString();
			}
			return "<dmsv-status>DMSV-ERROR: No Services available for specified URL key</dmsv-status>";
		} else if (split.size() == 3) {
			String key = StringOps.append(split.get(0), delim, split.get(1));
			String rco = split.get(2);
			if (hm.containsKey(key)) {
				toreturn.append("<dmsv-service>\n<url>");
				toreturn.append(split.get(0));
				toreturn.append("</url>\n<method>");
				toreturn.append(split.get(1));
				toreturn.append("</method>\n<default_response_code>");
				toreturn.append(hm.get(key).getResponseCode());
				toreturn.append("</default_response_code>\n<dmsv-response-script>\n");
				toreturn.append(hm.get(key).getDMSScriptTemplate(rco));
				toreturn.append("\n</dmsv-response-script>\n<dmsv-epws>\n");
				toreturn.append(hm.get(key).getEPWs(rco));
				toreturn.append("\n</dmsv-epws>\n<dmsv-locked>\n");
				toreturn.append(String.valueOf(hm.get(key).getLockedStatus()));
				toreturn.append("\n</dmsv-locked>\n</dmsv-service>\n");

				return toreturn.toString();
			} else {
				return "<dmsv-status>DMSV-ERROR: No Services available for specified URL key</dmsv-status>";
			}
		} else
			return "<dmsv-status>DMSV-ERROR: Improper input. Needs URL, HTTP METHOD, and RESPONSE CODE (optional) separated by delimiter \"__\" (double underscore) </dmsv-status>";
	}

	public static String updateRCode(String url, String type, String rco) {
		url = url.replace("/", slash);
		String key = StringOps.append(url, delim, type);

		if (hm.containsKey(key)) {
			if (hm.get(key).getLockedStatus()) {
				return StringOps.append("DMSV-ERROR: Service is locked and can't be updated", key);
			} else {

				hm.get(key).updateRCode(rco);

				for (String s : hm.get(key).getResponseCodeAsSet()) {
					String keyU = StringOps.append(key, delim, s);
					String queryUpdate = "update VS_URL_DMSSCRIPT set VS_KEY = ? where VS_KEY like ?";
					SQLite.getInstance().dmlQuery(queryUpdate, keyU, StringOps.append(keyU, "%"));
				}
				String queryUpdate = "update VS_URL_DMSSCRIPT set VS_KEY = ? where VS_KEY = ?";
				String keyU = key + delim + rco;
				int rows = SQLite.getInstance().dmlQuery(queryUpdate, StringOps.append(keyU, delim, "default"), keyU);
				return StringOps.append("DMSV-SUCCESS: Updated ", String.valueOf(rows), " rows");

			}

		} else {
			return StringOps.append("DMSV-ERROR: Service not found: ", key);
		}
	}

	public static String deleteService(String url, String type, String rco) {

		String urlwithslash = url.replace("/", slash);
		String key = StringOps.append(urlwithslash, delim, type);
		if (hm.containsKey(key)) {
			if (rco.isEmpty() || rco == null) {

				if (hm.get(key).getLockedStatus()) {
					return StringOps.append("<dmsv-status>Service ", key,
							" is locked and can't be deleted</dmsv-status>\n");
				} else {
					int result = 0;
					hm.remove(key);

					String queryDelete = "delete from VS_URL_DMSSCRIPT where VS_KEY like ?";
					result = SQLite.getInstance().dmlQuery(queryDelete, key + "%");
					if (result > 0)
						return StringOps.append("<dmsv-status>Service deleted @ ", url, " | ", type, "</dmsv-status>");
					else
						return StringOps.append("<dmsv-status>Service couldn't be deleted from db ", url, " | ", type,
								"</dmsv-status>");
				}

			} else {

				if (hm.get(key).getLockedStatus()) {
					return StringOps.append("<dmsv-status>Service ", key,
							" is locked and can't be deleted</dmsv-status>\n");
				} else {
					hm.get(key).deleteResponseCode(rco);
					int result = 0;

					key = StringOps.append(key, delim, rco);
					String queryDelete = "delete from VS_URL_DMSSCRIPT where VS_KEY like ?";
					result = SQLite.getInstance().dmlQuery(queryDelete, key + "%");
					if (result > 0)
						return StringOps.append("<dmsv-status>Service deleted @ ", url, " | ", type, " | ", rco,
								"</dmsv-status>");
					else
						return StringOps.append("<dmsv-status>Service couldn't be deleted from db ", url, " | ", type,
								" | ", rco, "</dmsv-status>");
				}
			}
		} else
			return StringOps.append("<dmsv-status>Service couldn't be found @ ", url + " | ", type, " | ", rco,
					"</dmsv-status>");
	}

	public static String updateService(String url, String type, String action) {

		String urlwithslash = url.replace("/", slash);
		String key = urlwithslash + delim + type;
		StringBuilder sb = new StringBuilder();

		if (hm.containsKey(key)) {

			if (action.equals("lock") || action.startsWith("unlock:")) {
				if (action.equals("lock")) {
					if (hm.get(key).getLockedStatus())
						sb.append("<dmsv-status>Service Name: ").append(key)
								.append(" is already locked </dmsv-status>\n");
					else {
						sb.append("<dmsv-status>Service Name: ").append(key).append(" is locked with unlock <key>")
								.append(hm.get(key).lockService())
								.append("</key>. You will need this key to unlock the service </dmsv-status>\n");
					}
				} else {
					if (!hm.get(key).getLockedStatus())
						sb.append("<dmsv-status>Service Name: ").append(key)
								.append(" is already unlocked </dmsv-status>\n");
					else {
						String keyU = StringOps.fastSplit(action, ":").get(1);
						Boolean lockedStatus = hm.get(key).unlockService(keyU);
						sb.append("<dmsv-status>Service Name: ").append(key).append(" now has locked status of ")
								.append(lockedStatus)
								.append(". If still \"true\", the unlock key used may be incorrect </dmsv-status>\n");
					}
				}

			} else {

				switch (action.toLowerCase()) {
				case "enable_response_post":
					hm.get(key).enablePost(true);
					sb.append("<dmsv-status>Existing Service @ ").append(url).append(" with method ")
							.append(type.toUpperCase()).append(" has been updated</dmsv-status>");
					break;

				case "disable_response_post":
					hm.get(key).enablePost(false);
					sb.append("<dmsv-status>Existing Service @ ").append(url).append(" with method ")
							.append(type.toUpperCase()).append(" has been updated</dmsv-status>");
					break;

				case "enable_request_log":
					hm.get(key).enableRequestLog(true);
					sb.append("<dmsv-status>Existing Service @ ").append(url).append(" with method ")
							.append(type.toUpperCase()).append(" has been updated</dmsv-status>");
					break;

				case "disable_request_log":
					hm.get(key).enableRequestLog(false);
					sb.append("<dmsv-status>Existing Service @ ").append(url).append(" with method ")
							.append(type.toUpperCase()).append(" has been updated</dmsv-status>");
					break;

				case "enable_response_log":
					hm.get(key).enableResponseLog(true);
					sb.append("<dmsv-status>Existing Service @ ").append(url).append(" with method ")
							.append(type.toUpperCase()).append(" has been updated</dmsv-status>");
					break;

				case "disable_response_log":
					hm.get(key).enableResponseLog(false);
					sb.append("<dmsv-status>Existing Service @ ").append(url).append(" with method ")
							.append(type.toUpperCase()).append(" has been updated</dmsv-status>");
					break;

				case "enable_post_response_log":
					hm.get(key).enablePostResponseLog(true);
					sb.append("<dmsv-status>Existing Service @ ").append(url).append(" with method ")
							.append(type.toUpperCase()).append(" has been updated</dmsv-status>");
					break;

				case "disable_post_response_log":
					hm.get(key).enablePostResponseLog(false);
					sb.append("<dmsv-status>Existing Service @ ").append(url).append(" with method ")
							.append(type.toUpperCase()).append(" has been updated</dmsv-status>");
					break;

				default:
					sb.append("<dmsv-status>DMSV-ERROR: Service URL: ").append(url).append(" Method: ")
							.append(type.toUpperCase())
							.append(" doesn't have proper action (only enable_request_log / disable_request_log / enable_response_log / disable_response_log / disable_response_post / enable_response_post / lock / unlock:<unlockkey> actions are available) </dmsv-status>\n");
					break;
				}
			}
			return sb.toString();

		} else {
			return StringOps.append("<dmsv-status>DMSV-ERROR: Service doesn't exist. Can't run update on service key: ", key,
					". Register it first.</dmsv-status>");
		}
	}

	public static String updateService(String url, String type, String responseCode, String dmsScriptTemplate,
			String epws, String setDefaultRCode) {

		String urlwithslash = url.replace("/", slash);
		String key = StringOps.append(urlwithslash, delim, type);

		if (hm.containsKey(key)) {
			if (hm.get(key).getLockedStatus()) {
				return StringOps.append("<dmsv-status>Service ", key,
						" is locked and can't be updated</dmsv-status>\n");
			} else {
				if (epws != null)
					hm.get(key).setEPWs(responseCode, epws);
				hm.get(key).setDMSScriptTemplate(responseCode, dmsScriptTemplate);

				String keyU = StringOps.append(key, delim, responseCode);
				String queryDelete = "delete from VS_URL_DMSSCRIPT where VS_KEY like ?";
				SQLite.getInstance().dmlQuery(queryDelete, StringOps.append(keyU, "%"));

				if (setDefaultRCode.equalsIgnoreCase("true")) {
					keyU = StringOps.append(keyU, delim, "default");
					hm.get(key).setResponseCode(responseCode);
				}

				String query = "insert into VS_URL_DMSSCRIPT (VS_KEY, DMS_SCRIPT_CONTENT, EPWS) values (?, ?, ? )";
				SQLite.getInstance().dmlQuery(query, keyU, dmsScriptTemplate, epws);

				return StringOps.append("<dmsv-status>Existing Service @ ", url, " with method ", type.toUpperCase(),
						" has been updated</dmsv-status>");
			}
		} else {

			return StringOps.append("<dmsv-status>DMSV-ERROR: Service doesn't exist. Can't run update on service key: ", key,
					". Register it first.</dmsv-status>");
		}
	}

	public static String registerService(String url, String overwrite, String type, String responseCode,
			String dmsScriptTemplate, String epws, String setRCode) {

		String urlwithslash = url.replace("/", slash);
		String key = StringOps.append(urlwithslash, delim, type);

		if (hm.containsKey(key)) {
			if (overwrite.equalsIgnoreCase("true") && !hm.get(key).getLockedStatus()) {
				hm.get(key).stop();
				hm.remove(key);
				WebVirtualService ws = new WebVirtualService(key, "false", "true", "true", "true");

				if (epws != null)
					ws.setEPWs(responseCode, epws);
				ws.setDMSScriptTemplate(responseCode, dmsScriptTemplate);
				if (setRCode.equalsIgnoreCase("true"))
					ws.setResponseCode(responseCode);
				hm.put(key, ws);
				String keyU = StringOps.append(key, delim, responseCode);
				String queryDelete = "delete from VS_URL_DMSSCRIPT where VS_KEY like ?";
				SQLite.getInstance().dmlQuery(queryDelete, StringOps.append(keyU, "%"));
				if (setRCode.equalsIgnoreCase("true"))
					keyU = keyU + delim + "default";

				String query = "insert into VS_URL_DMSSCRIPT (VS_KEY, DMS_SCRIPT_CONTENT, EPWS) values (?, ?, ? )";
				SQLite.getInstance().dmlQuery(query, keyU, dmsScriptTemplate, epws);

				return StringOps.append("<dmsv-status>Existing Service @ ", url, " with method ", type.toUpperCase(),
						" has been updated</dmsv-status>");
			} else {
				return StringOps.append("<dmsv-status>Service already exists @ ", url, " with method ",
						type.toUpperCase(),
						". Make sure service is unlocked and use <DMSV-OVERWRITE-EXISTING>true</DMSV-OVERWRITE-EXISTING> to overwrite the existing service</dmsv-status>");
			}
		} else {
			int result = 0;
			String keyU = StringOps.append(key, delim, responseCode);
			if (setRCode.equalsIgnoreCase("true"))
				keyU = StringOps.append(keyU, delim, "default");
			String query = "insert into VS_URL_DMSSCRIPT (VS_KEY, DMS_SCRIPT_CONTENT, EPWS) values (?, ?, ? )";
			result = SQLite.getInstance().dmlQuery(query, keyU, dmsScriptTemplate, epws);

			if (result != 0) {
				hm.put(key, new WebVirtualService(key, "false", "true", "true", "true"));
				hm.get(key).setEPWs(responseCode, epws);
				hm.get(key).setDMSScriptTemplate(responseCode, dmsScriptTemplate);
				if (setRCode.equalsIgnoreCase("true"))
					hm.get(key).setResponseCode(responseCode);
				key = key.replace("{}", "DMS-CURLY");
				return StringOps.append("<dmsv-status>New Service @ ", key, " is ready for use</dmsv-status>");
			} else {
				key = key.replace("{}", "DMS-CURLY");
				return StringOps.append("<dmsv-status>Error creating new service @ ", key, "</dmsv-status>");
			}
		}
	}

	public static void initialize() {

		logFileClean();

		String query = "select VS_KEY, DMS_SCRIPT_CONTENT, EPWS, REQSERVED, enablePost, enableRequestLog, enableResponseLog, enablePostResponseLog from VS_URL_DMSSCRIPT";

		List<List<String>> qr = SQLite.getInstance().selectQuery(query, false);
		if (qr.size() > 0) {
			for (List<String> ls : qr) {
				String keyU = ls.get(0);
				String dmsvScript = ls.get(1);
				String epws = ls.get(2);
				String reqServed = ls.get(3);
				List<String> keyPieces = StringOps.fastSplit(keyU, delim);
				String key = StringOps.append(keyPieces.get(0), delim, keyPieces.get(1));
				String rcode = keyPieces.get(2);

				hm.putIfAbsent(key, new WebVirtualService(key, ls.get(4), ls.get(5), ls.get(6), ls.get(7)));

				hm.get(key).setEPWs(rcode, epws);
				hm.get(key).setDMSScriptTemplate(rcode, dmsvScript);

				if (keyPieces.size() == 4 && keyPieces.get(3).equals("default")) {
					hm.get(key).setResponseCode(keyPieces.get(2));
					hm.get(key).setReqServed(reqServed);
				}
			}
		}

		query = "select id, filename, servicename, epwcounter, iswaitfirst, epwCount from waitingmessageidweb";
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

				List<String> pieces = StringOps.fastSplit(serviceName, delim);

				String key = StringOps.append(pieces.get(0), delim, pieces.get(1));
				String rco = pieces.get(2);

				if (hm.get(key) != null) {

					Runnable r1 = () -> {
						if (isWaitFirst) {
							hm.get(key).processEPWs(hm.get(key).getEPWsList(rco), id, filename, serviceName,
									epwCounter);
						} else {
							if (epwCounter < epwCount)
								hm.get(key).processEPWs(hm.get(key).getEPWsList(rco), id, filename, serviceName,
										epwCounter + 1);
						}
						WebSVMap.incCount(key);
					};
					esx.submit(r1);
				}
			}
		}

	}

	public static String getRCode(String key) {
		return hm.get(key).getRCode();
	}

	public static List<String> getDMSScriptTemplateAndRCode(String url, String type) {

		String urlwithslash = url.replace("/", slash);
		String key = StringOps.append(urlwithslash, delim, type);
		List<String> toReturn = new ArrayList<>();
		if (hm.containsKey(key)) {
			toReturn.add(hm.get(key).getDMSScriptTemplate(null));
			toReturn.add(getRCode(key));
			toReturn.add(key);
		} else {
			boolean flag = false;
			for (String k : hm.keySet()) {
				if (k.contains("{}")) {
					List<String> pieces = StringOps.fastSplit(k, delim);
					String typem = pieces.get(1);
					if (typem.equals(type)) {
						String urlm = pieces.get(0).replace(slash, "/");
						List<String> split = StringOps.fastSplit(urlm, "{}");
						StringBuilder sb = new StringBuilder();
						if (urlm.startsWith("{}"))
							sb.append(regexp);

						int counter = 0;
						for (String s : split) {
							if (!s.isEmpty()) {
								sb.append(Pattern.quote(s));
								if (counter < split.size() - 2)
									sb.append(regexp);
							}
							counter++;
						}
						if (urlm.endsWith("{}"))
							sb.append(regexp);

						String patternStr = sb.toString();

						Matcher matcher = Pattern.compile(patternStr).matcher(url);
						if (matcher.find()) {
							flag = true;
							toReturn.add(hm.get(k).getDMSScriptTemplate(null));
							toReturn.add(getRCode(k));
							toReturn.add(k);
							break;
						}
					}
				}
			}
			if (!flag) {
				toReturn.add(null);
			}
		}
		return toReturn;
	}
}
