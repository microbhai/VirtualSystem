package vs.web;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import akhil.DataUnlimited.util.FileOperation;
import vs.util.OauthCredentials;
import vs.util.SQLite;
import vs.util.SchemaValidator;
import vs.util.StringOps;

public class ApiCallProcessor {

	private List<String> responses = new ArrayList<>();
	private static ExecutorService esfw = Executors.newFixedThreadPool(5);
	private ExecutorService es;
	private static Object lock = new Object();

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
		String filename = StringOps.append(WebSVMap.getTempDataGenDir() , File.separator ,  processorId, File.separator , messageId);
		return FileOperation.getFileContentAsString(filename);
	}
	private void addResponse( String r) {
		synchronized (this.responses) {
			responses.add(r);
		}
	}

	private static void pace(ArrayList<Long> testTimetaken, double tpm, int thread) {
		double timeAvg = testTimetaken.stream().mapToDouble(d -> d).average().getAsDouble();
		long wait = Math.round((60 / (tpm * 1.03) - timeAvg / 1000) * 1000);
		wait = wait / thread;
		if (wait < 0) {
			wait = 0;
			/*System.out.println(
					"WARNING: Test may not be able to achieve required transaction per minute. Increase number of users or test transactions should response faster. Average time to run test steps is (ms): "
							+ timeAvg);*/
		}
		try {
			Thread.sleep(wait);
		} catch (InterruptedException e) {
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

	public void processDataGen(String apiName, String data, boolean isDataFolderPath) {

		ArrayList<Long> testTimetaken = new ArrayList<>();

		String querySelect = "select details from apicalldetails where name = ?";
		List<List<String>> result = SQLite.getInstance().selectQuery(querySelect, false, apiName);
		if (result.isEmpty())
			addResponse(StringOps.append("DMSV-ERROR: No API Call Details found for name: ", apiName));
		else {
			String apiDetails = result.get(0).get(0);
			String url = StringOps.getInBetweenFast(apiDetails, "<url>", "</url>", true, false).get(0);
			List<String> oktaurl = StringOps.getInBetweenFast(apiDetails, "<okta-url>", "</okta-url>", true, true);
			String contentType = StringOps
					.getInBetweenFast(apiDetails, "<content-type>", "</content-type>", true, false).get(0);
			String oktacredsname = StringOps
					.getInBetweenFast(apiDetails, "<okta-creds-name>", "</okta-creds-name>", true, false).get(0);
			String headers = StringOps.getInBetweenFast(apiDetails, "<headers>", "</headers>", false, false).get(0);
			List<String> xcsrfs = StringOps.getInBetweenFast(apiDetails, "<x-csrf-token-url>", "</x-csrf-token-url>",
					true, true);
			List<String> xcsrfsHeaders = StringOps.getInBetweenFast(apiDetails, "<x-csrf-token-headers>",
					"</x-csrf-token-headers>", true, true);
			List<String> tpms = StringOps.getInBetweenFast(apiDetails, "<tpm>", "</tpm>", true, true);
			List<String> threads = StringOps.getInBetweenFast(apiDetails, "<threads>", "</threads>", true, true);

			double tpm = 60;
			if (!tpms.isEmpty())
				tpm = Double.parseDouble(tpms.get(0));

			int thread = 1;
			if (!threads.isEmpty())
				thread = Integer.parseInt(threads.get(0));

			String bearerToken = null;

			if (oktaurl != null && !oktaurl.isEmpty())
				if (!(oktaurl.get(0).equals("NA") || oktaurl.get(0).equals("NONE")))
					bearerToken = StringOps.append("Bearer ",
							APICall.getBearerToken(oktaurl.get(0),
									OauthCredentials.getInstance().getCreds(oktacredsname).getOauthClientID(),
									OauthCredentials.getInstance().getCreds(oktacredsname).getOauthClientSecret(),
									OauthCredentials.getInstance().getCreds(oktacredsname).getOauthScope(),
									OauthCredentials.getInstance().getCreds(oktacredsname).getOauthGrantType()));

			APICall apicall = new APICall();
			String xcsrfToken = null;
			if (xcsrfs != null && !xcsrfs.isEmpty())
				xcsrfToken = apicall.getXcsrfToken(xcsrfs.get(0), bearerToken, xcsrfsHeaders);

			if (isDataFolderPath) {
				es = Executors.newFixedThreadPool(thread);

				List<String> files = FileOperation.getListofFiles(data, true, false);
				
				String responseDir = data;//.replace(WebSVMap.getTempDataGenDir(), WebSVMap.getSVTempDir());

				for (String file : files) {

					final String bearerTokenx = bearerToken;
					final String xcsrfTokenx = xcsrfToken;
					final double tpmx = tpm;
					final int threadx = thread;
					Runnable r = () -> {
						StringBuilder sb = new StringBuilder();
						String payload = FileOperation.getFileContentAsString(file);
						sb.append("<data>\n");
						sb.append(payload);
						sb.append("\n</data>");
						sb.append("\n<response>");
						long st = new Date().getTime();
						sb.append(apicall.makeCallPostPut(url, payload, contentType, bearerTokenx, headers, xcsrfTokenx,
								false));
						sb.append("\n</response>");
						//String id = UUID.randomUUID().toString();
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
					List<String> dataFiles = StringOps.getInBetweenFast(data, "<data-file>", "</data-file>", true,
							true);

					for (String file : dataFiles) {

						final String bearerTokenx = bearerToken;
						final String xcsrfTokenx = xcsrfToken;
						final double tpmx = tpm;
						final int threadx = thread;
						Runnable r = () -> {

							StringBuilder sb = new StringBuilder();

							sb.append("<record><data>\n");
							sb.append(file);
							sb.append("\n</data>");
							sb.append("\n<response>");
							long st = new Date().getTime();
							sb.append(apicall.makeCallPostPut(url, file, contentType, bearerTokenx, headers,
									xcsrfTokenx, false));
							long ed = new Date().getTime();
							testTimetaken.add(ed - st);
							sb.append("\n<response></record>");
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
					StringBuilder sb = new StringBuilder();
					sb.append("<record><data>\n");
					sb.append(data);
					sb.append("\n</data>");
					sb.append("\n<response>");
					sb.append(apicall.makeCallPostPut(url, data, contentType, bearerToken, headers, xcsrfToken, false));
					sb.append("\n</response></record>");
					addResponse(sb.toString());
					
				}
			}
		}
	}

	public static String processGetDeleteCall(String apiName, String urlPrefixWithServername, String urlParamString,
			String headers, boolean isDelete_OrGet) {
		String querySelect = "select details from apicalldetails where name = ?";
		List<List<String>> result = SQLite.getInstance().selectQuery(querySelect, false, apiName);
		if (result.isEmpty())
			return "DMSV-ERROR: No API Call Details found for name: " + apiName;
		else {
			String apiDetails = result.get(0).get(0);
			String url = StringOps.getInBetweenFast(apiDetails, "<url>", "</url>", true, false).get(0);

			if (urlPrefixWithServername != null && !urlPrefixWithServername.isEmpty())
				url = urlPrefixWithServername + url;
			if (urlParamString != null && !urlParamString.isEmpty())
				url = url + urlParamString;

			List<String> oktaurl = StringOps.getInBetweenFast(apiDetails, "<okta-url>", "</okta-url>", true, true);
			String oktacredsname = StringOps
					.getInBetweenFast(apiDetails, "<okta-creds-name>", "</okta-creds-name>", true, false).get(0);
			List<String> headersStored = StringOps.getInBetweenFast(apiDetails, "<headers>", "</headers>", true, true);

			String headersString = "";
			if (headersStored != null && !headersStored.isEmpty())
				headersString = StringOps.append(headersString, headersStored.get(0));
			if (headers != null && !headers.isEmpty())
				headersString = StringOps.append(headersString, headers);

			List<String> xcsrfs = StringOps.getInBetweenFast(apiDetails, "<x-csrf-token-url>", "</x-csrf-token-url>",
					true, true);
			List<String> xcsrfsHeaders = StringOps.getInBetweenFast(apiDetails, "<x-csrf-token-headers>",
					"</x-csrf-token-headers>", true, true);

			String bearerToken = null;

			if (oktaurl != null && !oktaurl.isEmpty())
				if (!(oktaurl.get(0).equals("NA") || oktaurl.get(0).equals("NONE")))
					bearerToken = StringOps.append("Bearer ",
							APICall.getBearerToken(oktaurl.get(0),
									OauthCredentials.getInstance().getCreds(oktacredsname).getOauthClientID(),
									OauthCredentials.getInstance().getCreds(oktacredsname).getOauthClientSecret(),
									OauthCredentials.getInstance().getCreds(oktacredsname).getOauthScope(),
									OauthCredentials.getInstance().getCreds(oktacredsname).getOauthGrantType()));

			APICall apicall = new APICall();
			String xcsrfToken = null;
			if (xcsrfs != null && !xcsrfs.isEmpty())
				xcsrfToken = apicall.getXcsrfToken(xcsrfs.get(0), bearerToken, xcsrfsHeaders);
			if (isDelete_OrGet)
				return apicall.makeCallGetDelete(url, bearerToken, headersString, xcsrfToken, true);
			else
				return apicall.makeCallGetDelete(url, bearerToken, headersString, xcsrfToken, false);
		}

	}

	public static String process(String apiName, String urlPrefixWithServername, String urlParamString,
			String headerString, String data, boolean isPut_OrPost) {

		String headers = "";
		String responseBody = "";
		if (data.contains("<dmsv-body>")) {
			responseBody = StringOps.getInBetweenFast(data, "<dmsv-body>", "</dmsv-body>", true, false).get(0);
			List<String> headerslist = StringOps.getInBetweenFast(data, "<dmsv-headers>", "</dmsv-headers>", true,
					true);

			if (headerString != null && !headerString.isEmpty())
				headers = StringOps.append(headers, headerString);
			if (headerslist != null && !headerslist.isEmpty())
				headers = StringOps.append(headers, headerslist.get(0));

		} else
			responseBody = data;

		String querySelect = "select details from apicalldetails where name = ?";
		List<List<String>> result = SQLite.getInstance().selectQuery(querySelect, false, apiName);
		if (result.isEmpty())
			return "DMSV-ERROR: No API Call Details found for name: " + apiName;
		else {
			String apiDetails = result.get(0).get(0);

			String url = StringOps.getInBetweenFast(apiDetails, "<url>", "</url>", true, false).get(0);

			if (urlPrefixWithServername != null && !urlPrefixWithServername.isEmpty())
				url = urlPrefixWithServername + url;
			if (urlParamString != null && !urlParamString.isEmpty())
				url = url + urlParamString;

			List<String> oktaurl = StringOps.getInBetweenFast(apiDetails, "<okta-url>", "</okta-url>", true, true);
			String contentType = StringOps
					.getInBetweenFast(apiDetails, "<content-type>", "</content-type>", true, false).get(0);
			String oktacredsname = StringOps
					.getInBetweenFast(apiDetails, "<okta-creds-name>", "</okta-creds-name>", true, false).get(0);
			List<String> headersStored = StringOps.getInBetweenFast(apiDetails, "<headers>", "</headers>", true, true);

			if (headersStored != null && !headersStored.isEmpty())
				headers = StringOps.append(headers, headersStored.get(0));

			List<String> xcsrfs = StringOps.getInBetweenFast(apiDetails, "<x-csrf-token-url>", "</x-csrf-token-url>",
					true, true);
			List<String> xcsrfsHeaders = StringOps.getInBetweenFast(apiDetails, "<x-csrf-token-headers>",
					"</x-csrf-token-headers>", true, true);

			List<String> schema = StringOps.getInBetweenFast(apiDetails, "<dmsv-response-schema>",
					"</dmsv-response-schema>", true, true);
			String schemaStr = null;
			if (!schema.isEmpty()) {
				schemaStr = schema.get(0).trim();
			}

			boolean isValidated = SchemaValidator.validate(responseBody, schemaStr, false);
			if (isValidated) {
				String bearerToken = null;

				if (oktaurl != null && !oktaurl.isEmpty())
					if (!(oktaurl.get(0).equals("NA") || oktaurl.get(0).equals("NONE")))
						bearerToken = StringOps.append("Bearer ",
								APICall.getBearerToken(oktaurl.get(0),
										OauthCredentials.getInstance().getCreds(oktacredsname).getOauthClientID(),
										OauthCredentials.getInstance().getCreds(oktacredsname).getOauthClientSecret(),
										OauthCredentials.getInstance().getCreds(oktacredsname).getOauthScope(),
										OauthCredentials.getInstance().getCreds(oktacredsname).getOauthGrantType()));

				APICall apicall = new APICall();
				String xcsrfToken = null;
				if (xcsrfs != null && !xcsrfs.isEmpty())
					xcsrfToken = apicall.getXcsrfToken(xcsrfs.get(0), bearerToken, xcsrfsHeaders);
				if (isPut_OrPost)
					return apicall.makeCallPostPut(url, responseBody, contentType, bearerToken, headers, xcsrfToken,
							true);
				else
					return apicall.makeCallPostPut(url, responseBody, contentType, bearerToken, headers, xcsrfToken,
							false);
			} else {
				return "DMSV-ERROR: Schema validation failed";
			}
		}
	}

	public static String deleteAPICall(String name) {
		String queryDelete = "delete from apicalldetails where name = ?";
		Integer i = SQLite.getInstance().dmlQuery(queryDelete, name);
		if (i != null) {
			return StringOps.append("DMSV-SUCCESS: Deleted ", String.valueOf(i), " rows...");
		} else {
			return "DMSV-ERROR: in delete process... check logs";
		}
	}

	public static String getAPISearchResults(String tname, String searchtype) {

		String query;
		List<List<String>> qr;
		if (tname != null) {
			if (searchtype != null && searchtype.equals("name")) {
				query = "select name, description from apicalldetails where name = ?";
				qr = SQLite.getInstance().selectQuery(query, false, tname);
			} else if (searchtype != null && searchtype.equals("namepartial")) {
				query = "select name, description from apicalldetails where name like ? order by name";
				qr = SQLite.getInstance().selectQuery(query, false, "%" + tname + "%");
			} else if (searchtype != null && searchtype.equals("namedescriptionpartial")) {
				query = "select name, description from apicalldetails where name like ? or DESCRIPTION like ? order by name";
				qr = SQLite.getInstance().selectQuery(query, false, "%" + tname + "%", "%" + tname + "%");
			} else if (searchtype != null && searchtype.equals("descriptionpartial")) {
				query = "select name, description from apicalldetails where DESCRIPTION like ? order by name";
				qr = SQLite.getInstance().selectQuery(query, false, "%" + tname + "%");
			} else {
				query = "select name, description, details from apicalldetails where name = ?";
				qr = SQLite.getInstance().selectQuery(query, false, tname);
			}
		} else {
			query = "select NAME, description, details from apicalldetails order by NAME";
			qr = SQLite.getInstance().selectQuery(query, false);
		}

		StringBuilder sb = new StringBuilder();
		sb.append("<dmsv-api-search-results>");

		for (List<String> ls : qr) {
			sb.append("<dmsv-api-search-result>");
			sb.append("<dmsv-api-search-name>" + ls.get(0) + "</dmsv-api-search-name>");
			sb.append("<dmsv-api-search-description>" + ls.get(1) + "</dmsv-api-search-description>");
			if (ls.size() == 3) {
				sb.append("<dmsv-api-search-details>");
				sb.append(ls.get(2));
				sb.append("</dmsv-api-search-details>");
			}
			sb.append("</dmsv-api-search-result>");

		}
		sb.append("</dmsv-api-search-results>");

		return sb.toString();

	}

	public static String updateAPICall(String name, String details, String description) {

		Integer i;
		if (description.isEmpty()) {
			String query = "update apicalldetails set details = ? where name = ?";
			i = SQLite.getInstance().dmlQuery(query, details, name);
		} else if (details.isEmpty()) {
			String query = "update apicalldetails set description = ? where name = ?";
			i = SQLite.getInstance().dmlQuery(query, description, name);
		} else {
			String query = "update apicalldetails set details = ?, description = ? where name = ?";
			i = SQLite.getInstance().dmlQuery(query, details, description, name);
		}
		if (i != null) {
			return StringOps.append("DMSV-SUCCESS: Updated ", String.valueOf(i), " rows...");
		} else {
			return "DMSV-ERROR: Failed update... check logs";
		}
	}

	public static String saveAPICall(String name, String details, String description) {

		String queryInsert = "insert into apicalldetails (name, description, details) values (?, ?, ?)";
		Integer i = SQLite.getInstance().dmlQuery(queryInsert, name, description, details);
		if (i != null) {
			return StringOps.append("DMSV-SUCCESS: Inserted ", String.valueOf(i), " rows...");
		} else {
			return "DMSV-ERROR: Failed insert... check logs";
		}
	}
}
