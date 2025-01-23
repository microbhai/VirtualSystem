package vs.servlets;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import akhil.DataUnlimited.DataUnlimitedApi;
import akhil.DataUnlimited.model.types.Types;
import vs.kafka.KafkaCallProcessor;
import vs.util.StringOps;
import vs.util.TemplateProcessor;
import vs.web.ApiCallProcessor;
import vs.web.WebSVMap;

public class DataGenerator extends HttpServlet {
	private static final long serialVersionUID = 1L;

	public DataGenerator() {
		super();
	}

	private static Map<String, ApiCallProcessor> hma = new HashMap<>();
	private static Map<String, KafkaCallProcessor> hmk = new HashMap<>();

	protected void doDelete(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String processorId = request.getParameter("dmsv-processor-id");
		String processorType = request.getParameter("dmsv-processor-type");

		if (processorId != null) {
			if (processorType.equals("KAFKA")) {
				hmk.get(processorId).remove(processorId);
				hmk.remove(processorId);
			} else {
				hma.get(processorId).remove(processorId);
				hma.remove(processorId);
			}

			response.setStatus(200);
			response.getWriter().append("<dmsv-status>Processor removed</dmsv-status>");
		} else {
			response.setStatus(400);
			response.getWriter().append("<dmsv-status>dmsv-processor-id parameter is mandatory</dmsv-status>");
		}
	}

	@Override
	protected void doPut(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		String body = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));
		String dir = StringOps.getInBetweenFast(body, "<dmsv-custom-datatype-param-dir>",
				"</dmsv-custom-datatype-param-dir>", true, false).get(0);

		if (!Types.checkCustomParamDirExists(dir)) {
			String msg = "";
			try {
				new DataUnlimitedApi(dir);
			} catch (Exception e) {
				msg = e.getMessage();
			} finally {
				if (!Types.checkCustomParamDirExists(dir)) {
					response.setStatus(500);
					response.getWriter()
							.append(StringOps.append("<dmsv-status>DMSV-ERROR: ", dir,
									" couldn't be added. It is possible that it couldn't be found on the server. ", msg,
									"</dmsv-status>"));
				} else {
					response.setStatus(201);
					response.getWriter().append(StringOps.append("<dmsv-status>DMSV-SUCCESS: ", dir,
							" added successfully as custom param dir</dmsv-status>"));
				}
			}
		} else {
			response.setStatus(202);
			response.getWriter().append(StringOps.append("<dmsv-status>DMSV-SUCCESS: ", dir,
					" already added to custom param dir list</dmsv-status>"));
		}
	}

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		Enumeration<String> headers = request.getHeaders("dmsv-substitution-argument");
		String numOfFiles = request.getParameter("dmsv-num-of-files");
		String toLog = request.getParameter("dmsv-include-logs");
		String tname = request.getParameter("dmsv-template");
		String processorId = request.getParameter("dmsv-processor-id");
		String processorType = request.getParameter("dmsv-processor-type");
		String messageId = request.getParameter("dmsv-message-id");

		if (processorId != null) {
			if (messageId == null) {
				response.setStatus(200);
				if (processorType.equals("KAFKA"))
					response.getWriter().append(hmk.get(processorId).getResponse());
				else
					response.getWriter().append(hma.get(processorId).getResponse());
			} else {
				response.setStatus(200);
				if (processorType.equals("KAFKA"))
					response.getWriter().append(hmk.get(processorId).getResponse(messageId, processorId));
				else
					response.getWriter().append(hma.get(processorId).getResponse(messageId, processorId));
			}

		} else if (processorType != null) {
			if (processorType.equals("KAFKA")) {
				response.getWriter().append("<processor-id>");
				response.getWriter().append(String.join("</processor-id>\n<processor-id>",hmk.keySet()));
				response.getWriter().append("</processor-id>");
			} else {
				response.getWriter().append("<processor-id>");
				response.getWriter().append(String.join("</processor-id>\n<processor-id>",hma.keySet()));
				response.getWriter().append("</processor-id>");
			}
		} else {

			if (tname != null) {
				String template = TemplateProcessor.getTemplate(tname, true);
				if (template.contains("ERROR")) {
					response.setContentType("text/txt");
					response.setStatus(400);
					response.getWriter().append("DMSV-ERROR: Template not found");

				} else {

					String dmsScript = template;
					while (headers.hasMoreElements()) {
						String value = headers.nextElement();
						if (value.contains("###") && value.startsWith("{") && value.endsWith("}")) {
							List<String> findReplace = StringOps.fastSplit(value, "###");
							String find = findReplace.get(0).substring(1, findReplace.get(0).length() - 1);
							String replace = findReplace.get(1).substring(1, findReplace.get(1).length() - 1);
							dmsScript = dmsScript.replace(find, replace);
						}
					}

					if (numOfFiles == null)
						numOfFiles = "1";

					List<String> result = new DataUnlimitedApi().generateData(dmsScript, numOfFiles);

					if (toLog != null && toLog.equalsIgnoreCase("true")) {
						response.setContentType("text/txt");
						response.setStatus(201);
						response.getWriter().append(result.get(0) + result.get(1));
					} else {
						response.setContentType("text/txt");
						response.setStatus(201);
						response.getWriter().append(result.get(0));
					}
				}
			} else {
				response.setContentType("text/txt");
				response.setStatus(400);
				response.getWriter().append("DMSV-ERROR: No template name provided");
			}
		}
	}

	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		Enumeration<String> headers = request.getHeaders("dmsv-substitution-argument");
		Enumeration<String> headerNumOfFiles = request.getHeaders("dmsv-num-of-files");
		Enumeration<String> headerIncludeLogs = request.getHeaders("dmsv-include-logs");
		String numOfFiles = "1";
		String toLog = "false";

		while (headerNumOfFiles.hasMoreElements()) {
			numOfFiles = headerNumOfFiles.nextElement();
		}
		while (headerIncludeLogs.hasMoreElements()) {
			toLog = headerIncludeLogs.nextElement();
		}

		String body = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));
		List<String> tname = StringOps.getInBetweenFast(body, "<dmsv-template>", "</dmsv-template>", true, true);

		if (!tname.isEmpty()) {
			String postExternal = null;
			String postexternaltype = null;
			List<String> postexternalL = StringOps.getInBetweenFast(body, "<dmsv-post-external>",
					"</dmsv-post-external>", true, false);

			if (postexternalL != null && !postexternalL.isEmpty()) {
				postExternal = postexternalL.get(0);

				List<String> postexternaltypeL = StringOps.getInBetweenFast(body, "<dmsv-post-external-type>",
						"</dmsv-post-external-type>", true, false);
				if (postexternaltypeL != null && !postexternaltypeL.isEmpty())
					postexternaltype = postexternaltypeL.get(0);
				else
					postexternaltype = "API";
			}

			List<String> dmsScriptAddendum = StringOps.getInBetweenFast(body, "<dmsv-script-addendum>",
					"</dmsv-script-addendum>", true, true);

			String template = TemplateProcessor.getTemplate(tname.get(0), false);
			if (template.contains("ERROR")) {
				response.setContentType("text/txt");
				response.setStatus(400);
				response.getWriter().append("DMSV-ERROR: Template not found");

			} else {
				String dmsScript;

				if (!dmsScriptAddendum.isEmpty())
					dmsScript = template + dmsScriptAddendum.get(0);
				else
					dmsScript = template;

				while (headers.hasMoreElements()) {
					String value = headers.nextElement();
					if (value.contains("###") && value.startsWith("{") && value.endsWith("}")) {
						List<String> findReplace = StringOps.fastSplit(value, "###");
						String find = findReplace.get(0).substring(1, findReplace.get(0).length() - 1);
						String replace = findReplace.get(1).substring(1, findReplace.get(1).length() - 1);
						dmsScript = dmsScript.replace(find, replace);
					}
				}

				String id;
				synchronized (hma) {
					String pattern = "yyyy-MM-dd-HH-mm-ss-SSS";
					SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
					id = simpleDateFormat.format(new Date());
				}

				// if (Integer.parseInt(numOfFiles) > 10) {
				String filepath = StringOps.append(WebSVMap.getTempDataGenDir(), File.separator, id);
				new DataUnlimitedApi().generateData(dmsScript, numOfFiles, filepath, "txt");

				response.setContentType("text/txt");
				response.setStatus(201);

				if (postExternal != null) {
					if (postexternaltype.equalsIgnoreCase("API")) {
						ApiCallProcessor ap = new ApiCallProcessor();
						hma.put(id, ap);
						ap.processDataGen(postExternal, filepath, true);
						response.getWriter().append("\n<postexternal-result>\n");
						response.getWriter().append(id);
						response.getWriter().append("\n</postexternal-result>");
					} else if (postexternaltype.equalsIgnoreCase("KAFKA")) {
						KafkaCallProcessor kp = new KafkaCallProcessor();
						hmk.put(id, kp);
						kp.processDataGen(postExternal, filepath, true);
						response.getWriter().append("\n<postexternal-result>\n");
						response.getWriter().append(id);
						response.getWriter().append("\n</postexternal-result>");
					} else {
						response.setStatus(501);
						response.getWriter().append("\n<postexternal-result>\n");
						response.getWriter().append("Post type specified is not supported yet.");
						response.getWriter().append("\n</postexternal-result>");
					}
				} else {
					response.setStatus(400);
					response.getWriter().append("\n<postexternal-result>");
					response.getWriter().append("Post external type not specified");
					response.getWriter().append("\n</postexternal-result>");

				}

				// }
				/*
				 * else { List<String> result = new DMSApi().generateData(dmsScript,
				 * numOfFiles);
				 * 
				 * response.setContentType("text/txt"); response.setStatus(201);
				 * 
				 * if (toLog != null && toLog.equalsIgnoreCase("true"))
				 * response.getWriter().append(result.get(0) + result.get(1)); else
				 * response.getWriter().append(result.get(0));
				 * 
				 * if (postExternal != null) { if (postexternaltype.equalsIgnoreCase("API")) {
				 * ApiCallProcessor ap = new ApiCallProcessor(); hma.put(id, ap);
				 * ap.processDataGen(postExternal, result.get(0), false);
				 * response.getWriter().append("\n<postexternal-result>\n");
				 * response.getWriter().append(id);
				 * response.getWriter().append("\n</postexternal-result>"); } else if
				 * (postexternaltype.equalsIgnoreCase("KAFKA")) { KafkaCallProcessor kp = new
				 * KafkaCallProcessor(); hmk.put(id, kp); kp.processDataGen(postExternal,
				 * result.get(0), false);
				 * response.getWriter().append("\n<postexternal-result>\n");
				 * response.getWriter().append(id);
				 * response.getWriter().append("\n</postexternal-result>"); } else {
				 * response.setStatus(501);
				 * response.getWriter().append("\n<postexternal-result>\n");
				 * response.getWriter().append("Post type specified is not supported yet.");
				 * response.getWriter().append("\n</postexternal-result>"); } } }
				 */
			}
		} else {

			String dmsScript = body;
			while (headers.hasMoreElements()) {
				String value = headers.nextElement();
				if (value.contains("###") && value.startsWith("{") && value.endsWith("}")) {
					List<String> findReplace = StringOps.fastSplit(value, "###");
					String find = findReplace.get(0).substring(1, findReplace.get(0).length() - 1);
					String replace = findReplace.get(1).substring(1, findReplace.get(1).length() - 1);
					dmsScript = dmsScript.replace(find, replace);
				}
			}
			List<String> result = new DataUnlimitedApi().generateData(dmsScript, numOfFiles);

			response.setContentType("text/txt");
			response.setStatus(201);

			if (toLog != null && toLog.equalsIgnoreCase("true"))
				response.getWriter().append(result.get(0) + result.get(1));
			else
				response.getWriter().append(result.get(0));
		}
	}

}
