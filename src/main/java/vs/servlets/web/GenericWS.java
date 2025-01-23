package vs.servlets.web;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import akhil.DataUnlimited.DataUnlimitedApi;
import vs.util.RequestBuilderForExtractMapper;
import vs.util.StringOps;
import vs.util.TemplateProcessor;
import vs.web.WebSVMap;

public class GenericWS extends HttpServlet {
	private static final long serialVersionUID = 1L;

	public GenericWS() {
		super();

	}

	private static final Logger LOGGER = LogManager.getLogger(GenericWS.class.getName());

	private static String delim = "+";

	private void getResponse(String type, HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		String url = request.getRequestURI();
		List<String> dmsScriptTemplateAndRcode = WebSVMap.getDMSScriptTemplateAndRCode(url, type);
		String dmsScriptTemplate = dmsScriptTemplateAndRcode.get(0);
		long st = new Date().getTime();
		String responseString;
		if (dmsScriptTemplate == null) {
			responseString = StringOps.append("<dmsv-status>DMSV-ERROR: Service not available @ ", url, delim, type,
					"</dmsv-status>");
			response.setStatus(400);
			response.getWriter().append(responseString);
		} else {
			response.setStatus(Integer.parseInt(dmsScriptTemplateAndRcode.get(1)));
			String requestString = RequestBuilderForExtractMapper.buildRequest(request, type);
			String dmsScript = getDmsScript(dmsScriptTemplate);
			if (dmsScript != null) {
				List<String> responseHeader = StringOps.getInBetweenFast(dmsScript, "<dmsv-response-header>",
						"</dmsv-response-header>", true, true);
				List<String> responseTime = StringOps.getInBetweenFast(dmsScript, "<dmsv-response-time-ms>",
						"</dmsv-response-time-ms>", true, true);
				long wait = 0;
				if (!responseTime.isEmpty())
					wait = Long.parseLong(responseTime.get(0));

				if (!responseHeader.isEmpty()) {
					dmsScript = dmsScript
							.replaceAll(StringOps.append("(?s)" + "<dmsv-response-header>" + "(.+?)" + "</dmsv-response-header>"), "");
					responseString = new DataUnlimitedApi().getVirtualResponse(requestString, dmsScript, "", true,
							true);
					for (String s : responseHeader) {
						String name = StringOps.getInBetweenFast(s, "<name>", "</name>", true, true).get(0);
						String value = StringOps.getInBetweenFast(s, "<value>", "</value>", true, true).get(0);
						if (name.equalsIgnoreCase("Content-Length"))
							value = String.valueOf(responseString.length());
						if (value.contains("<dmsv-header-template>") && value.contains("</dmsv-header-template>")) {
							String headerTemplate = StringOps.getInBetweenFast(s, "<dmsv-header-template>",
									"</dmsv-header-template>", true, true).get(0);
							value = new DataUnlimitedApi().getVirtualResponse(requestString, headerTemplate, "", true,
									true);
						}

						response.addHeader(name, value);
					}
				} else
					responseString = new DataUnlimitedApi().getVirtualResponse(requestString, dmsScript, "", true,
							true);
				long timeSpent = new Date().getTime() - st;

				while (timeSpent < wait) {
					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
					}
					timeSpent = new Date().getTime() - st;
				}

				response.getWriter().append(responseString);

				WebSVMap.getWVS(dmsScriptTemplateAndRcode.get(2)).processAsync(requestString,
						dmsScriptTemplateAndRcode.get(2));
			} else
				responseString = StringOps.append(
						"<dmsv-status>DMSV-ERROR: DMS Script not found for the specified template @ ", url, delim, type,
						"</dmsv-status>");
		}
	}

	public String getDmsScript(String templateName) {
		String template = null;
		template = TemplateProcessor.getTemplate(templateName, true);
		if (template.contains("DMSV-ERROR")) {
			LOGGER.error("DMSV-ERROR: EPW DMSV Script Template not found...\n Template name : {}\n",
					new Object[] { templateName });
			return null;
		}
		return template;
	}

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		getResponse("GET", request, response);
	}

	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		getResponse("POST", request, response);
	}

	@Override
	protected void doPut(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		getResponse("PUT", request, response);
	}

	@Override
	protected void doDelete(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		getResponse("DELETE", request, response);
	}

	@Override
	protected void doHead(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		getResponse("HEAD", request, response);
	}

}
