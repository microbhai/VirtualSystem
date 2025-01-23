package vs.servlets.web;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import vs.util.StringOps;
import vs.web.WebSVMap;

public class WebSVRegister extends HttpServlet {
	private static final long serialVersionUID = 1L;

	public WebSVRegister() {
		super();
	}

	@Override
	protected void doDelete(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String reqBody = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));

		String url = "";
		List<String> urls = StringOps.getInBetweenFast(reqBody, "<dmsv-url>", "</dmsv-url>", true, true);

		String rco = "";
		List<String> rc = StringOps.getInBetweenFast(reqBody, "<dmsv-response-code>", "</dmsv-response-code>", true,
				true);

		String type = "";
		List<String> types = StringOps.getInBetweenFast(reqBody, "<dmsv-method>", "</dmsv-method>", true, true);

		if (!urls.isEmpty() && !types.isEmpty()) {
			url = urls.get(0).trim();
			type = types.get(0).trim();

			if (!rc.isEmpty())
				rco = rc.get(0).trim();

			response.getWriter().append(WebSVMap.deleteService(url, type, rco));
		} else {
			response.setStatus(400);
			response.getWriter().append(
					"<dmsv-status>DMSV-ERROR: Required fields missing in the request <dmsv-url>, <dmsv-method> and <dmsv-response-code> </dmsv-status>");
		}
	}

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String key = request.getParameter("dmsv-service-key");
		if (key == null) {
			String services = WebSVMap.getAvailableServices().replace("{}", "DMS-CURLY");
			response.getWriter().append(services);
		} else {
			key = key.replace("DELIMKEY", "+");
			key = key.replace("DMS-CURLY", "{}");
			String service = WebSVMap.getAvailableServices(key).replace("{}", "DMS-CURLY");
			response.getWriter().append(service);
		}
	}

	@Override
	protected void doPut(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		String reqBody = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));

		String url = "";
		List<String> urls = StringOps.getInBetweenFast(reqBody, "<dmsv-url>", "</dmsv-url>", true, true);

		String rco = "";
		List<String> rc = StringOps.getInBetweenFast(reqBody, "<dmsv-response-code>", "</dmsv-response-code>", true,
				true);
		String setRCode = "false";
		List<String> rcd = StringOps.getInBetweenFast(reqBody, "<dmsv-set-default-response-code>",
				"</dmsv-set-default-response-code>", true, true);

		String type = "";
		List<String> types = StringOps.getInBetweenFast(reqBody, "<dmsv-method>", "</dmsv-method>", true, true);

		String action = "";
		List<String> actions = StringOps.getInBetweenFast(reqBody, "<dmsv-action>", "</dmsv-action>", true, true);

		String dmsScriptTemplate = "";
		List<String> scripts = StringOps.getInBetweenFast(reqBody, "<dmsv-response-template>",
				"</dmsv-response-template>", true, true);

		String epws = null;
		List<String> epwslist = StringOps.getInBetweenFast(reqBody, "<dmsv-epws>", "</dmsv-epws>", true, true);

		if (!epwslist.isEmpty())
			epws = epwslist.get(0).trim();

		if (!urls.isEmpty() && !types.isEmpty()) {
			url = urls.get(0).trim();
			type = types.get(0).trim();

			if (!rco.isEmpty())
				rco = rc.get(0).trim();

			if (!actions.isEmpty()) {
				action = actions.get(0);
				String resp = WebSVMap.updateService(url, type, action);
				if (resp.contains("ERROR"))
					response.setStatus(400);
				else
					response.setStatus(200);
				response.getWriter().append(resp);

			} else if (!scripts.isEmpty()) {
				if (rco.isEmpty()) {
					response.setStatus(400);
					response.getWriter().append(
							"<dmsv-status>DMSV-ERROR: <dmsv-action> is not specified and <dmsv-response-template> is provided, hence  and <dmsv-response-code> is mandatory (200/400/500, etc.)</dmsv-status>");
				} else {
					dmsScriptTemplate = scripts.get(0).trim();
					if (!rcd.isEmpty())
						setRCode = rcd.get(0).trim();
					String resp = WebSVMap.updateService(url, type, rco, dmsScriptTemplate, epws, setRCode);
					if (resp.contains("ERROR"))
						response.setStatus(400);
					else
						response.setStatus(200);
					response.getWriter().append(resp);
				}

			} else {
				if (rco.isEmpty()) {
					response.setStatus(400);
					response.getWriter().append(
							"<dmsv-status>DMSV-ERROR: <dmsv-action> and <dmsv-response-template> are not specified, hence  and <dmsv-response-code> is mandatory (200/400/500, etc.) to update the response code</dmsv-status>");
				} else {

					String result = WebSVMap.updateRCode(url, type, rco);
					if (result.contains("SUCCESS")) {
						response.setStatus(200);
						response.getWriter().append("<dmsv-status>DMSV-SUCCESS: Response Code Updated</dmsv-status>");
					} else {
						response.setStatus(400);
						response.getWriter().append("<dmsv-status>");
						response.getWriter().append(result);
						response.getWriter().append("</dmsv-status>");
					}
				}

			}
		} else {
			response.setStatus(400);
			response.getWriter().append(
					"<dmsv-status>DMSV-ERROR: Required fields missing <dmsv-url> and <dmsv-method> (POST/GET/PUT etc.)</dmsv-status>");
		}

	}

	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String reqBody = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));

		String url = "";
		List<String> urls = StringOps.getInBetweenFast(reqBody, "<dmsv-url>", "</dmsv-url>", true, true);

		String overwrite = "";
		List<String> ow = StringOps.getInBetweenFast(reqBody, "<dmsv-overwrite-existing>", "</dmsv-overwrite-existing>",
				true, true);

		String rco = "";
		List<String> rc = StringOps.getInBetweenFast(reqBody, "<dmsv-response-code>", "</dmsv-response-code>", true,
				false);

		String setRCode = "";
		List<String> rcd = StringOps.getInBetweenFast(reqBody, "<dmsv-set-default-response-code>",
				"</dmsv-set-default-response-code>", true, true);

		String type = "";
		List<String> types = StringOps.getInBetweenFast(reqBody, "<dmsv-method>", "</dmsv-method>", true, true);

		String dmsScriptTemplate = "";
		List<String> scripts = StringOps.getInBetweenFast(reqBody, "<dmsv-response-template>",
				"</dmsv-response-template>", true, true);

		String epws = null;
		List<String> epwslist = StringOps.getInBetweenFast(reqBody, "<dmsv-epws>", "</dmsv-epws>", true, true);

		if (!epwslist.isEmpty())
			epws = epwslist.get(0).trim();

		if (!urls.isEmpty() && !types.isEmpty() && !scripts.isEmpty() && !rc.isEmpty() && !rcd.isEmpty()) {
			url = urls.get(0).trim();
			type = types.get(0).trim();
			dmsScriptTemplate = scripts.get(0).trim();
			rco = rc.get(0).trim();
			setRCode = rcd.get(0).trim();
			if (!ow.isEmpty())
				overwrite = ow.get(0);

			if (!rco.isEmpty() && !setRCode.isEmpty()) {
				response.getWriter()
						.append(WebSVMap.registerService(url, overwrite, type, rco, dmsScriptTemplate, epws, setRCode));
			} else {
				response.setStatus(400);
				response.getWriter().append(
						"<dmsv-status>DMSV-ERROR: Required fields missing <dmsv-response-code> (http code, 200/400/201 etc) and/or <dmsv-set-default-response-code> (true/false)</dmsv-status>");
			}
		} else {
			response.setStatus(400);
			response.getWriter().append(
					"<dmsv-status>DMSV-ERROR: Required fields missing in the request.\nNew service definition requires <dmsv-url>, <dmsv-method>(get, post etc), <dmsv-response-code> (http code, 200/400/201 etc), <dmsv-response-code-default> (true/false) and <dmsv-script> as mandatory fields.\n<dmsv-overwrite-existing> (true/false) is optional</dmsv-status>");
		}
	}
}
