package vs.servlets;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import vs.util.StringOps;
import vs.web.ApiCallProcessor;

public class APICallRegister extends HttpServlet {
	private static final long serialVersionUID = 1L;

	public APICallRegister() {
		super();
	}

	@Override
	protected void doDelete(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String name_ = request.getParameter("names");
		List<String> names = StringOps.fastSplit(name_, ",");

		if (!names.isEmpty()) {

			StringBuilder sb = new StringBuilder();

			for (String name : names) {

				sb.append(ApiCallProcessor.deleteAPICall(name));

			}

			String result = sb.toString();
			if (result.contains("ERROR")) {
				response.setStatus(500);
				response.getWriter().append(StringOps.append("<status>DMSV-ERROR:", result, "</status>"));
			} else {
				response.setStatus(200);
				response.getWriter().append("<status>DMSV-SUCCESS</status>");
			}
		} else {

			response.setStatus(400);
			response.getWriter().append(
					"<status>DMSV-ERROR: Required fields missing in the request. <name></name> is mandatory.</status>");
		}
	}

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String searchtype = request.getParameter("search-type");
		String tname = request.getParameter("dmsv-api");

		String searchResults = ApiCallProcessor.getAPISearchResults(tname, searchtype);

		response.setContentType("text/txt");
		response.setStatus(200);
		response.getWriter().append(searchResults);
	}

	@Override
	protected void doPut(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String reqBody = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));

		List<String> calls = StringOps.getInBetweenFast(reqBody, "<apicall>", "</apicall>", true, true);

		if (!calls.isEmpty()) {

			for (String call : calls) {
				List<String> name = StringOps.getInBetweenFast(call, "<name>", "</name>", true, true);
				List<String> details = StringOps.getInBetweenFast(call, "<details>", "</details>", true, true);
				List<String> descriptions = StringOps.getInBetweenFast(call, "<description>", "</description>", true,
						true);

				if (name.isEmpty() || details.isEmpty()) {
					response.setStatus(400);
					response.getWriter().append(
							"<status>DMSV-ERROR: Required fields for API Call are missing in the request. <name></name> and <details></details> are mandatory.</status>");
				} else {

					String details_ = "";
					if (!details.isEmpty())
						details_ = details.get(0).trim();

					List<String> url = StringOps.getInBetweenFast(details_, "<url>", "</url>", true, true);

					List<String> contenttype = StringOps.getInBetweenFast(details_, "<content-type>", "</content-type>",
							true, true);
					List<String> headers = StringOps.getInBetweenFast(details_, "<headers>", "</headers>", true, true);
					
					List<String> oktaurl = StringOps.getInBetweenFast(details_, "<okta-url>", "</okta-url>", true,
							true);
					List<String> oktacreds = StringOps.getInBetweenFast(details_, "<okta-creds-name>",
							"</okta-creds-name>", true, true);

					if (url.isEmpty() || contenttype.isEmpty() || oktaurl.isEmpty() || oktacreds.isEmpty()
							|| headers.isEmpty()) {

						response.setStatus(400);
						response.getWriter().append(
								"<status>DMSV-ERROR: Required fields missing in the details. <url>, <okta-url>, <okta-creds-name>, <content-type> and <headers> are mandatory.</status>");

					} else {

						String name_ = name.get(0).trim();
						String descriptions_ = "";

						if (!descriptions.isEmpty())
							descriptions_ = descriptions.get(0).trim();

						String result = ApiCallProcessor.updateAPICall(name_, details_, descriptions_);
						if (result.contains("ERROR")) {
							response.setStatus(500);
							response.getWriter().append(StringOps.append("<status>DMSV-ERROR: ", result, "</status>"));
						} else {
							response.setStatus(200);
							response.getWriter().append("<status>SUCCESS</status>");
						}
					}
				}
			}
		} else {
			response.setStatus(400);
			response.getWriter().append(
					"<status>DMSV-ERROR: Required fields missing in the request. <apicall></apicall> is mandatory.</status>");
		}
	}

	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String reqBody = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));

		List<String> calls = StringOps.getInBetweenFast(reqBody, "<apicall>", "</apicall>", true, true);

		if (!calls.isEmpty()) {

			for (String call : calls) {
				List<String> name = StringOps.getInBetweenFast(call, "<name>", "</name>", true, true);
				List<String> details = StringOps.getInBetweenFast(call, "<details>", "</details>", true, true);
				List<String> descriptions = StringOps.getInBetweenFast(call, "<description>", "</description>", true,
						true);

				if (name.isEmpty() || details.isEmpty()) {
					response.setStatus(400);
					response.getWriter().append(
							"<status>DMSV-ERROR: Required fields for API Call are missing in the request. <name></name> and <details></details> are mandatory.</status>");
				} else {

					String details_ = "";
					if (!details.isEmpty())
						details_ = details.get(0).trim();

					List<String> url = StringOps.getInBetweenFast(details_, "<url>", "</url>", true, true);

					List<String> contenttype = StringOps.getInBetweenFast(details_, "<content-type>", "</content-type>",
							true, true);
					List<String> headers = StringOps.getInBetweenFast(details_, "<headers>", "</headers>", true, true);
					List<String> oktaurl = StringOps.getInBetweenFast(details_, "<okta-url>", "</okta-url>", true,
							true);
					List<String> oktacreds = StringOps.getInBetweenFast(details_, "<okta-creds-name>",
							"</okta-creds-name>", true, true);

					if (url.isEmpty() || contenttype.isEmpty() || oktaurl.isEmpty() || oktacreds.isEmpty()
							|| headers.isEmpty()) {

						response.setStatus(400);
						response.getWriter().append(
								"<status>DMSV-ERROR: Required fields missing in the details. <url>, <okta-url>, <okta-creds-name>, <content-type> and <headers> are mandatory.</status>");

					} else {

						String name_ = name.get(0).trim();
						String descriptions_ = "";

						if (!descriptions.isEmpty())
							descriptions_ = descriptions.get(0).trim();

						String result = ApiCallProcessor.saveAPICall(name_, details_, descriptions_);
						if (result.contains("DMSV-ERROR")) {
							response.setStatus(500);
							response.getWriter().append(StringOps.append("<status>DMSV-ERROR: ", result, "</status>"));
						} else {
							response.setStatus(200);
							response.getWriter().append("<status>DMSV-SUCCESS</status>");
						}
					}
				}
			}

		} else {
			response.setStatus(400);
			response.getWriter().append(
					"<status>DMSV-ERROR: Required fields missing in the request. <apicall></apicall> is mandatory.</status>");
		}
	}
}
