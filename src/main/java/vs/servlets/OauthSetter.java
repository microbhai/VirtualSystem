package vs.servlets;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import vs.util.OauthCredentials;
import vs.util.OauthIDSecret;
import vs.util.StringOps;

public class OauthSetter extends HttpServlet {
	private static final long serialVersionUID = 1L;

	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		String reqBody = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));

		List<String> id = StringOps.getInBetweenFast(reqBody, "<dmsv-client-id>", "</dmsv-client-id>", true, true);
		List<String> type = StringOps.getInBetweenFast(reqBody, "<dmsv-oauth-name>", "</dmsv-oauth-name>", true, true);
		List<String> secret = StringOps.getInBetweenFast(reqBody, "<dmsv-client-secret>", "</dmsv-client-secret>", true,
				true);
		List<String> scope = StringOps.getInBetweenFast(reqBody, "<dmsv-scope>", "</dmsv-scope>", true, true);
		List<String> grantType = StringOps.getInBetweenFast(reqBody, "<dmsv-grant-type>", "</dmsv-grant-type>", true,
				true);

		if (!id.isEmpty() && !secret.isEmpty() && !type.isEmpty()) {

			OauthIDSecret o = new OauthIDSecret();
			if (scope.isEmpty() && grantType.isEmpty())
				o.setOauth(id.get(0).trim(), secret.get(0).trim(), type.get(0).trim(), "null", "null", false);
			else if (!scope.isEmpty() && grantType.isEmpty())
				o.setOauth(id.get(0).trim(), secret.get(0).trim(), type.get(0).trim(), scope.get(0).trim(), "null",
						false);
			else if (scope.isEmpty() && !grantType.isEmpty())
				o.setOauth(id.get(0).trim(), secret.get(0).trim(), type.get(0).trim(), "null", grantType.get(0).trim(),
						false);
			else
				o.setOauth(id.get(0).trim(), secret.get(0).trim(), type.get(0).trim(), scope.get(0).trim(),
						grantType.get(0).trim(), false);

			OauthCredentials.getInstance().addCreds(type.get(0).trim(), o);

			response.setStatus(201);
			response.getWriter().append("<dmsv-status>DMSV-SUCCESS: Information updated.</dmsv-status>");

		} else {
			response.setStatus(400);
			response.getWriter().append(
					"<dmsv-status>DMSV-ERROR: Required information not found. Fields <dmsv-oauth-name>, <dmsv-client-id> and <dmsv-client-secret> are mandatory.</dmsv-status>");
		}

	}

	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String name = request.getParameter("dmsv-oauth-name").trim();
		OauthCredentials oc = OauthCredentials.getInstance();
		if (name.equals("FETCH_ALL_CREDS")) {
			Map<String, OauthIDSecret> map = oc.getCreds();
			StringBuilder sb = new StringBuilder();
			for (Map.Entry<String, OauthIDSecret> o : map.entrySet()) {
				if (o != null) {
					sb.append("<dmsv-oauth-name>");
					sb.append(o.getKey());
					sb.append("</dmsv-oauth-name>\n<dmsv-client-id>");
					sb.append(o.getValue().getOauthClientID());
					sb.append("</dmsv-client-id>\n<dmsv-client-secret>");
					sb.append(o.getValue().getOauthClientSecret());
					sb.append("</dmsv-client-secret>\n<dmsv-scope>");
					sb.append(o.getValue().getOauthScope());
					sb.append("</dmsv-scope>\n<dmsv-grant-type>");
					sb.append(o.getValue().getOauthGrantType());
					sb.append("</dmsv-grant-type>\n");
				}
				response.setStatus(200);
				response.getWriter().append("<dmsv-status>\n" + sb.toString() + "</dmsv-status>");
			}
		} else {
			OauthIDSecret o = oc.getCreds(name);
			if (o != null) {
				String resp = StringOps.append("<dmsv-client-id>", o.getOauthClientID(),
						"</dmsv-client-id>\n<dmsv-client-secret>", o.getOauthClientSecret(),
						"</dmsv-client-secret>\n<dmsv-scope>", o.getOauthScope(), "</dmsv-scope>\n<dmsv-grant-type>",
						o.getOauthGrantType(), "</dmsv-grant-type>\n");
				response.setStatus(200);
				response.getWriter().append(StringOps.append("<dmsv-status>\n", resp, "</dmsv-status>"));
			} else {
				response.setStatus(404);
				response.getWriter().append("<dmsv-status>DMSV-ERROR: NOT FOUND</dmsv-status>");
			}
		}
	}

	protected void doDelete(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String name = request.getParameter("dmsv-oauth-name");
		String resp = OauthCredentials.getInstance().removeCreds(name);
		response.setStatus(200);
		response.getWriter().append(StringOps.append("<dmsv-status>\n", resp, "</dmsv-status>"));

	}

}
