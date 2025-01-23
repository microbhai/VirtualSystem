package vs.servlets;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import vs.kafka.KafkaSVMap;
import vs.util.StringOps;
import vs.web.WebSVMap;

public class LogSettings extends HttpServlet {
	private static final long serialVersionUID = 1L;

	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		String reqBody = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));

		List<String> type = StringOps.getInBetweenFast(reqBody, "<dmsv-service-type>", "</dmsv-service-type>", true,
				true);
		List<String> time = StringOps.getInBetweenFast(reqBody, "<dmsv-logfile-retention-ms>",
				"</dmsv-logfile-retention-ms>", true, true);

		if (!type.isEmpty() && !time.isEmpty()) {

			if (type.get(0).equalsIgnoreCase("Kafka")) {
				KafkaSVMap.setLogDeleteTime(Integer.parseInt(time.get(0)));
			} else if (type.get(0).equalsIgnoreCase("web")) {
				WebSVMap.setLogDeleteTime(Integer.parseInt(time.get(0)));
			} else {
				response.setStatus(400);
				response.getWriter().append("<dmsv-status>DMSV-ERROR: Unsupported service type.</dmsv-status>");

			}

			response.setStatus(200);
			response.getWriter().append(
					"<dmsv-status>DMSV-SUCCESS: Log retention time updated (no action if value <= 0).</dmsv-status>");

		} else {
			response.setStatus(400);
			response.getWriter().append(
					"<dmsv-status>DMSV-ERROR: Required information not found. Fields <dmsv-service-type> and <dmsv-logfile-retention-ms> are mandatory.</dmsv-status>");
		}

	}

	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String type = request.getParameter("dmsv-service-type");
		int time = 0;

		if (type != null) {
			boolean errorFlag = false;
			if (type.equalsIgnoreCase("kafka")) {
				time = KafkaSVMap.getLogDeleteTime();
			} else if (type.equalsIgnoreCase("web")) {
				time = WebSVMap.getLogDeleteTime();
			} else {
				errorFlag = true;
				response.setStatus(400);
				response.getWriter()
						.append("<dmsv-status>DMSV-ERROR: Service type not available or supported <dmsv-status>");
			}
			if (!errorFlag) {
				response.setStatus(200);
				response.getWriter().append("<dmsv-status>DMSV-SUCCESS: Log retention time is ");
				response.getWriter().append(String.valueOf(time));
				response.getWriter().append(".</dmsv-status>");
			}
		} else {
			response.setStatus(400);
			response.getWriter()
					.append("<dmsv-status>DMSV-ERROR: Parameter dmsv-service-type (kafka/web) is needed.</dmsv-status>");
		}

	}

}
