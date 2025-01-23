package vs.servlets.kafka;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import vs.kafka.KafkaSVMap;
import vs.util.SQLite;
import vs.util.StringOps;

/**
 * Servlet implementation class FilterValue
 */
public class WaitRelease extends HttpServlet {
	private static final long serialVersionUID = 1L;

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {

		String query = "select id, servicename from waitingmessageid";

		List<List<String>> qr1 = SQLite.getInstance().selectQuery(query, false);

		response.setStatus(200);
		response.getWriter().append("<dmsv-waiting-messages>\n");
		if (!qr1.isEmpty()) {
			for (List<String> ls : qr1) {
				response.getWriter().append("<id>").append(ls.get(0)).append("</id>\n<service-name>").append(ls.get(1)).append("</service-name>\n");
			}
		}
		response.getWriter().append("</dmsv-waiting-messages>\n");
	}

	protected void doDelete(HttpServletRequest request, HttpServletResponse response) throws IOException {

		String query = "delete from waitingmessageid";

		int result = SQLite.getInstance().dmlQuery(query);

		response.setStatus(200);
		response.getWriter().append("<dmsv-status>");
		response.getWriter().append("DELETED ");
		response.getWriter().append(String.valueOf(result));
		response.getWriter().append(" message IDs");
		response.getWriter().append("</dmsv-status>\n");
	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		String reqBody = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));

		List<String> ids = StringOps.getInBetweenFast(reqBody, "<dmsv-message-id>", "</dmsv-message-id>", true, true);

		if (!ids.isEmpty()) {
			response.setStatus(200);
			for (String id : ids) {
				final String idx = id.replace(".txt", "");
				Runnable r1 = () -> KafkaSVMap.removeWaitingMessageId(idx);
				KafkaSVMap.submit(r1);
			}
			response.getWriter().append("<dmsv-status>").append("DMSV-SUCCESS: IDs to be removed asynchronously")
					.append("</dmsv_status>");
		} else
			response.getWriter().append("<dmsv-status>")
					.append("DMSV-ERROR: <dmsv-message-id> tags are mandatory in the request body").append("</dmsv-status>");

	}

}
