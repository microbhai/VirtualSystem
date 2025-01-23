package vs.servlets.kafka;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import vs.kafka.KafkaSVMap;
import vs.util.StringOps;

/**
 * Servlet implementation class FilterValue
 */
public class Throttle extends HttpServlet {
	private static final long serialVersionUID = 1L;

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {

		response.setStatus(200);
		response.getWriter().append("<dmsv-batch-size>").append(String.valueOf(KafkaSVMap.getBatchSize()))
				.append("</dmsv-batch-size>\n").append("<dmsv-wait-per-message>")
				.append(String.valueOf(KafkaSVMap.getWaitPerMessage())).append("</dmsv-wait-per-message>\n")
				.append("<dmsv-max-parallel>").append(String.valueOf(KafkaSVMap.getMaxParallel()))
				.append("</dmsv-max-parallel>\n");

	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		String reqBody = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));

		List<String> restoreDefault = StringOps.getInBetweenFast(reqBody, "<dmsv-restore-defaults>",
				"</dmsv-restore-defaults>", true, true);

		if (restoreDefault.isEmpty())
			updateThrottle(reqBody, response);
		else {
			if (restoreDefault.get(0).equalsIgnoreCase("true")) {
				KafkaSVMap.setBatchSize("15000");
				KafkaSVMap.setMaxParallel("500");
				KafkaSVMap.setWaitPerMessage("500");
			} else {
				updateThrottle(reqBody, response);
			}
		}

	}

	private void updateThrottle(String reqBody, HttpServletResponse response) throws IOException {
		List<String> batch = StringOps.getInBetweenFast(reqBody, "<dmsv-batch-size>", "</dmsv-batch-size>", true, true);
		List<String> wait = StringOps.getInBetweenFast(reqBody, "<dmsv-wait-per-message>", "</dmsv-wait-per-message>",
				true, true);
		List<String> maxParallel = StringOps.getInBetweenFast(reqBody, "<dmsv-max-parallel>", "</dmsv-max-parallel>",
				true, true);

		if (!batch.isEmpty())
			KafkaSVMap.setBatchSize(batch.get(0));

		if (!maxParallel.isEmpty())
			KafkaSVMap.setMaxParallel(maxParallel.get(0));

		if (!wait.isEmpty())
			KafkaSVMap.setWaitPerMessage(wait.get(0));

		if (batch.isEmpty() && wait.isEmpty() && maxParallel.isEmpty()) {
			response.setStatus(400);
			response.getWriter().append(
					"<dmsv-status>DMSV-ERROR: Nothing to update. Either Restore Default OR <dmsv-batch-size>, <dmsv-max-parallel> and/or <dmsv-wait-per-message> are mandatory</dmsv-status>");
		} else {
			response.setStatus(201);
			response.getWriter().append("<dmsv-status>DMSV-SUCCESS: Updated</dmsv-status>");
		}

	}
}
