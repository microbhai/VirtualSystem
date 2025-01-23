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
public class FilterValue extends HttpServlet {
	private static final long serialVersionUID = 1L;

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		String name = request.getParameter("dmsv-name");
		if (name != null) {
			response.setStatus(200);
			response.getWriter().append(KafkaSVMap.getFilterValues(name));
		} else {
			response.setStatus(400);
			response.getWriter().append(
					"<dmsv-status>DMSV-ERROR: Required information not found. Fields dmsv-name as request parameter is mandatory.</dmsv-status>");
		}
	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		String reqBody = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));

		List<String> name = StringOps.getInBetweenFast(reqBody, "<dmsv-name>", "</dmsv-name>", true, true);
		List<String> filterValue = StringOps.getInBetweenFast(reqBody, "<dmsv-filter-values>", "</dmsv-filter-values>",
				true, true);
		List<String> filterValueType = StringOps.getInBetweenFast(reqBody, "<dmsv-filter-value-type>",
				"</dmsv-filter-value-type>", true, true);

		if (!name.isEmpty() && !filterValue.isEmpty() && !filterValueType.isEmpty()) {
			String res = KafkaSVMap.setFilterValue(name.get(0).trim(), filterValue.get(0).trim(),
					filterValueType.get(0).trim());
			if (res.contains("SUCCESS")) {
				response.setStatus(201);
				response.getWriter().append(res);
			} else {
				response.setStatus(400);
				response.getWriter().append(res);
			}
		} else {
			response.setStatus(400);
			response.getWriter().append(
					"<dmsv-status>DMSV-ERROR: Required details not found. Fields <dmsv-name>, <dmsv-filter-value> and <dmsv-filter-value-type> are mandatory.</dmsv-status>");
		}

	}

	protected void doPut(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		String reqBody = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));

		List<String> name = StringOps.getInBetweenFast(reqBody, "<dmsv-name>", "</dmsv-name>", true, true);
		List<String> setMandatory = StringOps.getInBetweenFast(reqBody, "<dmsv-set-mandatory>", "</dmsv-set-mandatory>",
				true, true);
		List<String> filterValueType = StringOps.getInBetweenFast(reqBody, "<dmsv-filter-type>", "</dmsv-filter-type>",
				true, true);

		if (!name.isEmpty() && !setMandatory.isEmpty() && !filterValueType.isEmpty()) {
			String res = KafkaSVMap.setFilterMandatory(name.get(0).trim(), setMandatory.get(0).trim(),
					filterValueType.get(0).trim());
			if (res.contains("SUCCESS")) {
				response.setStatus(201);
				response.getWriter().append(res);
			} else {
				response.setStatus(400);
				response.getWriter().append(res);
			}
		} else {
			response.setStatus(400);
			response.getWriter().append(
					"<dmsv-status>DMSV-ERROR: Required details not found. Fields <dmsv-name>, <dmsv-set-mandatory> and <dmsv-filter-value-type> are mandatory.</dmsv-status>");
		}

	}

	protected void doDelete(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		String reqBody = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));
		List<String> name = StringOps.getInBetweenFast(reqBody, "<dmsv-name>", "</dmsv-name>", true, true);
		List<String> filterValue = StringOps.getInBetweenFast(reqBody, "<dmsv-filter-values>", "</dmsv-filter-values>",
				true, true);
		List<String> filterValueType = StringOps.getInBetweenFast(reqBody, "<dmsv-filter-value-type>",
				"</dmsv-filter-value-type>", true, true);
		if (!name.isEmpty() && !filterValueType.isEmpty()) {
			if (filterValue.isEmpty())
				response.getWriter().append(KafkaSVMap.clearFilter(name.get(0).trim(), filterValueType.get(0).trim()));
			else {
				List<String> fvs = StringOps.fastSplit(filterValue.get(0).trim(), "<DMSDELIM>");
				response.getWriter()
						.append(KafkaSVMap.clearFilter(name.get(0).trim(), filterValueType.get(0).trim(), fvs));
			}
		} else
			response.getWriter().append(
					"<dmsv-status>DMSV-ERROR: Required details not found. Fields <dmsv-name> & <dmsv-filter-value-type> are mandatory.</dmsv-status>");
	}

}
