package vs.servlets.kafka;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import vs.kafka.KafkaSVMap;
import vs.util.StringOps;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class KafkaSVRegister extends HttpServlet {
	private static final long serialVersionUID = 1L;

	public KafkaSVRegister() {
		super();
	}

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		String name = request.getParameter("dmsv-name");

		if (name == null)
			response.getWriter().append(KafkaSVMap.getAvailableService());
		else {
			response.getWriter().append(KafkaSVMap.getAvailableService(name));
		}
	}

	protected void doPut(HttpServletRequest request, HttpServletResponse response) throws IOException {

		String reqBody = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));

		List<String> services = StringOps.getInBetweenFast(reqBody, "<dmsv-services>", "</dmsv-services>", true, true);
		List<String> service = StringOps.getInBetweenFast(reqBody, "<dmsv-service>", "</dmsv-service>", true, true);
		List<String> name = StringOps.getInBetweenFast(reqBody, "<dmsv-name>", "</dmsv-name>", true, true);
		List<String> action = StringOps.getInBetweenFast(reqBody, "<dmsv-action>", "</dmsv-action>", true, true);

		if (!services.isEmpty() && !service.isEmpty() && !name.isEmpty() && !action.isEmpty()) {
			response.setStatus(200);
			response.getWriter().append(KafkaSVMap.setServiceStatus(services));
		} else {
			response.setStatus(400);
			response.getWriter().append(
					"<dmsv-status>ERROR: Required field or fields missing in the request.\nUpdating Service Status definition requires <dmsv-services>, <dmsv-service> , <dmsv-name> and <dmsv-action> as mandatory fields.</dmsv-status>");
		}
	}

	protected void doDelete(HttpServletRequest request, HttpServletResponse response) throws IOException {

		String reqBody = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));

		List<String> svname = StringOps.getInBetweenFast(reqBody, "<dmsv-name>", "</dmsv-name>", true, true);

		if (!svname.isEmpty()) {
			response.setStatus(200);
			response.getWriter().append(KafkaSVMap.deleteService(svname));
		} else {
			response.setStatus(400);
			response.getWriter().append(
					"<dmsv-status>ERROR: Required field missing in the request.\nDeleting SV service definition requires <dmsv-name> as mandatory field.</dmsv-status>");
		}
	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {

		String reqBody = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));

		String name;
		List<String> svname = StringOps.getInBetweenFast(reqBody, "<dmsv-name>", "</dmsv-name>", true, true);

		String description = "Not Provided";
		List<String> svdescription = StringOps.getInBetweenFast(reqBody, "<dmsv-description>", "</dmsv-description>",
				true, true);

		String sub;
		List<String> subParam = StringOps.getInBetweenFast(reqBody, "<dmsv-sub>", "</dmsv-sub>", true, true);

		String overwrite = "";
		List<String> ow = StringOps.getInBetweenFast(reqBody, "<dmsv-overwrite-existing>", "</dmsv-overwrite-existing>",
				true, true);

		String epws;
		List<String> epwslist = StringOps.getInBetweenFast(reqBody, "<dmsv-epws>", "</dmsv-epws>", true, true);

		if (!svname.isEmpty() && !subParam.isEmpty() && !epwslist.isEmpty()) {
			name = svname.get(0).trim();
			sub = subParam.get(0).trim();
			epws = epwslist.get(0).trim();
			response.setStatus(201);
			if (!ow.isEmpty()) {
				overwrite = ow.get(0);
				response.setStatus(200);
			}
			if (!svdescription.isEmpty()) {
				description = svdescription.get(0);
			}
			response.getWriter()
					.append(KafkaSVMap.getInstance().registerService(name, description, overwrite, sub, epws));
		} else {
			response.setStatus(400);
			response.getWriter().append(
					"<dmsv-status>ERROR: Required field or fileds missing in the request.\nNew SV service definition requires <dmsv-name>, <dmsv-sub> , <dmsv-epws> as mandatory fields.</dmsv-status>");
		}
	}
}
