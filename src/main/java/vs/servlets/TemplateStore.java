package vs.servlets;

import java.io.IOException;

import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import vs.util.StringOps;
import vs.util.TemplateProcessor;

public class TemplateStore extends HttpServlet {
	private static final long serialVersionUID = 1L;

	public TemplateStore() {
		super();
	}

	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		// String responseString = RequestBuilderForExtractMapper.buildRequest(request,
		// "get");
		// response.getWriter().append(responseString);
		String searchtype = request.getParameter("search-type");
		String tname = request.getParameter("dmsv-template");

		String searchResults = TemplateProcessor.getTemplateSearchResults(tname, searchtype);

		response.setContentType("text/txt");
		response.setStatus(200);
		response.getWriter().append(searchResults);
	}

	@Override
	protected void doDelete(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String tname = request.getParameter("dmsv-template");
		if (tname != null) {
			int result = TemplateProcessor.deleteTemplate(tname);
			response.setContentType("text/txt");
			response.setStatus(200);
			response.getWriter().append(StringOps.append("<dmsv-status>", String.valueOf(result),
					" rows deleted in the template store</dmsv-status>"));
		} else {
			response.setContentType("text/txt");
			response.setStatus(404);
			response.getWriter()
					.append("<dmsv-status>No template name found in request parameters (dmsv-template)</dmsv-status>");
		}

	}

	protected void doPut(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String reqBody = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));
		String tname = StringOps.getInBetweenFast(reqBody, "<dmsv-template-name>", "</dmsv-template-name>", true, false)
				.get(0);
		String tdes = StringOps
				.getInBetweenFast(reqBody, "<dmsv-template-description>", "</dmsv-template-description>", true, false)
				.get(0);
		String script = StringOps
				.getInBetweenFast(reqBody, "<dmsv-template-script>", "</dmsv-template-script>", true, false).get(0);
		String addendum = StringOps
				.getInBetweenFast(reqBody, "<dmsv-template-addendum>", "</dmsv-template-addendum>", true, false).get(0);

		if (tname != null && !tname.isEmpty() && tdes != null && !tdes.isEmpty() && script != null && !script.isEmpty()
				&& addendum != null && !addendum.isEmpty()) {
			Integer result = TemplateProcessor.updateTemplate(tname, tdes, script, addendum);
			if (result != null) {
				response.setContentType("text/txt");
				response.setStatus(200);
				response.getWriter().append(StringOps.append("<dmsv-status>", String.valueOf(result),
						" rows updated in the template store</dmsv-status>"));
			} else {
				response.setContentType("text/txt");
				response.setStatus(500);
				response.getWriter().append("<dmsv-status>DMSV-ERROR: in update template, check logs</dmsv-status>");
			}
		} else {
			response.setContentType("text/txt");
			response.setStatus(400);
			response.getWriter().append(
					"<dmsv-status>DMSV-ERROR: Update call needs <dmsv-template-name>, <dmsv-template-description>, <dmsv-template-addendum> and <dmsv-template-script></dmsv-status>");
		}

	}

	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String reqBody = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));
		String tname = StringOps.getInBetweenFast(reqBody, "<dmsv-template-name>", "</dmsv-template-name>", true, false)
				.get(0);
		String tdes = StringOps
				.getInBetweenFast(reqBody, "<dmsv-template-description>", "</dmsv-template-description>", true, false)
				.get(0);
		String script = StringOps
				.getInBetweenFast(reqBody, "<dmsv-template-script>", "</dmsv-template-script>", true, false).get(0);
		String addendum = StringOps
				.getInBetweenFast(reqBody, "<dmsv-template-addendum>", "</dmsv-template-addendum>", true, false).get(0);
		if (tname != null && !tname.isEmpty() && tdes != null && !tdes.isEmpty() && script != null && !script.isEmpty()
				&& addendum != null && !addendum.isEmpty()) {
			Integer result = TemplateProcessor.saveTemplate(tname, tdes, script, addendum);
			if (result != null) {
				response.setContentType("text/txt");
				response.setStatus(201);
				response.getWriter().append(StringOps.append("<dmsv-status>", String.valueOf(result),
						" rows inserted in the template store</dmsv-status>"));
			} else {
				response.setContentType("text/txt");
				response.setStatus(500);
				response.getWriter().append("<dmsv-status>DMSV-ERROR: in update template, check logs</dmsv-status>");
			}

		} else {
			response.setContentType("text/txt");
			response.setStatus(400);
			response.getWriter().append(
					"<dmsv-status>DMSV-ERROR: New template save call needs <dmsv-template-name>, <dmsv-template-description> and <dmsv-template-script></dmsv-status>");
		}

	}
}
