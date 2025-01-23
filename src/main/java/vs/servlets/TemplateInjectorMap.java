package vs.servlets;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import vs.util.StringOps;
import vs.util.TemplateProcessor;

public class TemplateInjectorMap extends HttpServlet {
	private static final long serialVersionUID = 1L;

	public TemplateInjectorMap() {
		super();
	}

	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String searchtype = request.getParameter("search-type");
		String tname = request.getParameter("search-value");

		if (searchtype == null) {
			response.setContentType("text/txt");
			response.setStatus(400);
			response.getWriter().append("<dmsv-status>DMSV-ERROR: Parameter search-type is mandatory</dmsv-status>");
		} else {
			String searchResults = TemplateProcessor.getTemplateInjectorMapSearchResults(tname, searchtype);

			if (searchResults.startsWith("ERROR")) {
				response.setContentType("text/txt");
				response.setStatus(400);
				response.getWriter().append("<dmsv-status>").append(searchResults).append("</dmsv-status>");
			} else {
				response.setContentType("text/txt");
				response.setStatus(200);
				response.getWriter().append(searchResults);
			}
		}
	}

	@Override
	protected void doDelete(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String type = request.getParameter("delete-type");
		String injector = request.getParameter("post-external-name");
		String template = request.getParameter("template-name");
		if (type == null || injector == null || template == null) {
			response.setContentType("text/txt");
			response.setStatus(400);
			response.getWriter().append(
					"<dmsv-status>Parameters delete-type (KAFKA, API), template-name, post-external-name are mandatory parameters</dmsv-status>");
		} else {
			int result = TemplateProcessor.deleteTemplateInjectorMap(type, template, injector);
			response.setContentType("text/txt");
			response.setStatus(200);
			response.getWriter().append(StringOps.append("<dmsv-status>", String.valueOf(result),
					" rows deleted in the template store</dmsv-status>"));
		}

	}

	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String reqBody = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));
		List<String> tname = StringOps.getInBetweenFast(reqBody, "<dmsv-template-name>", "</dmsv-template-name>", true,
				true);
		List<String> injectorType = StringOps.getInBetweenFast(reqBody, "<dmsv-post-external-type>",
				"</dmsv-post-external-type>", true, true);
		List<String> injectorName = StringOps.getInBetweenFast(reqBody, "<dmsv-post-external>", "</dmsv-post-external>",
				true, true);

		if (!tname.isEmpty() && !injectorType.isEmpty() && !injectorName.isEmpty()) {
			Integer result = TemplateProcessor.saveTemplateInjectorMap(injectorType.get(0), tname.get(0),
					injectorName.get(0));
			if (result != null) {
				response.setContentType("text/txt");
				response.setStatus(201);
				response.getWriter().append(StringOps.append("<dmsv-status>", String.valueOf(result),
						" rows inserted in the template injector map</dmsv-status>"));
			} else {
				response.setContentType("text/txt");
				response.setStatus(500);
				response.getWriter()
						.append("<dmsv-status>DMSV-ERROR: in template injector map insert, check logs</dmsv-status>");
			}
		} else {
			response.setContentType("text/txt");
			response.setStatus(400);
			response.getWriter().append(
					"<dmsv-status>DMSV-ERROR: New template injector map call needs <dmsv-template-name>, <dmsv-post-external-type> and <dmsv-post-external></dmsv-status>");
		}

	}
}
