package vs.servlets;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import vs.util.RequestBuilderForExtractMapper;

public class DMSVEcho extends HttpServlet {
	private static final long serialVersionUID = 1L;

	public DMSVEcho() {
		super();

	}

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		String responseString = RequestBuilderForExtractMapper.buildRequest(request, "get");
		response.getWriter().append(responseString);
	}

	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String responseString = RequestBuilderForExtractMapper.buildRequest(request, "post");
		response.getWriter().append(responseString);
	}

	protected void doPut(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String responseString = RequestBuilderForExtractMapper.buildRequest(request, "put");
		response.getWriter().append(responseString);
	}
}
