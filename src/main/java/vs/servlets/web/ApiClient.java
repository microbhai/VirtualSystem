package vs.servlets.web;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import vs.util.StringOps;
import vs.web.ApiCallProcessor;

public class ApiClient extends HttpServlet {
	private static final long serialVersionUID = 1L;

	public ApiClient() {
		super();

	}

	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String reqBody = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));

		String apiName = null;
		List<String> name = StringOps.getInBetweenFast(reqBody, "<apiName>", "</apiName>", true, true);

		String urlPrefixWithServername = null;
		List<String> prefix = StringOps.getInBetweenFast(reqBody, "<urlPrefixWithServername>",
				"</urlPrefixWithServername>", true, true);

		String urlParamString = null;
		List<String> params = StringOps.getInBetweenFast(reqBody, "<urlParamString>", "</urlParamString>", true, true);
		
		String headerString = null;
		List<String> headersProvided = StringOps.getInBetweenFast(reqBody, "<headers>", "</headers>", true, true);
		if (headersProvided != null && !headersProvided.isEmpty())
			headerString = headersProvided.get(0);
		
		String data = null;
		List<String> datas = StringOps.getInBetweenFast(reqBody, "<data>", "</data>", true, true);
		if (datas != null && !datas.isEmpty())
			data = datas.get(0);
		
		String method = null;
		List<String> methods = StringOps.getInBetweenFast(reqBody, "<method>", "</method>", true, true);
		if (methods != null && !methods.isEmpty())
			method = methods.get(0);
		

		if (name != null && !name.isEmpty()) {
			apiName = name.get(0);
			if (prefix != null && !prefix.isEmpty())
				urlPrefixWithServername = prefix.get(0);

			if (params != null && !params.isEmpty())
				urlParamString = params.get(0);

			
			String responseString = null;
			
			if (data!=null)
			{
				if (method!=null && method.equalsIgnoreCase("put"))
					responseString = ApiCallProcessor.process(apiName, urlPrefixWithServername, urlParamString, headerString, data, true);
				else 
					responseString = ApiCallProcessor.process(apiName, urlPrefixWithServername, urlParamString, headerString, data, false);
			}
			else
			{
				if (method!=null && method.equalsIgnoreCase("delete"))
					responseString = ApiCallProcessor.processGetDeleteCall(apiName, urlPrefixWithServername, urlParamString, headerString, true);
				else
					responseString = ApiCallProcessor.processGetDeleteCall(apiName, urlPrefixWithServername, urlParamString, headerString, false);
			}
				

			response.setStatus(200);
			response.getWriter().append(responseString);
		} else {
			response.setStatus(200);
			response.getWriter().append("<dmsv-status>API name not provided, <apiName> is mandatory</dmsv-status>");
		}

	}
}
