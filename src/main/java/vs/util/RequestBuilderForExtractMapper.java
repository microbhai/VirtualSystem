package vs.util;

import java.io.IOException;

import java.util.Enumeration;
import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import akhil.DataUnlimited.util.FormatConversion;

public class RequestBuilderForExtractMapper { //// to be enhanced for all types of request types, not just get, put and
												//// post

	public static String buildRequest(HttpServletRequest request, String type) throws ServletException, IOException {
		StringBuilder sb = new StringBuilder();
		Enumeration<String> headerNames = request.getHeaderNames();

		sb.append("<top>\n<dmsv-request-url>");
		sb.append(request.getRequestURL());
		sb.append("</dmsv-request-url>\n");
		sb.append("<dmsv-request-headers>\n");
		while (headerNames.hasMoreElements()) {
			String key = headerNames.nextElement();
			sb.append("<");
			sb.append(key);
			sb.append(">");
			sb.append(request.getHeader(key));
			sb.append("</");
			sb.append(key);
			sb.append(">\n");

		}
		sb.append("</dmsv-request-headers>\n");

		if (type.equalsIgnoreCase("GET")) {
			Enumeration<String> parameterNames = request.getParameterNames();
			sb.append("<dmsv-request-parameters>\n");
			while (parameterNames.hasMoreElements()) {
				String key = parameterNames.nextElement();
				sb.append("<");
				sb.append(key);
				sb.append(">");
				sb.append(request.getParameter(key));
				sb.append("</");
				sb.append(key);
				sb.append(">\n");
			}
			sb.append("</dmsv-request-parameters>\n");
		} else if (type.equalsIgnoreCase("POST") || type.equalsIgnoreCase("PUT") || type.equalsIgnoreCase("DELETE")
				|| type.equalsIgnoreCase("HEAD")) {

			Enumeration<String> parameterNames = request.getParameterNames();
			sb.append("<dmsv-request-parameters>\n");
			while (parameterNames.hasMoreElements()) {
				String key = parameterNames.nextElement();
				sb.append("<");
				sb.append(key);
				sb.append(">");
				sb.append(request.getParameter(key));
				sb.append("</");
				sb.append(key);
				sb.append(">\n");
			}
			sb.append("</dmsv-request-parameters>\n");
			String body = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));

			if (FormatConversion.isJSONValid(body))
				body = FormatConversion.jsonToXML(body);

			sb.append("<dmsv-request-body>\n" + body + "\n</dmsv-request-body>\n");

		} else {
		}

		sb.append("</top>\n");
		return sb.toString();
	}

}
