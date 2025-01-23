package vs.web;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.json.JSONObject;

import akhil.DataUnlimited.util.FormatConversion;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import vs.util.LogStackTrace;
import vs.util.StringOps;
import okhttp3.Cookie;
import okhttp3.CookieJar;
import okhttp3.FormBody;
import okhttp3.Headers;
import okhttp3.HttpUrl;

public class APICall {

	private OkHttpClient client;

	public APICall() {
		OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder();
		clientBuilder.cookieJar(new CookieJar() {

			private List<Cookie> cookies;

			@Override
			public void saveFromResponse(HttpUrl url, List<Cookie> cookies) {
				this.cookies = cookies;
			}

			@Override
			public List<Cookie> loadForRequest(HttpUrl url) {
				if (cookies != null)
					return cookies;
				return new ArrayList<Cookie>();
			}
		});

		client = clientBuilder.readTimeout(120, TimeUnit.SECONDS).build();
	}

	private static final Logger LOGGER = LogManager.getLogger(APICall.class.getName());

	public String makeCallGetDelete(String url, String token, String headers, String xcsrfToken,
			boolean isDelete_OrGet) {

		Headers h = null;
		List<String> hh = StringOps.getInBetweenFast(headers, "<header>", "</header>", true, true);
		if (token == null && xcsrfToken == null && hh.isEmpty()) {

		} else {
			Headers.Builder builder = new Headers.Builder();
			if (!hh.isEmpty()) {
				for (String hx : hh) {
					List<String> names = StringOps.getInBetweenFast(hx, "<name>", "</name>", true, true);
					List<String> values = StringOps.getInBetweenFast(hx, "<value>", "</value>", true, false);
					if (!names.isEmpty())
						builder.add(names.get(0), values.get(0));
				}
			}
			if (token != null)
				builder.add("Authorization", token);
			if (xcsrfToken != null)
				builder.add("x-csrf-token", xcsrfToken);
			h = builder.build();
		}

		Request request;
		if (isDelete_OrGet) {
			if (h != null)
				request = new Request.Builder().url(url).headers(h).delete().build();
			else
				request = new Request.Builder().url(url).delete().build();
		} else {
			if (h != null)
				request = new Request.Builder().url(url).headers(h).build();
			else
				request = new Request.Builder().url(url).build();
		}

		try {
			Response response = client.newCall(request).execute();
			Headers responseHeaders = response.headers();
			int code = response.code();
			StringBuilder sb = new StringBuilder();
			sb.append("<response>\n");
			sb.append("<code>");
			sb.append(String.valueOf(code));
			sb.append("</code>\n");
			sb.append("<headers>\n");
			for (int i = 0; i < responseHeaders.size(); i++) {
				sb.append("<name>");
				sb.append(responseHeaders.name(i));
				sb.append("</name>");
				sb.append("<value>");
				sb.append(responseHeaders.value(i));
				sb.append("</value>");
			}
			sb.append("\n</headers>\n");
			sb.append("<body>");
			String body = response.body().string().trim();
			if (body.startsWith("{") && body.endsWith("}") && FormatConversion.isJSONValid(body))
				body = FormatConversion.jsonToXML(body);
			sb.append(body);
			sb.append("\n</body>\n");
			sb.append("</response>\n");

			return sb.toString();
		} catch (IOException e) {
			LOGGER.error(StringOps.append("Exception in API Call :", LogStackTrace.get(e)));
			return "DMSV-ERROR: Check logs for more details";
		}
	}

	public String makeCallPostPut(String url, String body, String contentType, String token, String headers,
			String xcsrfToken, boolean isPut_OrPost) {

		if (contentType == null)
			contentType = "application/json";
		RequestBody rbody = RequestBody.create(body, MediaType.parse(contentType));
		Headers h = null;
		List<String> hh = StringOps.getInBetweenFast(headers, "<header>", "</header>", true, true);
		if (token == null && xcsrfToken == null && hh.isEmpty()) {

		} else {
			Headers.Builder builder = new Headers.Builder();

			if (!hh.isEmpty()) {
				for (String hx : hh) {
					List<String> names = StringOps.getInBetweenFast(hx, "<name>", "</name>", true, true);
					List<String> values = StringOps.getInBetweenFast(hx, "<value>", "</value>", true, false);
					if (!names.isEmpty())
						builder.add(names.get(0), values.get(0));
				}
			}
			if (token != null)
				builder.add("Authorization", token);
			if (xcsrfToken != null)
				builder.add("x-csrf-token", xcsrfToken);
			h = builder.build();
		}

		Request request;
		if (isPut_OrPost) {
			if (h != null)
				request = new Request.Builder().url(url).headers(h).put(rbody).build();
			else
				request = new Request.Builder().url(url).put(rbody).build();
		} else {
			if (h != null)
				request = new Request.Builder().url(url).headers(h).post(rbody).build();
			else
				request = new Request.Builder().url(url).post(rbody).build();
		}

		try {
			Response response = client.newCall(request).execute();
			Headers responseHeaders = response.headers();
			int code = response.code();
			StringBuilder sb = new StringBuilder();
			sb.append("<response>\n");
			sb.append("<code>");
			sb.append(String.valueOf(code));
			sb.append("</code>\n");
			sb.append("<headers>\n");
			for (int i = 0; i < responseHeaders.size(); i++) {
				sb.append("<name>");
				sb.append(responseHeaders.name(i));
				sb.append("</name>");
				sb.append("<value>");
				sb.append(responseHeaders.value(i));
				sb.append("</value>");
			}
			sb.append("\n</headers>\n");
			sb.append("<body>");
			body = response.body().string().trim();
			if (body.startsWith("{") && body.endsWith("}") && FormatConversion.isJSONValid(body))
				body = FormatConversion.jsonToXML(body);
			sb.append(body);
			sb.append("\n</body>\n");
			sb.append("</response>\n");

			return sb.toString();
		} catch (IOException e) {
			LOGGER.error(StringOps.append("Exception in API Call :", LogStackTrace.get(e)));
			return "DMSV-ERROR: Check logs for more details";
		}
	}

	public String getXcsrfToken(String url, String token, List<String> headers) {
		Request.Builder requestB = new Request.Builder();
		requestB.url(url);

		if (token != null)
			requestB.addHeader("Authorization", token);

		if (headers != null && !headers.isEmpty()) {
			List<String> xcsrfsHeaders = StringOps.getInBetweenFast(headers.get(0), "<header>", "</header>", true,
					true);
			if (!xcsrfsHeaders.isEmpty()) {
				for (String s : xcsrfsHeaders) {
					List<String> names = StringOps.getInBetweenFast(s, "<name>", "</name>", true, true);
					List<String> values = StringOps.getInBetweenFast(s, "<value>", "</value>", true, false);
					if (!names.isEmpty())
						requestB.addHeader(names.get(0), values.get(0));
				}
			}
		}

		Request request = requestB.build();

		try (Response response = client.newCall(request).execute()) {

			if (!response.isSuccessful())
				throw new IOException(StringOps.append("Exception in XCSRF Token call : ", response.body().string()));

			Headers responseHeaders = response.headers();
			String xcsrftoken = null;
			for (int i = 0; i < responseHeaders.size(); i++) {
				if (responseHeaders.name(i).equalsIgnoreCase("x-csrf-token"))
					xcsrftoken = responseHeaders.value(i);
			}
			LOGGER.info(StringOps.append("XCSRF Token call :", xcsrftoken));
			return xcsrftoken;
		} catch (IOException e) {
			LOGGER.error(StringOps.append("Exception in XCSRF Token call : ", LogStackTrace.get(e)));
			return null;
		}
	}

	public static String getBearerToken(String url, String client_id, String client_secret, String scope,
			String grantType) {

		RequestBody body;
		String responseString = "not set";
		if (scope.equals("null") && grantType.equals("null"))
			body = new FormBody.Builder().add("client_id", client_id).add("client_secret", client_secret)
					.add("grant_type", "client_credentials").build();
		else
			body = new FormBody.Builder().add("client_id", client_id).add("client_secret", client_secret)
					.add("scope", scope).add("grant_type", grantType).add("grant_type", "client_credentials").build();

		OkHttpClient client = new OkHttpClient.Builder().readTimeout(120, TimeUnit.SECONDS).build();
		Request request = new Request.Builder().url(url).post(body).build();
		try {
			responseString = client.newCall(request).execute().body().string();
			JSONObject response = new JSONObject(responseString);
			return response.getString("access_token");
		} catch (Exception e) {
			LOGGER.error(StringOps.append("Exception in bearer token...\n", responseString, LogStackTrace.get(e)));
			return null;
		}
	}

	public static String getDomainName(String url) {
		try {
			URI uri = new URI(url);
			String domain = uri.getHost();
			return domain.startsWith("www.") ? domain.substring(4) : domain;
		} catch (URISyntaxException e) {
			LOGGER.error(StringOps.append("Exception in HOST name/URI Syntax...", LogStackTrace.get(e)));

			int start = url.indexOf("://");
			int end = url.indexOf("/", start + 3);
			String domain = url.substring(start + 3, end);
			return domain.startsWith("www.") ? domain.substring(4) : domain;
		}

	}
}
