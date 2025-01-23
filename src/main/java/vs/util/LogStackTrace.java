package vs.util;

import java.io.PrintWriter;
import java.io.StringWriter;

public class LogStackTrace {
	private LogStackTrace() {
	}

	public static String get(Exception e) {
		StringWriter sw = new StringWriter();
		e.printStackTrace(new PrintWriter(sw));
		return sw.toString();
	}
}
