package vs.util;

import java.util.ArrayList;
import java.util.List;

public class StringOps {

	public static String append(String... asManyStrings) {
		StringBuilder sb = new StringBuilder();
		for (String s : asManyStrings) {
			sb.append(s);
		}
		return sb.toString();
	}

	public static List<String> fastSplit(String xyz, String delim) {
		List<String> as = new ArrayList<>();
		int index = 0;
		int delimLength = delim.length();
		if (delim.equals("\\t")) {
			char delm = '\t';
			while (index <= xyz.lastIndexOf(delm)) {
				int foundAt = xyz.indexOf(delm, index);
				String x = xyz.substring(index, foundAt);
				as.add(x);
				index = foundAt + 1;
			}
			String toAdd = xyz.substring(index);
			if (!toAdd.isEmpty())
				as.add(toAdd);
		} else {
			while (index <= xyz.lastIndexOf(delim)) {
				int foundAt = xyz.indexOf(delim, index);
				String x = xyz.substring(index, foundAt);
				as.add(x);
				index = foundAt + delimLength;
			}
			String toAdd = xyz.substring(index);
			if (!toAdd.isEmpty())
				as.add(toAdd);
		}
		return as;
	}

	public static List<String> getInBetweenFast(String str, String startPattern, String endPattern,
			boolean excludeStartEndPattern, boolean ignoreEmptyStrings) {
		List<String> toReturn = new ArrayList<>();
		int index = 0;
		int is;
		int ie;

		while (index <= str.length() && str.indexOf(startPattern, index) >= 0 && str.indexOf(endPattern, index) >= 0) {

			if (excludeStartEndPattern) {
				is = str.indexOf(startPattern, index) + startPattern.length();
				ie = str.indexOf(endPattern, is);
				index = ie + +endPattern.length();
			} else {
				is = str.indexOf(startPattern, index);
				ie = str.indexOf(endPattern, is + startPattern.length()) + endPattern.length();
				index = ie;
			}
			if (ie < 0 || is < 0)
				break;

			String toAdd = str.substring(is, ie);
			if (ignoreEmptyStrings) {
				if (!toAdd.isEmpty())
					toReturn.add(toAdd);
			} else
				toReturn.add(toAdd);

		}
		return toReturn;
	}

}
