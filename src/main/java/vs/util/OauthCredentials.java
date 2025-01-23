package vs.util;

import java.util.HashMap;
import java.util.Map;

public class OauthCredentials {

	private static OauthCredentials oc = null;

	public static OauthCredentials getInstance() {
		if (oc == null)
			oc = new OauthCredentials();

		return oc;
	}

	private OauthCredentials() {
	}

	private static Map<String, OauthIDSecret> map = new HashMap<>();

	public String removeCreds(String type) {
		String toReturn = map.get(type).deleteOauthCreds(type);
		map.remove(type);
		return toReturn;
	}

	public void addCreds(String type, OauthIDSecret oauth) {
		map.put(type, oauth);
	}

	public OauthIDSecret getCreds(String type) {
		return map.get(type);
	}

	public Map<String, OauthIDSecret> getCreds() {
		return map;
	}
}
