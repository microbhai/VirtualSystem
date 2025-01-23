package vs.util;

public class OauthIDSecret {
	private String OAUTH_CLIENT_ID = "not set";
	private String OAUTH_CLIENT_SECRET = "not set";
	private String OAUTH_SCOPE = "not set";
	private String OAUTH_GRANT_TYPE = "not set";

	public String getOauthClientID() {
		return this.OAUTH_CLIENT_ID;
	}

	public String getOauthClientSecret() {
		return this.OAUTH_CLIENT_SECRET;
	}

	public String getOauthScope() {
		return this.OAUTH_SCOPE;
	}

	public String getOauthGrantType() {
		return this.OAUTH_GRANT_TYPE;
	}

	public String deleteOauthCreds(String type) {
		String queryDelete = "delete from VS_OKTA_CREDENTIALS where TYPE = ?";
		String result = StringOps.append("Updated record:",
				String.valueOf(SQLite.getInstance().dmlQuery(queryDelete, type)));
		this.OAUTH_CLIENT_ID = "not set";
		this.OAUTH_CLIENT_SECRET = "not set";
		this.OAUTH_SCOPE = "not set";
		this.OAUTH_GRANT_TYPE = "not set";
		return result;
	}

	public void setOauth(String id, String secret, String type, String scope, String grantType, boolean initialize) {
		this.OAUTH_CLIENT_ID = id;
		this.OAUTH_CLIENT_SECRET = secret;
		this.OAUTH_SCOPE = scope;
		this.OAUTH_GRANT_TYPE = grantType;

		if (!initialize) {
			String queryDelete = "delete from VS_OKTA_CREDENTIALS where TYPE = ?";
			SQLite.getInstance().dmlQuery(queryDelete, type);
			String query = "insert into VS_OKTA_CREDENTIALS (ID, SECRET, TYPE, GRANTTYPE, OAUTHSCOPE ) values (?, ?, ?, ?, ?)";
			SQLite.getInstance().dmlQuery(query, id, secret, type, grantType, scope);
		}

	}

}
