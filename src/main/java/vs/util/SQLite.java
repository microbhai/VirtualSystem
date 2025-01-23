package vs.util;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
//import java.sql.Driver;
import java.util.ArrayList;
//import java.util.Enumeration;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sqlite.SQLiteConfig;

import akhil.DataUnlimited.model.types.Types;
import akhil.DataUnlimited.util.LogStackTrace;

public class SQLite {
	private static final Logger LOGGER = LogManager.getLogger(SQLite.class.getName());
	private static SQLite sqlite;
	private String dbUrl;
	private String DBCLASS;

	private SQLite(String dbUrl) {
		this.dbUrl = dbUrl;
	}

	public static SQLite getInstance(String dbUrl) {
		if (sqlite == null)
			sqlite = new SQLite(dbUrl);
		return sqlite;
	}

	public static SQLite getInstance() {
		return sqlite;
	}

	public String getDBUrl() {
		return this.dbUrl;
	}

	public void setDBCLASS(String s) {
		this.DBCLASS = s;
	}

	public static void closeConnection(Connection con, Statement pstmt, ResultSet rs) {
		try {
			if (rs != null)
				rs.close();
		} catch (Exception e) {
		}
		try {
			if (pstmt != null)
				pstmt.close();
		} catch (Exception e) {
		}
		try {
			if (con != null)
				con.close();
		} catch (Exception e) {
		}
	}

	public Connection connect(String dbConnectionString) {
		try {
			Class.forName(this.DBCLASS);
			Connection conn = null;
			if (this.DBCLASS.contains("sqlite")) {
				SQLiteConfig config = new SQLiteConfig();
				config.enforceForeignKeys(true);
				conn = DriverManager.getConnection(dbConnectionString, config.toProperties());
			} else
				conn = DriverManager.getConnection(dbConnectionString, Types.DBUSR, Types.DBPWD);
			return conn;
		} catch (SQLException e) {
			LOGGER.error(LogStackTrace.get(e));
			return null;
		} catch (Exception ex) {
			LOGGER.error(LogStackTrace.get(ex));
			return null;
		}
	}

	public synchronized Integer ddlQuery(String query) {
		Connection con = null;
		Statement pstmt = null;
		Integer result = null;
		try {
			con = connect(this.dbUrl);
			assert con != null;
			pstmt = con.createStatement();
			result = pstmt.executeUpdate(query);
			LOGGER.info(StringOps.append("SQL Lite result....", String.valueOf(result)));
		} catch (SQLException e) {
			String err = LogStackTrace.get(e);
			if (err.contains("SQLITE_BUSY")) {
				try {
					Thread.sleep(5000);
				} catch (InterruptedException ex) {
				}
				result = ddlQuery(query);
			} else
				LOGGER.error(StringOps.append("SQL Lite exception...\n", err));
		} catch (Exception e) {
			LOGGER.error(StringOps.append("SQL exception...\n", LogStackTrace.get(e)));
		} finally {
			closeConnection(con, pstmt, null);
		}
		return result;
	}

	public synchronized Integer dmlQuery(String query, String... values) {
		Connection con = null;
		PreparedStatement pstmt = null;
		Integer result = null;
		try {
			con = connect(this.dbUrl);
			assert con != null;
			pstmt = con.prepareStatement(query);
			int index = 1;
			if (values.length > 0 && query.contains("?")) {
				for (String s : values) {
					pstmt.setString(index, s);
					index++;
				}
			}
			result = pstmt.executeUpdate();
		} catch (SQLException e) {
			String err = LogStackTrace.get(e);
			if (err.contains("SQLITE_BUSY")) {
				try {
					Thread.sleep(5000);
				} catch (InterruptedException ex) {
				}
				result = dmlQuery(query, values);
			} else
				LOGGER.error(StringOps.append("SQL Lite exception....\n", err));
		} catch (Exception e) {
			LOGGER.error(StringOps.append("SQL exception....\n", LogStackTrace.get(e)));
		} finally {
			closeConnection(con, pstmt, null);
		}
		return result;
	}

	public List<List<String>> selectQuery(String query, boolean includeColumnNames, String... values) {
		List<List<String>> result = new ArrayList<>();
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			con = connect(dbUrl);
			assert con != null;
			pstmt = con.prepareStatement(query);
			int index = 1;
			if (values.length > 0 && query.contains("?")) {
				for (String s : values) {
					pstmt.setString(index, s);
					index++;
				}
			}
			rs = pstmt.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			int hmp = rsmd.getColumnCount();
			if (includeColumnNames) {
				List<String> row = new ArrayList<>();
				for (int i = 1; i <= hmp; i++) {
					row.add(rsmd.getColumnName(i));
				}
			}
			while (rs.next()) {
				List<String> row = new ArrayList<>();
				for (int i = 1; i <= hmp; i++) {
					row.add(rs.getString(i));
				}
				result.add(row);
			}
		} catch (SQLException e) {
			String err = LogStackTrace.get(e);
			if (err.contains("SQLITE_BUSY")) {
				try {
					Thread.sleep(5000);
				} catch (InterruptedException ex) {
				}
				result = selectQuery(query, includeColumnNames, values);
			} else
				LOGGER.error(StringOps.append("SQL Lite exception....\n", err));
		} catch (Exception e) {
			LOGGER.error(StringOps.append("SQL exception....\n", LogStackTrace.get(e)));
		} finally {
			closeConnection(con, pstmt, rs);
		}
		return result;
	}

	public List<List<String>> selectQuery(String query, boolean includeColumnNames) {
		List<List<String>> result = new ArrayList<>();
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			con = connect(dbUrl);
			assert con != null;
			pstmt = con.prepareStatement(query);
			rs = pstmt.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			int hmp = rsmd.getColumnCount();
			if (includeColumnNames) {
				List<String> row = new ArrayList<>();
				for (int i = 1; i <= hmp; i++) {
					row.add(rsmd.getColumnName(i));
				}
			}
			while (rs.next()) {
				List<String> row = new ArrayList<>();
				for (int i = 1; i <= hmp; i++) {
					row.add(rs.getString(i));
				}
				result.add(row);
			}
		} catch (SQLException e) {
			String err = LogStackTrace.get(e);
			if (err.contains("SQLITE_BUSY")) {
				try {
					Thread.sleep(5000);
				} catch (InterruptedException ex) {
				}
				result = selectQuery(query, includeColumnNames);
			} else
				LOGGER.error(StringOps.append("SQL Lite exception....\n", err));

		} catch (Exception e) {
			LOGGER.error(StringOps.append("SQL exception....\n", LogStackTrace.get(e)));
		} finally {
			closeConnection(con, pstmt, rs);
		}
		return result;
	}

	/*
	 * public static void driverDeregister() { Enumeration<Driver> drivers =
	 * DriverManager.getDrivers(); while (drivers.hasMoreElements()) { Driver driver
	 * = drivers.nextElement(); try { DriverManager.deregisterDriver(driver); }
	 * catch (SQLException e) {
	 * LOGGER.error(StringOps.append("SQL Lite exception....\n",
	 * LogStackTrace.get(e))); } catch (Exception e) {
	 * LOGGER.error(StringOps.append("SQL exception....\n", LogStackTrace.get(e)));
	 * } } }
	 */
}
