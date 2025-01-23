package vs.servlets;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import vs.util.SQLite;
import vs.util.StringOps;

public class SQLiteDataLoader extends HttpServlet {
	private static final long serialVersionUID = 1L;

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {

		String name = request.getParameter("sqlite-table-name");
		if (name != null) {
			response.setStatus(200);
			List<List<String>> result = SQLite.getInstance().selectQuery("SELECT * FROM " + name, true);
			if (result.isEmpty()) {
				response.getWriter().append("<dmsv-status>Not data/table found</dmsv-status>");
			} else {
				response.getWriter().append("<dmsv-status>\n");
				StringBuilder sb = new StringBuilder();
				for (List<String> row : result) {
					for (String s : row) {
						sb.append(s);
						sb.append("<DMSDELIM>");
					}
					sb.append("\n");
				}
				response.getWriter().append(sb.toString());
				response.getWriter().append("</dmsv-status>");
			}
		} else {
			response.setStatus(400);
			response.getWriter().append(
					"<dmsv-status>DMSV-ERROR: Required information not found. Fields sqlite-table-name as request parameter is mandatory.</dmsv-status>");
		}

	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String reqBody = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));

		if (reqBody.contains("<dms-sqlite-table>") && reqBody.contains("</dms-sqlite-table>")) {
			List<String> dbDetails = StringOps.getInBetweenFast(reqBody, "<dms-sqlite-table>", "</dms-sqlite-table>",
					true, true);
			for (String detail : dbDetails) {
				if (!detail.contains("<dms-tablename>") || !detail.contains("</dms-tablename>")
						|| !detail.contains("<dms-delimiter>") || !detail.contains("</dms-delimiter>")
						|| !detail.contains("<dms-columnnames>") || !detail.contains("</dms-columnnames>")
						|| !detail.contains("<dms-tabledata>") || !detail.contains("</dms-tabledata>")) {
					String msg = "DMSV-ERROR: Improper sqlite table definition found. Table definitions should have <dms-tablename>, <dms-delimiter>, <dms-columnnames> and <dms-tabledata> ...\n";
					response.setStatus(400);
					response.getWriter().append("<dmsv-status>" + msg + "</dmsv-status>\n");

				} else {

					String delim = StringOps.getInBetweenFast(detail, "<dms-delimiter>", "</dms-delimiter>", true, true)
							.get(0).trim();
					String tablename = StringOps
							.getInBetweenFast(detail, "<dms-tablename>", "</dms-tablename>", true, true).get(0).trim();
					List<String> columnnames = StringOps.fastSplit(
							StringOps.getInBetweenFast(detail, "<dms-columnnames>", "</dms-columnnames>", true, true)
									.get(0).trim(),
							delim);

					StringBuilder sb = new StringBuilder();
					sb.append("CREATE TABLE ");

					sb.append(tablename);
					sb.append("( ");
					for (String col : columnnames) {
						sb.append(col);
						sb.append(" TEXT NOT NULL, ");
					}
					sb.append(")");
					String query = sb.toString().replace(", )", ")");

					Integer createResult = SQLite.getInstance().ddlQuery(query);
					if (createResult != null && createResult == 0) {
						List<String> datarows = StringOps.fastSplit(
								StringOps.getInBetweenFast(detail, "<dms-tabledata>", "</dms-tabledata>", true, true)
										.get(0).trim().replace("\r", ""),
								"\n");
						sb = new StringBuilder();
						sb.append("INSERT INTO ");
						sb.append(tablename);
						sb.append(" VALUES (");
						for (int i = 0; i < columnnames.size(); i++) {
							sb.append("?, ");
						}
						query = sb.toString().trim();
						query = query.substring(0, query.length() - 1) + ")";
						for (String datarow : datarows) {
							List<String> data = StringOps.fastSplit(datarow, delim);
							SQLite.getInstance().dmlQuery(query, data.toArray(new String[0]));
						}

						response.setStatus(201);
						response.getWriter().append("<dmsv-status>Table created " + tablename + "</dmsv-status>\n");
					} else {
						response.setStatus(200);
						response.getWriter().append("<dmsv-status>Table already exists " + tablename
								+ ". Drop and recreate the table with new data.</dmsv-status>\n");
					}
				}
			}
		} else {
			response.setStatus(400);
			response.getWriter().append(
					"<dmsv-status>DMSV-ERROR: Required details not found. Tags <dms-sqlite-table> and </dms-sqlite-table> are mandatory.</dmsv-status>");
		}

	}

	protected void doDelete(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String name = request.getParameter("sqlite-table-name");

		if (name != null) {
			response.setStatus(200);
			Integer result = SQLite.getInstance().ddlQuery("DROP TABLE " + name);
			if (result != null && result == 0)
				response.getWriter().append("<dmsv-status>DMSV-SUCCESS: Table dropped</dmsv-status>");
			else
				response.getWriter().append("<dmsv-status>DMSV-ERROR: Table not found</dmsv-status>");
		} else {
			response.setStatus(400);
			response.getWriter().append(
					"<dmsv-status>DMSV-ERROR: Required information not found. Fields sqlite-table-name as request parameter is mandatory.</dmsv-status>");
		}

	}
}
