package vs.util;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TemplateProcessor {
	private static final Logger LOGGER = LogManager.getLogger(TemplateProcessor.class.getName());
	
	private static Map<String, String>lhmaddendum = new LinkedHashMap<String, String>() {
        private static final long serialVersionUID = 1L;

		@Override
        protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
        	int MAX_SIZE=20;
            return size() > MAX_SIZE;
        }
    };
    
    private static Map<String, String>lhm = new LinkedHashMap<String, String>() {
        private static final long serialVersionUID = 1L;

		@Override
        protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
        	int MAX_SIZE=20;
            return size() > MAX_SIZE;
        }
    };
	
	public static int deleteTemplate(String tname) {
		lhmaddendum.remove(tname);
		lhm.remove(tname);
		String query = "delete from TEMPLATE_STORE where NAME = ?";
		int result = SQLite.getInstance().dmlQuery(query, tname);
		return result;
	}

	public static Integer updateTemplate(String tname, String tdes, String script, String addendum) {
		lhmaddendum.remove(tname);
		lhm.remove(tname);
		String query = "update TEMPLATE_STORE set ADDENDUM = ?, DESCRIPTION = ?, DMS_SCRIPT_CONTENT = ? where NAME = ?";
		LOGGER.info(StringOps.append("Update template logging... \n NAME \n", tname,"\n DESCRIPTION \n",tdes,"\n SCRIPT \n",script,"\n ADDENDUM \n ",addendum,"\n Update template LOG END \n"));
		Integer result = SQLite.getInstance().dmlQuery(query, addendum, tdes, script, tname);
		return result;
	}

	public static String getTemplate(String tname, boolean getAddendum) {
		if (getAddendum) {
			if (lhmaddendum.containsKey(tname))
			{
				return lhmaddendum.get(tname);
			}
			else
			{
			String query = "select ADDENDUM, DMS_SCRIPT_CONTENT from TEMPLATE_STORE where NAME = ?";
			List<List<String>> qr = SQLite.getInstance().selectQuery(query, false, tname);
			if (!qr.isEmpty() && !qr.get(0).isEmpty())
			{
				LOGGER.info(StringOps.append("Get template logging... \n NAME \n", tname,"\n ADDENDUM \n",qr.get(0).get(1),"\n SCRIPT \n",qr.get(0).get(0),"\n Update template LOG END \n"));
				return StringOps.append(qr.get(0).get(0), "\n", qr.get(0).get(1));
			}
			else
				return "DMSV-ERROR: NOT FOUND";
			}
		} else {
			if (lhm.containsKey(tname))
			{
				return lhm.get(tname);
			}
			else
			{
			String query = "select DMS_SCRIPT_CONTENT from TEMPLATE_STORE where NAME = ?";
			List<List<String>> qr = SQLite.getInstance().selectQuery(query, false, tname);
			if (!qr.isEmpty() && !qr.get(0).isEmpty())
			{
				LOGGER.info(StringOps.append("Get template logging... \n NAME \n", tname,"\n SCRIPT \n",qr.get(0).get(0),"\n Get template LOG END \n"));
				return qr.get(0).get(0);
			}
			else
				return "DMSV-ERROR: NOT FOUND";
			}
		}
	}

	public static int deleteTemplateInjectorMap(String type, String template, String injector) {

		String query;
		if (type.trim().equals("KAFKA"))
			query = "delete from TEMPLATEINJECTORMAPKAFKA where ";
		else
			query = "delete from TEMPLATEINJECTORMAPAPI where ";

		query = StringOps.append(query, " TEMPLATENAME = ? and INJECTORNAME = ?");

		int result = SQLite.getInstance().dmlQuery(query, template, injector);
		return result;
	}

	public static Integer saveTemplateInjectorMap(String type, String tname, String injectorName) {
		String query;
		if (type.trim().equals("KAFKA"))
			query = "insert into TEMPLATEINJECTORMAPKAFKA(TEMPLATENAME, INJECTORNAME) values (?,?)";
		else
			query = "insert into TEMPLATEINJECTORMAPAPI(TEMPLATENAME, INJECTORNAME) values (?,?)";

		Integer result = SQLite.getInstance().dmlQuery(query, tname, injectorName);
		return result;
	}

	public static String getTemplateInjectorMapSearchResults(String tname, String searchtype) {
		String query;
		List<List<String>> qr;

		if (searchtype.contains("KAFKA"))
			query = "select TEMPLATENAME, INJECTORNAME from TEMPLATEINJECTORMAPKAFKA";
		else
			query = "select TEMPLATENAME, INJECTORNAME from TEMPLATEINJECTORMAPAPI";

		boolean flag = false;
		if (tname != null && searchtype.contains("TEMPLATENAME")) {
			query = StringOps.append(query, " where TEMPLATENAME = ?");
			flag = true;
		}

		if (tname != null && searchtype.contains("POSTEXTERNAL")) {
			query = StringOps.append(query, " where INJECTORNAME = ?");
			flag = true;
		}

		if (flag)
			qr = SQLite.getInstance().selectQuery(query, false, tname);
		else
			qr = SQLite.getInstance().selectQuery(query, false);

		String result = "DMSV-ERROR: NOT FOUND";
		if (!qr.isEmpty() && !qr.get(0).isEmpty()) {
			result = "<search-results>\n";
			for (List<String> ls : qr) {
				result = StringOps.append(result, "<result>", "<templatename>", ls.get(0), "</templatename>",
						"<injectorname>", ls.get(1), "</injectorname></result>\n");
			}
			result = StringOps.append(result, "</search-results>");
		}

		return result;
	}

	public static String getTemplateSearchResults(String tname, String searchtype) {

		String query;
		List<List<String>> qr;
		if (tname != null) {
			if (searchtype != null && searchtype.equals("name")) {
				query = "select NAME, DESCRIPTION from TEMPLATE_STORE where NAME = ?";
				qr = SQLite.getInstance().selectQuery(query, false, tname);
			} else if (searchtype != null && searchtype.equals("namepartial")) {
				query = "select NAME, DESCRIPTION from TEMPLATE_STORE where NAME like ? order by NAME";
				qr = SQLite.getInstance().selectQuery(query, false, "%" + tname + "%");
			} else if (searchtype != null && searchtype.equals("namedescriptionpartial")) {
				query = "select NAME, DESCRIPTION from TEMPLATE_STORE where NAME like ? or DESCRIPTION like ? order by NAME";
				qr = SQLite.getInstance().selectQuery(query, false, "%" + tname + "%", "%" + tname + "%");
			} else if (searchtype != null && searchtype.equals("descriptionpartial")) {
				query = "select NAME, DESCRIPTION from TEMPLATE_STORE where DESCRIPTION like ? order by NAME";
				qr = SQLite.getInstance().selectQuery(query, false, "%" + tname + "%");
			} else {
				query = "select NAME, DESCRIPTION, DMS_SCRIPT_CONTENT, ADDENDUM from TEMPLATE_STORE where NAME = ?";
				qr = SQLite.getInstance().selectQuery(query, false, tname);
			}
		} else {
			query = "select NAME, DESCRIPTION, DMS_SCRIPT_CONTENT, ADDENDUM from TEMPLATE_STORE order by NAME";
			qr = SQLite.getInstance().selectQuery(query, false);
		}

		StringBuilder sb = new StringBuilder();
		sb.append("<dmsv-template-search-results>");

		for (List<String> ls : qr) {
			sb.append("<dmsv-template-search-result>");
			sb.append("<dmsv-template-search-name>");
			sb.append(ls.get(0));
			sb.append("</dmsv-template-search-name>");
			sb.append("<dmsv-template-search-description>");
			sb.append(ls.get(1));
			sb.append("</dmsv-template-search-description>");
			if (ls.size() == 4) {
				sb.append("<dmsv-template-search-dmsvscript>");
				sb.append(ls.get(2));
				sb.append("</dmsv-template-search-dmsvscript>");
				sb.append("<dmsv-template-search-addendum>");
				sb.append(ls.get(3));
				sb.append("</dmsv-template-search-addendum>");
			}
			sb.append("</dmsv-template-search-result>");
		}
		sb.append("</dmsv-template-search-results>");
		
		LOGGER.info(StringOps.append("getTemplateSearchResults template logging... \n payload \n", sb.toString(),"\n getTemplateSearchResults template LOG END \n"));

		return sb.toString();

	}

	public static Integer saveTemplate(String tname, String tdes, String script, String addendum) {
		String query = "insert into TEMPLATE_STORE(NAME, DESCRIPTION, DMS_SCRIPT_CONTENT, ADDENDUM) values (?,?,?,?)";
		LOGGER.info(StringOps.append("Save template logging... \n NAME \n", tname,"\n DESCRIPTION \n",tdes,"\n SCRIPT \n",script,"\n ADDENDUM \n ",addendum,"\n Save template LOG END \n"));
		Integer result = SQLite.getInstance().dmlQuery(query, tname, tdes, script, addendum);
		return result;
	}
}
