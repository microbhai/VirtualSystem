package vs.initialize;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import akhil.DataUnlimited.model.types.Types;
import vs.kafka.KafkaSVMap;
import vs.util.SQLite;
import vs.web.WebSVMap;

public class ContextListener implements ServletContextListener {

	public ContextListener() {

	}

	public void contextDestroyed(ServletContextEvent sce) {
		KafkaSVMap.stop();
		WebSVMap.stop();
		// SQLite.driverDeregister();
	}

	public void contextInitialized(ServletContextEvent sce) {
		ServletContext context = sce.getServletContext();

		KafkaSVMap.setLogRequestDir(context.getInitParameter("logdirrequest"));
		KafkaSVMap.setLogResponseDir(context.getInitParameter("logdirresponse"));
		KafkaSVMap.setLogSchemaDir(context.getInitParameter("logdirschema"));
		KafkaSVMap.setLogPostHttpDir(context.getInitParameter("logdirposthttp"));
		KafkaSVMap.setSVTempDir(context.getInitParameter("svtempdir"));
		KafkaSVMap.setTempDataGenDir(context.getInitParameter("tempdatagendir"));

		WebSVMap.setLogRequestDir(context.getInitParameter("weblogdirrequest"));
		WebSVMap.setLogResponseDir(context.getInitParameter("weblogdirresponse"));
		WebSVMap.setLogSchemaDir(context.getInitParameter("weblogdirschema"));
		WebSVMap.setLogPostHttpDir(context.getInitParameter("weblogdirposthttp"));
		WebSVMap.setTempDataGenDir(context.getInitParameter("tempdatagendir"));
		WebSVMap.setSVTempDir(context.getInitParameter("websvtempdir"));

		String dbUrl;
		if (System.getProperty("os.name").toLowerCase().contains("win"))
			dbUrl = context.getInitParameter("dbUrlWindows");
		else
			dbUrl = context.getInitParameter("dbUrlUnix");

		SQLite.getInstance(dbUrl).setDBCLASS(context.getInitParameter("dbclass"));
		Types.setDBURL(dbUrl);
		Types.setDBCLASS(context.getInitParameter("dbclass"));
		String user = context.getInitParameter("dbuser");
		String pwd = context.getInitParameter("dbpwd");
		
		if (user!=null)
			Types.setDBUSR(user);
		
		if (pwd!=null)
			Types.setDBPWD(pwd);
		
		
		KafkaSVMap.initialize();
		WebSVMap.initialize();

	}

}
