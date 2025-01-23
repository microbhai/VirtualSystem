package vs.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class EPW {

	private final Logger LOGGER = LogManager.getLogger(EPW.class.getName());
	private long wait;
	private boolean postGVF = false;
	private boolean isDmsScriptTemplate = false;
	private List<String> pubType = new ArrayList<>();
	private List<String> pubName = new ArrayList<>();
	private String dmsScript = "not set";

	public int getPubSize()
	{
		return pubName.size();
	}
	public String getPubType(int i) {
		return pubType.get(i);
	}

	public String getPubName(int i) {
		return pubName.get(i);
	}

	public boolean isGVF() {
		return postGVF;
	}

	public void setZeroWait() {
		wait = 0;
	}

	public EPW(String dmsScript, List<String> pubConfig, long wait, boolean isDmsScriptTemplate) {
		this.isDmsScriptTemplate = isDmsScriptTemplate;
		this.dmsScript = dmsScript;
		for (String pc : pubConfig) {
			if (pc.contains("<pub-type>") && pc.contains("</pub-type>")
					&& pc.contains("<pub-name>") && pc.contains("</pub-name>")) {
				pubType.add(StringOps.getInBetweenFast(pc, "<pub-type>", "</pub-type>", true, true).get(0).trim());
				pubName.add(StringOps.getInBetweenFast(pc, "<pub-name>", "</pub-name>", true, true).get(0).trim());
			} else if (pc.contains("gvf")) {
				postGVF = true;
			} else {

			}
		}
		this.wait = wait;
	}

	public String getDmsScriptSetting() {
		return dmsScript;
	}

	public String getDmsScript() {

		if (isDmsScriptTemplate) {
			String template = null;
			template = TemplateProcessor.getTemplate(dmsScript, true);
			if (template.contains("ERROR")) {
				LOGGER.error("DMSV-ERROR: EPW DMSV Script Template not found...\n Template name : {}\n",
						new Object[] { dmsScript });
				return null;
			}
			return template;
		} else
			return dmsScript;
	}
	
	public long getWait() {
		return wait;
	}

}
