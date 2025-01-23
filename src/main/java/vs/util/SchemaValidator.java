package vs.util;

import java.io.File;

import java.io.StringReader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import javax.xml.XMLConstants;
import javax.xml.transform.Source;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONException;
import org.json.JSONObject;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class SchemaValidator {
	private static final Logger LOGGER = LogManager.getLogger(SchemaValidator.class.getName());

	public static boolean validate(String response, String schemaOrFileName, boolean isFile) {
		if (schemaOrFileName != null) {
			String str = response.trim();
			if (str.startsWith("{") && str.endsWith("}")) {
				return validateJson(str, schemaOrFileName, isFile);
			} else if (str.startsWith("<") && str.endsWith(">")) {
				return validateXml(str, schemaOrFileName, isFile);
			} else
				return false;
		} else
			return true;
	}

	private static boolean validateJson(String json, String schemaOrFileName, boolean isFile) {
		try {
			String schema = schemaOrFileName;
			if (isFile)
				schema = akhil.DataUnlimited.util.FileOperation.getContentAsString(schemaOrFileName, "UTF-8");
			JSONObject jsonSchema = new JSONObject(schema);
			JSONObject jsonData = new JSONObject(json);

			Schema schm = SchemaLoader.load(jsonSchema);
			schm.validate(jsonData);
			return true;
		} catch (JSONException e) {
			LOGGER.error("DMSV-ERROR: Virtual Response JSON Validation Failed : {}", LogStackTrace.get(e));
			return false;
		} catch (ValidationException e) {
			LOGGER.error("DMSV-ERROR: Virtual Response Schema Validation Failed : {}", LogStackTrace.get(e));
			return false;
		}
	}

	private static boolean validateXml(String xml, String schemaOrFileName, boolean isFile) {
		try {
			Source xmlSrc = new StreamSource(new StringReader(xml));
			SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
			javax.xml.validation.Schema schema;
			if (isFile)
				schema = factory.newSchema(new File(schemaOrFileName));
			else
				schema = factory.newSchema(new SAXSource(new InputSource(new StringReader(schemaOrFileName))));
			Validator validator = schema.newValidator();
			validator.validate(xmlSrc);
			return true;
		} catch (SAXException e) {
			LOGGER.error("DMSV-ERROR: Virtual Response Schema Validation Failed : {}", LogStackTrace.get(e));
			return false;
		} catch (Exception e) {
			LOGGER.error("DMSV-ERROR: Virtual Response Schema Validation Failed : {}", LogStackTrace.get(e));
			return false;
		}
	}

}
