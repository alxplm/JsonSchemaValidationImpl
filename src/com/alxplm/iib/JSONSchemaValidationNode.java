package com.alxplm.iib;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;

import com.ibm.broker.plugin.MbElement;
import com.ibm.broker.plugin.MbException;
import com.ibm.broker.plugin.MbInputTerminal;
import com.ibm.broker.plugin.MbJSON;
import com.ibm.broker.plugin.MbMessage;
import com.ibm.broker.plugin.MbMessageAssembly;
import com.ibm.broker.plugin.MbNode;
import com.ibm.broker.plugin.MbNodeInterface;
import com.ibm.broker.plugin.MbOutputTerminal;
import com.ibm.broker.plugin.MbUserException;

public class JSONSchemaValidationNode extends MbNode implements MbNodeInterface {

	private String schemaPath = "";
	private boolean isFullLog;
	
	public String getSchemaPath() {
		return schemaPath;
	}

	public void setSchemaPath(String schemaPath) {
		this.schemaPath = schemaPath;
	}

	public String getIsFullLog() {
		return Boolean.toString(isFullLog);
	}

	public void setIsFullLog(String isFullLog) {
		this.isFullLog = Boolean.parseBoolean(isFullLog);
	}

	public JSONSchemaValidationNode() throws MbException {
		
		createInputTerminal("in");
		createOutputTerminal("failure");
		createOutputTerminal("valid");
		createOutputTerminal("invalid");
	}
	
	public static String getNodeName() {
		
		return "JSONSchemaValidationNode";
	}
	
	@Override
	public void evaluate(MbMessageAssembly assembly, MbInputTerminal inputTerminal)
			throws MbException {

		MbMessage newMsg = new MbMessage(assembly.getMessage());
		MbMessageAssembly newAssembly = new MbMessageAssembly(assembly, newMsg);
		
		InputStream schemaStream = null;
		InputStream dataStream = null;

		try {
			MbElement dataElement = newAssembly.getMessage().getRootElement().getFirstElementByPath("/JSON/Data");
			dataStream = new ByteArrayInputStream(dataElement.toBitstream(null, null, null, 0, 1208, 0));
			
			schemaStream = new FileInputStream(new File(schemaPath));
			
			JSONObject rawSchema = new JSONObject(new JSONTokener(schemaStream));
			Schema schema = SchemaLoader.load(rawSchema);
			
			try {
				schema.validate(new JSONObject(new JSONTokener(dataStream)));
			} catch (ValidationException ve) {
				
				newAssembly.getMessage().getRootElement().getFirstElementByPath("/JSON").delete();
				MbElement jsonRoot = newAssembly.getMessage().getRootElement().createElementAsLastChild("JSON");
				MbElement validationList = jsonRoot.createElementAsLastChild(MbJSON.ARRAY, "Data", null);
				
				if(isFullLog){
					List<String> validationMessages = ve.getAllMessages();
					for(String msg: validationMessages) {
						validationList.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Item", msg);
					}
				} else {
					validationList.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Item", ve.getMessage());
				}
				
				MbOutputTerminal invalidOutputTerminal = getOutputTerminal("invalid");
				invalidOutputTerminal.propagate(newAssembly);
				return;
			}
			
			MbOutputTerminal validOutputTerminal = getOutputTerminal("valid");
			validOutputTerminal.propagate(newAssembly);
			
		} catch (Exception e) {
			throw new MbUserException(this, "evaluate()", "", "", e.toString(), null);
		} finally {
			try {
				if(schemaStream != null) {
					schemaStream.close();
				}
				if(dataStream != null) {
					dataStream.close();
				}
			} catch (IOException ioe) {
				System.err.println(ioe);
			}
		}
	}
}
