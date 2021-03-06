package com.tk.bigdata.java_bin.domain;

import java.util.List;

public class DbBean {
	
	private List<String> schemas;
	private List<SchemaBean> schemas_list;
	public List<String> getSchemas() {
		return schemas;
	}
	public void setSchemas(List<String> schemas) {
		this.schemas = schemas;
	}
	public List<SchemaBean> getSchemas_list() {
		return schemas_list;
	}
	public void setSchemas_list(List<SchemaBean> schemas_list) {
		this.schemas_list = schemas_list;
	}
	@Override
	public String toString() {
		return "SchemasBean [schemas=" + schemas + ", schemas_list=" + schemas_list + "]";
	}
	
}
