package com.tk.bigdata.java_bin.domain;

public class SqoopBean {
	
	private String name;
	
	private String driver;
	private String url;
	private String username;
	private String password;
	private String schema;
	private String tableName;

	private Integer numMappers;
	private String hbaseTableName;
	private String where;
	private String colums;
	private String columnFamily;
	private String rowKey;
	private String splitBy;
	private String addRowKey;
	private String init;

	private String beforeImport;
	
	private boolean status;

	public SqoopBean(LinkBean linkBean, boolean dbStatus, String schema, TableBean tableBean, int i) {
		if(linkBean != null){
			this.driver = linkBean.getDriver();
			this.url = linkBean.getUrl();
			this.username = linkBean.getUsername();
			this.password = linkBean.getPassword();
		}
		this.schema = schema;
		if(tableBean != null){
			this.tableName = tableBean.getTable();
			this.hbaseTableName = tableBean.getHbase_table();
			this.numMappers = tableBean.getNum_mappers();
			this.where = tableBean.getWhere();
			this.colums = tableBean.getColumns();
			this.columnFamily = tableBean.getColumn_family();
			this.rowKey = tableBean.getRow_key();
			this.splitBy = tableBean.getSplit_by();
			this.addRowKey = tableBean.getAdd_row_key();
			this.beforeImport = tableBean.getBefore_import();
			this.init = tableBean.getInit();
			
//			this.name = schema + "_" + tableBean.getTable() + "_" + i;
			if(schema == null){
				this.name = tableBean.getTable();
			}else{
				this.name = schema + "_" + tableBean.getTable();
			}
		}else{
			this.name = schema + "_" + i;
		}
		this.status = dbStatus;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDriver() {
		return driver;
	}

	public void setDriver(String driver) {
		this.driver = driver;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public Integer getNumMappers() {
		return numMappers;
	}

	public void setNumMappers(Integer numMappers) {
		this.numMappers = numMappers;
	}

	public String getHbaseTableName() {
		return hbaseTableName;
	}

	public void setHbaseTableName(String hbaseTableName) {
		this.hbaseTableName = hbaseTableName;
	}

	public String getWhere() {
		return where;
	}

	public void setWhere(String where) {
		this.where = where;
	}

	public String getColums() {
		return colums;
	}

	public void setColums(String colums) {
		this.colums = colums;
	}

	public String getColumnFamily() {
		return columnFamily;
	}

	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
	}

	public String getRowKey() {
		return rowKey;
	}

	public void setRowKey(String rowKey) {
		this.rowKey = rowKey;
	}

	public String getSplitBy() {
		return splitBy;
	}

	public void setSplitBy(String splitBy) {
		this.splitBy = splitBy;
	}

	public String getAddRowKey() {
		return addRowKey;
	}

	public void setAddRowKey(String addRowKey) {
		this.addRowKey = addRowKey;
	}

	public String getInit() {
		return init;
	}

	public void setInit(String init) {
		this.init = init;
	}

	public String getBeforeImport() {
		return beforeImport;
	}

	public void setBeforeImport(String beforeImport) {
		this.beforeImport = beforeImport;
	}

	public boolean isStatus() {
		return status;
	}

	public void setStatus(boolean status) {
		this.status = status;
	}
	
}
