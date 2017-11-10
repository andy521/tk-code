package com.tk.track.fact.sparksql.desttable;

import java.io.Serializable;

public class RecommendWithoutAlgoScore implements Serializable {
	/*
	 * "app_id","event","user_id","visit_duration","visit_count","visit_time",
	 * "label","lrt_id"
	 */

	private static final long serialVersionUID = 2091574459929556517L;
	private String terminal_id;
	private String user_id;
	private String lrt_id;
	private String unique_pid;


	private String product_name;
	private String score;

	public RecommendWithoutAlgoScore() {

	}

	public RecommendWithoutAlgoScore(String terminal_id, String user_id,
			String visit_duration, String visit_count, String lrt_id,
			String product_name, String score) {
		super();
		this.terminal_id = terminal_id;
		this.user_id = user_id;
		this.lrt_id = lrt_id;
		this.product_name = product_name;
		this.score = score;
	}

	@Override
	public String toString() {
		return "RecommendWithoutAlgoScore [terminal_id=" + terminal_id
				+ ", user_id=" + user_id + ", lrt_id=" + lrt_id
				+ ", product_name=" + product_name + ", score=" + score + "]";
	}
	public String getUnique_pid() {
		return unique_pid;
	}

	public void setUnique_pid(String unique_pid) {
		this.unique_pid = unique_pid;
	}

	public String getTerminal_id() {
		return terminal_id;
	}

	public void setTerminal_id(String terminal_id) {
		this.terminal_id = terminal_id;
	}

	public String getUser_id() {
		return user_id;
	}

	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}

	public String getLrt_id() {
		return lrt_id;
	}

	public void setLrt_id(String lrt_id) {
		this.lrt_id = lrt_id;
	}

	public String getProduct_name() {
		return product_name;
	}

	public void setProduct_name(String product_name) {
		this.product_name = product_name;
	}

	public String getScore() {
		return score;
	}

	public void setScore(String score) {
		this.score = score;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}

}
