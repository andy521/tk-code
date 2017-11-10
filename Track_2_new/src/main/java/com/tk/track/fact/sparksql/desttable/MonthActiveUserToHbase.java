package com.tk.track.fact.sparksql.desttable;

import java.io.Serializable;

public class MonthActiveUserToHbase implements Serializable{

	private String memberId;

	public String getMemberId() {
		return memberId;
	}

	public void setMemberId(String memberId) {
		this.memberId = memberId;
	}

}
