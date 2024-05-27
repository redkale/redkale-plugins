/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.util.List;

/** @author zhangjx */
public class PgRespAuthResultSet extends PgResultSet {

	protected boolean authOK;

	protected byte[] authSalt;

	protected List<String> authMechanisms;

	protected PgReqAuthScramSaslContinueResult authSaslContinueResult;

	public PgRespAuthResultSet() {}

	public boolean isAuthOK() {
		return authOK;
	}

	public void setAuthOK(boolean authOK) {
		this.authOK = authOK;
	}

	public byte[] getAuthSalt() {
		return authSalt;
	}

	public void setAuthSalt(byte[] authSalt) {
		this.authSalt = authSalt;
	}

	public List<String> getAuthMechanisms() {
		return authMechanisms;
	}

	public void setAuthMechanisms(List<String> authMechanisms) {
		this.authMechanisms = authMechanisms;
	}

	public PgReqAuthScramSaslContinueResult getAuthSaslContinueResult() {
		return authSaslContinueResult;
	}

	public void setAuthSaslContinueResult(PgReqAuthScramSaslContinueResult authSaslContinueResult) {
		this.authSaslContinueResult = authSaslContinueResult;
	}
}
