/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.oidc;

/**
 *
 * @author zhangjx
 */
public abstract class OAuths {

    private OAuths() {
    }

    public static final class HeaderType {

        public static final String CONTENT_TYPE = "Content-Type";

        public static final String WWW_AUTHENTICATE = "WWW-Authenticate";

        public static final String AUTHORIZATION = "Authorization";
    }

    public static final class WWWAuthHeader {

        public static final String REALM = "realm";
    }

    public static final class ContentType {

        public static final String URL_ENCODED = "application/x-www-form-urlencoded";

        public static final String JSON = "application/json";
    }

    public static enum ResponseType {

        CODE("code"),
        TOKEN("token");

        private String code;

        ResponseType(String code) {
            this.code = code;
        }

        @Override
        public String toString() {
            return code;
        }
    }

    public static enum GrantType {
        AUTHORIZATION_CODE("authorization_code"),
        @Deprecated
        IMPLICIT("implicit"), //https://datatracker.ietf.org/doc/html/draft-ietf-oauth-security-topics
        PASSWORD("password"),
        REFRESH_TOKEN("refresh_token"),
        CLIENT_CREDENTIALS("client_credentials"),
        JWT_BEARER("urn:ietf:params:oauth:grant-type:jwt-bearer");

        private String grantType;

        GrantType(String grantType) {
            this.grantType = grantType;
        }

        @Override
        public String toString() {
            return grantType;
        }
    }

    public static enum TokenType {
        BEARER("Bearer"),
        MAC("MAC");

        private String tokenType;

        TokenType(String grantType) {
            this.tokenType = grantType;
        }

        @Override
        public String toString() {
            return tokenType;
        }
    }

    public static enum ParameterStyle {
        BODY("body"),
        QUERY("query"),
        HEADER("header");

        private String parameterStyle;

        ParameterStyle(String parameterStyle) {
            this.parameterStyle = parameterStyle;
        }

        @Override
        public String toString() {
            return parameterStyle;
        }
    }

    public static final String OAUTH_RESPONSE_TYPE = "response_type";

    public static final String OAUTH_CLIENT_ID = "client_id";

    public static final String OAUTH_CLIENT_SECRET = "client_secret";

    public static final String OAUTH_REDIRECT_URI = "redirect_uri";

    public static final String OAUTH_USERNAME = "username";

    public static final String OAUTH_PASSWORD = "password";

    public static final String OAUTH_ASSERTION_TYPE = "assertion_type";

    public static final String OAUTH_ASSERTION = "assertion";

    public static final String OAUTH_SCOPE = "scope";

    public static final String OAUTH_STATE = "state";

    public static final String OAUTH_GRANT_TYPE = "grant_type";

    public static final String OAUTH_HEADER_NAME = "Bearer";

    //Authorization response params
    public static final String OAUTH_CODE = "code";

    public static final String OAUTH_ACCESS_TOKEN = "access_token";

    public static final String OAUTH_EXPIRES_IN = "expires_in";

    public static final String OAUTH_REFRESH_TOKEN = "refresh_token";

    public static final String OAUTH_TOKEN_TYPE = "token_type";

    public static final String OAUTH_TOKEN = "oauth_token";

    public static final String OAUTH_TOKEN_DRAFT_0 = "access_token";

    public static final String OAUTH_BEARER_TOKEN = "access_token";

    public static final ParameterStyle DEFAULT_PARAMETER_STYLE = ParameterStyle.HEADER;

    public static final TokenType DEFAULT_TOKEN_TYPE = TokenType.BEARER;

    public static final String OAUTH_VERSION_DIFFER = "oauth_signature_method";

    public static final String ASSERTION = "assertion";
}
