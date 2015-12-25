/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkale.service.email;

import java.util.*;
import org.redkale.convert.json.*;

/**
 *
 * @see http://www.redkale.org
 * @author zhangjx
 */
public class EmailMessage implements java.io.Serializable {

    //发送人
    private String from = "demo@redkale.org";

    //主送人  多个用空格隔开
    private String to = "";

    //抄送人  多个用空格隔开
    private String cc = "";

    //暗送人  多个用空格隔开
    private String bcc = "";

    //标题
    private String title = "";

    //内容
    private String content = "";

    private String contentType = "text/html";

    private boolean htmltranfer = true;

    private Map<String, String> files;

    public EmailMessage() {
    }

    public void putFile(String filetitle, String filepath) {
        if (this.files == null) this.files = new HashMap<>();
        this.files.put(filetitle, filepath);
    }

    @Override
    public String toString() {
        return JsonFactory.root().getConvert().convertTo(this);
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        if (to != null) {
            this.to = to;
        }
    }

    public String getToNames() {
        return to;
    }

    public String getCc() {
        return cc;
    }

    public void setCc(String cc) {
        if (cc != null) {
            this.cc = cc;
        }
    }

    public String getCcNames() {
        return cc;
    }

    public String getBcc() {
        return bcc;
    }

    public void setBcc(String bcc) {
        if (bcc != null) {
            this.bcc = bcc.trim();
        }
    }

    public String getBccNames() {
        return bcc;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        if (content != null) {
            this.content = content;
        }
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        if (contentType != null) {
            this.contentType = contentType;
        }
    }

    public String getFromNames() {
        return from;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public Map<String, String> getFiles() {
        return files;
    }

    public void setFiles(Map<String, String> files) {
        this.files = files;
    }

    public boolean isHtmltranfer() {
        return htmltranfer;
    }

    public void setHtmltranfer(boolean htmltranfer) {
        this.htmltranfer = htmltranfer;
    }
}
