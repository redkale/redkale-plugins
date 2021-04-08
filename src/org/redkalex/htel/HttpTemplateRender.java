/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this referid file, choose Tools | Templates
 * and open the referid in the editor.
 */
package org.redkalex.htel;

import java.io.CharArrayReader;
import javax.xml.stream.*;
import org.redkale.convert.Convert;
import org.redkale.net.http.*;
import org.redkale.util.AnyValue;

/**
 * Http Template Express Language
 *
 * 尚未实现
 *
 * @author zhangjx
 */
public class HttpTemplateRender implements org.redkale.net.http.HttpRender<HttpScope> {

    @Override
    public void init(HttpContext context, AnyValue config) {
    }

    @Override
    public void renderTo(HttpRequest request, HttpResponse response, Convert convert, HttpScope scope) {
        response.setContentType("text/html; charset=utf-8");
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Class<HttpScope> getType() {
        return HttpScope.class;
    }

    public static void main(String[] args) throws Throwable {

        XMLStreamReader reader = XMLInputFactory.newFactory().createXMLStreamReader(new CharArrayReader(xhtml.toCharArray()));

        while (reader.hasNext()) {
            int event = reader.next();
            if (event == XMLStreamConstants.START_ELEMENT) {
                if ("for-each".equalsIgnoreCase(reader.getLocalName())) {
                    System.out.println("for-each开始: " + reader.getAttributeValue(null, "var"));
                } else if ("for-item".equalsIgnoreCase(reader.getLocalName())) {
                    System.out.println("for-item开始: " + reader.getAttributeValue(null, "value"));
                }
            } else if (event == XMLStreamConstants.END_ELEMENT) {
                if ("for-each".equalsIgnoreCase(reader.getLocalName())) {
                    System.out.println("for-each结束");
                } else if ("for-item".equalsIgnoreCase(reader.getLocalName())) {
                    System.out.println("for-item结束");
                }
            }
        }
    }

    private static final String xhtml = "<!DOCTYPE html>\n"
        + "<html>\n"
        + "    <head><title>Fortunes</title></head>\n"
        + "    <body>\n"
        + "        <table>\n"
        + "            <tr>\n"
        + "                <th>id</th>\n"
        + "                <th>message</th>\n"
        + "            </tr>\n"
        + "            <for-each var=\"fortune\" data=\"fortunes\">\n"
        + "                <tr>\n"
        + "                    <td><for-item var=\"fortune\" value=\"id\"/></td>\n"
        + "                    <td><for-item var=\"fortune\" value=\"message\"/></td>\n"
        + "                </tr>\n"
        + "            </for-each> \n"
        + "        </table>\n"
        + "    </body>\n"
        + "</html>\n"
        + "";
}
