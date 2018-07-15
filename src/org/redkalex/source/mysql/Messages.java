/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.text.MessageFormat;
import java.util.*;

/**
 *
 * @author zhangjx
 */
public class Messages {

    private static final String BUNDLE_NAME = "org.redkalex.source.mysql.LocalizedErrorMessages";

    private static final ResourceBundle RESOURCE_BUNDLE;

    static {
        ResourceBundle temp = null;

        //
        // Overly-pedantic here, some appserver and JVM combos don't deal well with the no-args version, others don't deal well with the three-arg version, so
        // we need to try both :(
        //

        try {
            temp = ResourceBundle.getBundle(BUNDLE_NAME, Locale.getDefault(), Messages.class.getClassLoader());
        } catch (Throwable t) {
            try {
                temp = ResourceBundle.getBundle(BUNDLE_NAME);
            } catch (Throwable t2) {
                RuntimeException rt = new RuntimeException("Can't load resource bundle due to underlying exception " + t.toString());
                rt.initCause(t2);

                throw rt;
            }
        } finally {
            RESOURCE_BUNDLE = temp;
        }
    }

    /**
     * Returns the localized message for the given message key
     * 
     * @param key
     *            the message key
     * @return The localized message for the key
     */
    public static String getString(String key) {
        if (RESOURCE_BUNDLE == null) {
            throw new RuntimeException("Localized messages from resource bundle '" + BUNDLE_NAME + "' not loaded during initialization of driver.");
        }

        try {
            if (key == null) {
                throw new IllegalArgumentException("Message key can not be null");
            }

            String message = RESOURCE_BUNDLE.getString(key);

            if (message == null) {
                message = "Missing error message for key '" + key + "'";
            }

            return message;
        } catch (MissingResourceException e) {
            return '!' + key + '!';
        }
    }

    public static String getString(String key, Object[] args) {
        return MessageFormat.format(getString(key), args);
    }

    /**
     * Dis-allow construction ...
     */
    private Messages() {
    }
}

