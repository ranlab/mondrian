/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.server;

import org.apache.log4j.Logger;

import mondrian.olap.MondrianServer;
import mondrian.olap.MondrianServerVersion;
import mondrian.olap.Util;
import mondrian.spi.CatalogLocator;
import mondrian.spi.impl.IdentityCatalogLocator;
import mondrian.util.LockBox;

/**
 * Registry of all servers within this JVM, and also serves as a factory for
 * servers.
 *
 * <p>This class is not a public API. User applications should use the
 * methods in {@link mondrian.olap.MondrianServer}.
 *
 * @author jhyde
 */
public class MondrianServerRegistry {
    public static final Logger logger = Logger.getLogger(MondrianServerRegistry.class);
    public static final MondrianServerRegistry INSTANCE = new MondrianServerRegistry();

    public MondrianServerRegistry() {
        super();
    }

    /**
     * Registry of all servers.
     */
    final LockBox lockBox = new LockBox();

    /**
     * The one and only one server that does not have a repository.
     */
    final MondrianServer staticServer = this.createWithRepository(null, null);

    /**
     * Looks up a server with a given id. If the id is null, returns the
     * static server.
     *
     * @param instanceId Unique identifier of server instance
     * @return Server
     * @throws RuntimeException if no server instance exists
     */
    public MondrianServer serverForId(String instanceId) {
        if (instanceId != null) {
            final LockBox.Entry entry = this.lockBox.get(instanceId);
            if (entry == null) {
                throw Util.newError("No server instance has id '" + instanceId + "'");
            }
            return (MondrianServer) entry.getValue();
        } else {
            return this.staticServer;
        }
    }

    public String getCopyrightYear() {
        return mondrian.olap.MondrianServerVersion.COPYRIGHT_YEAR;
    }

    public String getProductVersion() {
        return mondrian.olap.MondrianServerVersion.VERSION;
    }

    public MondrianServer.MondrianVersion getVersion() {
        if (logger.isDebugEnabled()) {
            logger.debug(" Vendor: " + mondrian.olap.MondrianServerVersion.VENDOR);
            final String title = mondrian.olap.MondrianServerVersion.NAME;
            logger.debug("  Title: " + title);
            final String versionString = MondrianServerVersion.VERSION;
            logger.debug("Version: " + versionString);
            final int majorVersion = java.lang.Integer.parseInt(mondrian.olap.MondrianServerVersion.MAJOR_VERSION);
            logger.debug(String.format("Major Version: %d", majorVersion));
            final int minorVersion = java.lang.Integer.parseInt(mondrian.olap.MondrianServerVersion.MINOR_VERSION);
            logger.debug(String.format("Minor Version: %d", minorVersion));
        }
        final StringBuilder sb = new StringBuilder();
        try {
            Integer.parseInt(mondrian.olap.MondrianServerVersion.VERSION);
            sb.append(mondrian.olap.MondrianServerVersion.VERSION);
        } catch (final NumberFormatException e) {
            // Version is not a number (e.g. "TRUNK-SNAPSHOT").
            // Fall back on VersionMajor, VersionMinor, if present.
            final String versionMajor = String.valueOf(mondrian.olap.MondrianServerVersion.MAJOR_VERSION);
            final String versionMinor = String.valueOf(mondrian.olap.MondrianServerVersion.MINOR_VERSION);
            if (versionMajor != null) {
                sb.append(versionMajor);
            }
            if (versionMinor != null) {
                sb.append(".").append(versionMinor);
            }
        }
        return new MondrianServer.MondrianVersion() {
            @Override
            public String getVersionString() {
                return sb.toString();
            }
            @Override
            public String getProductName() {
                return mondrian.olap.MondrianServerVersion.NAME;
            }
            @Override
            public int getMinorVersion() {
                return java.lang.Integer.parseInt(mondrian.olap.MondrianServerVersion.MINOR_VERSION);
            }
            @Override
            public int getMajorVersion() {
                return java.lang.Integer.parseInt(mondrian.olap.MondrianServerVersion.MAJOR_VERSION);
            }
        };
    }

    public MondrianServer createWithRepository(RepositoryContentFinder contentFinder, CatalogLocator catalogLocator) {
        if (catalogLocator == null) {
            catalogLocator = new IdentityCatalogLocator();
        }
        final Repository repository;
        if (contentFinder == null) {
            // NOTE: registry.staticServer is initialized by calling this
            // method; this is the only time that it is null.
            if (this.staticServer != null) {
                return this.staticServer;
            }
            repository = new ImplicitRepository();
        } else {
            repository = new FileRepository(contentFinder, catalogLocator);
        }
        return new MondrianServerImpl(this, repository, catalogLocator);
    }
}

// End MondrianServerRegistry.java
