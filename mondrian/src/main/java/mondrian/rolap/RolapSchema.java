/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2017 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;
import org.eigenbase.xom.DOMWrapper;
import org.eigenbase.xom.ElementDef;
import org.eigenbase.xom.Parser;
import org.eigenbase.xom.XOMException;
import org.eigenbase.xom.XOMUtil;
import org.olap4j.impl.Olap4jUtil;
import org.olap4j.mdx.IdentifierSegment;

import mondrian.olap.Access;
import mondrian.olap.Annotation;
import mondrian.olap.CacheControl;
import mondrian.olap.Category;
import mondrian.olap.Cube;
import mondrian.olap.Dimension;
import mondrian.olap.Exp;
import mondrian.olap.Formula;
import mondrian.olap.FunTable;
import mondrian.olap.Hierarchy;
import mondrian.olap.Id;
import mondrian.olap.Level;
import mondrian.olap.Member;
import mondrian.olap.MondrianDef;
import mondrian.olap.MondrianProperties;
import mondrian.olap.MondrianServer;
import mondrian.olap.NamedSet;
import mondrian.olap.OlapElement;
import mondrian.olap.Parameter;
import mondrian.olap.Role;
import mondrian.olap.RoleImpl;
import mondrian.olap.Schema;
import mondrian.olap.SchemaReader;
import mondrian.olap.Syntax;
import mondrian.olap.Util;
import mondrian.olap.fun.FunTableImpl;
import mondrian.olap.fun.GlobalFunTable;
import mondrian.olap.fun.Resolver;
import mondrian.olap.fun.UdfResolver;
import mondrian.olap.type.MemberType;
import mondrian.olap.type.NumericType;
import mondrian.olap.type.StringType;
import mondrian.olap.type.Type;
import mondrian.resource.MondrianResource;
import mondrian.rolap.aggmatcher.AggTableManager;
import mondrian.spi.DataSourceChangeListener;
import mondrian.spi.Dialect;
import mondrian.spi.DialectManager;
import mondrian.spi.UserDefinedFunction;
import mondrian.spi.impl.Scripts;
import mondrian.util.ByteString;
import mondrian.util.ClassResolver;

/**
 * A <code>RolapSchema</code> is a collection of {@link RolapCube}s and
 * shared {@link RolapDimension}s. It is shared betweeen {@link
 * RolapConnection}s. It caches {@link MemberReader}s, etc.
 *
 * @see RolapConnection
 * @author jhyde
 * @since 26 July, 2001
 */
public class RolapSchema implements Schema {
    static final Logger LOGGER = Logger.getLogger(RolapSchema.class);

    private static final Set<Access> schemaAllowed = Olap4jUtil.enumSetOf(Access.NONE, Access.ALL, Access.ALL_DIMENSIONS, Access.CUSTOM);

    private static final Set<Access> cubeAllowed = Olap4jUtil.enumSetOf(Access.NONE, Access.ALL, Access.CUSTOM);

    private static final Set<Access> dimensionAllowed = Olap4jUtil.enumSetOf(Access.NONE, Access.ALL, Access.CUSTOM);

    private static final Set<Access> hierarchyAllowed = Olap4jUtil.enumSetOf(Access.NONE, Access.ALL, Access.CUSTOM);

    private static final Set<Access> memberAllowed = Olap4jUtil.enumSetOf(Access.NONE, Access.ALL);

    private String name;

    /**
     * Internal use only.
     */
    private final RolapConnection internalConnection;

    /**
     * Holds cubes in this schema.
     */
    private final Map<String, RolapCube> mapNameToCube = new HashMap<String, RolapCube>();

    /**
     * Maps {@link String shared hierarchy name} to {@link MemberReader}.
     * Shared between all statements which use this connection.
     */
    private final Map<String, MemberReader> mapSharedHierarchyToReader = new HashMap<String, MemberReader>();

    /**
     * Maps {@link String names of shared hierarchies} to {@link
     * RolapHierarchy the canonical instance of those hierarchies}.
     */
    private final Map<String, RolapHierarchy> mapSharedHierarchyNameToHierarchy = new HashMap<String, RolapHierarchy>();

    /**
     * The default role for connections to this schema.
     */
    private Role defaultRole;

    private ByteString md5Bytes;

    /**
     * A schema's aggregation information
     */
    private AggTableManager aggTableManager;

    /**
     * This is basically a unique identifier for this RolapSchema instance
     * used it its equals and hashCode methods.
     */
    final SchemaKey key;

    /**
     * Maps {@link String names of roles} to {@link Role roles with those names}.
     */
    private final Map<String, Role> mapNameToRole = new HashMap<String, Role>();

    /**
     * Maps {@link String names of sets} to {@link NamedSet named sets}.
     */
    private final Map<String, NamedSet> mapNameToSet = new HashMap<String, NamedSet>();

    /**
     * Table containing all standard MDX functions, plus user-defined functions
     * for this schema.
     */
    private FunTable funTable;

    private MondrianDef.Schema xmlSchema;

    final List<RolapSchemaParameter> parameterList = new ArrayList<RolapSchemaParameter>();

    private Date schemaLoadDate;

    private DataSourceChangeListener dataSourceChangeListener;

    /**
     * List of warnings. Populated when a schema is created by a connection
     * that has
     * {@link mondrian.rolap.RolapConnectionProperties#Ignore Ignore}=true.
     */
    private final List<Exception> warningList = new ArrayList<Exception>();
    private Map<String, Annotation> annotationMap;

    /**
     * Unique schema instance id that will be used
     * to inform clients when the schema has changed.
     *
     * <p>Expect a different ID for each Mondrian instance node.
     */
    private final String id;

    /**
     * This is ONLY called by other constructors (and MUST be called
     * by them) and NEVER by the Pool.
     *
     * @param key Key
     * @param connectInfo Connect properties
     * @param dataSource Data source
     * @param md5Bytes MD5 hash
     * @param useContentChecksum Whether to use content checksum
     */
    private RolapSchema(final SchemaKey key, final Util.PropertyList connectInfo, final javax.sql.DataSource dataSource,
        final ByteString md5Bytes, boolean useContentChecksum) {
        this.id = Util.generateUuidString();
        this.key = key;
        this.md5Bytes = md5Bytes;
        if (useContentChecksum && (md5Bytes == null)) {
            throw new AssertionError();
        }

        // the order of the next two lines is important
        this.defaultRole = Util.createRootRole(this);
        final MondrianServer internalServer = MondrianServer.forId(null);
        this.internalConnection = new RolapConnection(internalServer, connectInfo, this, dataSource);
        internalServer.removeConnection(this.internalConnection);
        internalServer.removeStatement(this.internalConnection.getInternalStatement());

        this.aggTableManager = new AggTableManager(this);
        this.dataSourceChangeListener = this.createDataSourceChangeListener(connectInfo);
    }

    /**
     * Create RolapSchema given the MD5 hash, catalog name and string (content)
     * and the connectInfo object.
     *
     * @param md5Bytes may be null
     * @param catalogUrl URL of catalog
     * @param catalogStr may be null
     * @param connectInfo Connection properties
     */
    RolapSchema(SchemaKey key, ByteString md5Bytes, String catalogUrl, String catalogStr, Util.PropertyList connectInfo,
        javax.sql.DataSource dataSource) {
        this(key, connectInfo, dataSource, md5Bytes, md5Bytes != null);
        this.load(catalogUrl, catalogStr);
        assert this.md5Bytes != null;
    }

    /**
     * @deprecated for tests only!
     */
    @Deprecated
    RolapSchema(SchemaKey key, ByteString md5Bytes, RolapConnection internalConnection) {
        this.id = Util.generateUuidString();
        this.key = key;
        this.md5Bytes = md5Bytes;
        this.defaultRole = Util.createRootRole(this);
        this.internalConnection = internalConnection;
    }

    protected void flushSegments() {
        final RolapConnection internalConnection = this.getInternalConnection();
        if (internalConnection != null) {
            final CacheControl cc = internalConnection.getCacheControl(null);
            for (final RolapCube cube : this.getCubeList()) {
                cc.flush(cc.createMeasuresRegion(cube));
            }
        }
    }

    /**
     * Clears the cache of JDBC tables for the aggs.
     */
    protected void flushJdbcSchema() {
        // Cleanup the agg table manager's caches.
        if (this.aggTableManager != null) {
            this.aggTableManager.finalCleanUp();
            this.aggTableManager = null;
        }
    }

    /**
     * Performs a sweep of the JDBC tables caches and the segment data.
     * Only called internally when a schema and it's data must be refreshed.
     */
    protected void finalCleanUp() {
        // Cleanup the segment data.
        this.flushSegments();

        // Cleanup the agg JDBC cache
        this.flushJdbcSchema();
    }

    @Override
    protected void finalize()
        throws Throwable {
        try {
            super.finalize();
            // Only clear the JDBC cache to prevent leaks.
            this.flushJdbcSchema();
        } catch (final Throwable t) {
            LOGGER.info(MondrianResource.instance().FinalizerErrorRolapSchema.baseMessage, t);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof RolapSchema)) {
            return false;
        }
        final RolapSchema other = (RolapSchema) o;
        return other.key.equals(this.key);
    }

    @Override
    public int hashCode() {
        return this.key.hashCode();
    }

    protected Logger getLogger() {
        return LOGGER;
    }

    /**
     * Method called by all constructors to load the catalog into DOM and build
     * application mdx and sql objects.
     *
     * @param catalogUrl URL of catalog
     * @param catalogStr Text of catalog, or null
     */
    protected void load(String catalogUrl, String catalogStr) {
        try {
            final Parser xmlParser = XOMUtil.createDefaultParser();

            final DOMWrapper def;
            if (catalogStr == null) {
                InputStream in = null;
                try {
                    in = Util.readVirtualFile(catalogUrl);
                    def = xmlParser.parse(in);
                } finally {
                    if (in != null) {
                        in.close();
                    }
                }

                // Compute catalog string, if needed for debug or for computing
                // Md5 hash.
                if (this.getLogger().isDebugEnabled() || (this.md5Bytes == null)) {
                    try {
                        catalogStr = Util.readVirtualFileAsString(catalogUrl);
                    } catch (final java.io.IOException ex) {
                        this.getLogger().debug("RolapSchema.load: ex=" + ex);
                        catalogStr = "?";
                    }
                }

                if (this.getLogger().isDebugEnabled()) {
                    this.getLogger().debug("RolapSchema.load: content: \n" + catalogStr);
                }
            } else {
                if (this.getLogger().isDebugEnabled()) {
                    this.getLogger().debug("RolapSchema.load: catalogStr: \n" + catalogStr);
                }

                def = xmlParser.parse(catalogStr);
            }

            if (this.md5Bytes == null) {
                // If a null catalogStr was passed in, we should have
                // computed it above by re-reading the catalog URL.
                assert catalogStr != null;
                this.md5Bytes = new ByteString(Util.digestMd5(catalogStr));
            }

            // throw error if we have an incompatible schema
            this.checkSchemaVersion(def);

            this.xmlSchema = new MondrianDef.Schema(def);

            if (this.getLogger().isDebugEnabled()) {
                final StringWriter sw = new StringWriter(4096);
                final PrintWriter pw = new PrintWriter(sw);
                pw.println("RolapSchema.load: dump xmlschema");
                this.xmlSchema.display(pw, 2);
                pw.flush();
                this.getLogger().debug(sw.toString());
            }

            this.load(this.xmlSchema);
        } catch (final XOMException e) {
            throw Util.newError(e, "while parsing catalog " + catalogUrl);
        } catch (final org.apache.commons.vfs.FileSystemException e) {
            throw Util.newError(e, "while parsing catalog " + catalogUrl);
        } catch (final IOException e) {
            throw Util.newError(e, "while parsing catalog " + catalogUrl);
        }

        this.aggTableManager.initialize();
        this.setSchemaLoadDate();
    }

    private void checkSchemaVersion(final DOMWrapper schemaDom) {
        String schemaVersion = schemaDom.getAttribute("metamodelVersion");
        if (schemaVersion == null) {
            if (this.hasMondrian4Elements(schemaDom)) {
                schemaVersion = "4.x";
            } else {
                schemaVersion = "3.x";
            }
        }

        final String[] versionParts = schemaVersion.split("\\.");
        final String schemaMajor = versionParts.length > 0 ? versionParts[0] : "";

        final MondrianServer.MondrianVersion mondrianVersion = MondrianServer.forId(null).getVersion();
        final String serverMajor = mondrianVersion.getMajorVersion() + ""; // "3"

        if (serverMajor.compareTo(schemaMajor) < 0) {
            final String errorMsg = "Schema version '" + schemaVersion
                + "' is later than schema version "
                + "'3.x' supported by this version of Mondrian";
            throw Util.newError(errorMsg);
        }
    }

    private boolean hasMondrian4Elements(final DOMWrapper schemaDom) {
        // check for Mondrian 4 schema elements:
        for (final DOMWrapper child : schemaDom.getChildren()) {
            if ("PhysicalSchema".equals(child.getTagName())) {
                // Schema/PhysicalSchema
                return true;
            } else if ("Cube".equals(child.getTagName())) {
                for (final DOMWrapper grandchild : child.getChildren()) {
                    if ("MeasureGroups".equals(grandchild.getTagName())) {
                        // Schema/Cube/MeasureGroups
                        return true;
                    }
                }
            }
        }
        // otherwise assume version 3.x
        return false;
    }

    private void setSchemaLoadDate() {
        this.schemaLoadDate = new Date();
    }

    @Override
    public Date getSchemaLoadDate() {
        return this.schemaLoadDate;
    }

    @Override
    public List<Exception> getWarnings() {
        return Collections.unmodifiableList(this.warningList);
    }

    public Role getDefaultRole() {
        return this.defaultRole;
    }

    public MondrianDef.Schema getXMLSchema() {
        return this.xmlSchema;
    }

    @Override
    public String getName() {
        Util.assertPostcondition(this.name != null, "return != null");
        Util.assertPostcondition(this.name.length() > 0, "return.length() > 0");
        return this.name;
    }

    /**
     * Returns this schema instance unique ID.
     * @return A string representing the schema ID.
     */
    @Override
    public String getId() {
        return this.id;
    }

    /**
     * Returns this schema instance unique key.
     * @return a {@link SchemaKey}.
     */
    public SchemaKey getKey() {
        return this.key;
    }

    @Override
    public Map<String, Annotation> getAnnotationMap() {
        return this.annotationMap;
    }

    /**
     * Returns this schema's SQL dialect.
     *
     * <p>NOTE: This method is not cheap. The implementation gets a connection
     * from the connection pool.
     *
     * @return dialect
     */
    public Dialect getDialect() {
        final javax.sql.DataSource dataSource = this.getInternalConnection().getDataSource();
        return DialectManager.createDialect(dataSource, null);
    }

    private void load(MondrianDef.Schema xmlSchema) {
        this.name = xmlSchema.name;
        if ((this.name == null) || this.name.equals("")) {
            throw Util.newError("<Schema> name must be set");
        }

        this.annotationMap = RolapHierarchy.createAnnotationMap(xmlSchema.annotations);
        // Validate user-defined functions. Must be done before we validate
        // calculated members, because calculated members will need to use the
        // function table.
        final Map<String, UdfResolver.UdfFactory> mapNameToUdf = new HashMap<String, UdfResolver.UdfFactory>();
        for (final MondrianDef.UserDefinedFunction udf : xmlSchema.userDefinedFunctions) {
            final Scripts.ScriptDefinition scriptDef = toScriptDef(udf.script);
            this.defineFunction(mapNameToUdf, udf.name, udf.className, scriptDef);
        }
        final RolapSchemaFunctionTable funTable = new RolapSchemaFunctionTable(mapNameToUdf.values());
        funTable.init();
        this.funTable = funTable;

        // Validate public dimensions.
        for (final MondrianDef.Dimension xmlDimension : xmlSchema.dimensions) {
            if (xmlDimension.foreignKey != null) {
                throw MondrianResource.instance().PublicDimensionMustNotHaveForeignKey.ex(xmlDimension.name);
            }
        }

        // Create parameters.
        final Set<String> parameterNames = new HashSet<String>();
        for (final MondrianDef.Parameter xmlParameter : xmlSchema.parameters) {
            final String name = xmlParameter.name;
            if (!parameterNames.add(name)) {
                throw MondrianResource.instance().DuplicateSchemaParameter.ex(name);
            }
            Type type;
            if (xmlParameter.type.equals("String")) {
                type = new StringType();
            } else if (xmlParameter.type.equals("Numeric")) {
                type = new NumericType();
            } else {
                type = new MemberType(null, null, null, null);
            }
            final String description = xmlParameter.description;
            final boolean modifiable = xmlParameter.modifiable;
            final String defaultValue = xmlParameter.defaultValue;
            final RolapSchemaParameter param = new RolapSchemaParameter(this, name, defaultValue, description, type, modifiable);
            Util.discard(param);
        }

        // Create cubes.
        for (final MondrianDef.Cube xmlCube : xmlSchema.cubes) {
            if (xmlCube.isEnabled()) {
                final RolapCube cube = new RolapCube(this, xmlSchema, xmlCube, true);
                Util.discard(cube);
            }
        }

        // Create virtual cubes.
        for (final MondrianDef.VirtualCube xmlVirtualCube : xmlSchema.virtualCubes) {
            if (xmlVirtualCube.isEnabled()) {
                final RolapCube cube = new RolapCube(this, xmlSchema, xmlVirtualCube, true);
                Util.discard(cube);
            }
        }

        // Create named sets.
        for (final MondrianDef.NamedSet xmlNamedSet : xmlSchema.namedSets) {
            this.mapNameToSet.put(xmlNamedSet.name, this.createNamedSet(xmlNamedSet));
        }

        // Create roles.
        for (final MondrianDef.Role xmlRole : xmlSchema.roles) {
            final Role role = this.createRole(xmlRole);
            this.mapNameToRole.put(xmlRole.name, role);
        }

        // Set default role.
        if (xmlSchema.defaultRole != null) {
            final Role role = this.lookupRole(xmlSchema.defaultRole);
            if (role == null) {
                this.error("Role '" + xmlSchema.defaultRole + "' not found", this.locate(xmlSchema, "defaultRole"));
            } else {
                // At this stage, the only roles in mapNameToRole are
                // RoleImpl roles so it is safe to case.
                this.defaultRole = role;
            }
        }
    }

    static Scripts.ScriptDefinition toScriptDef(MondrianDef.Script script) {
        if (script == null) {
            return null;
        }
        final Scripts.ScriptLanguage language = Scripts.ScriptLanguage.lookup(script.language);
        if (language == null) {
            throw Util.newError("Invalid script language '" + script.language + "'");
        }
        return new Scripts.ScriptDefinition(script.cdata, language);
    }

    /**
     * Returns the location of an element or attribute in an XML document.
     *
     * <p>TODO: modify eigenbase-xom parser to return position info
     *
     * @param node Node
     * @param attributeName Attribute name, or null
     * @return Location of node or attribute in an XML document
     */
    XmlLocation locate(ElementDef node, String attributeName) {
        return null;
    }

    /**
     * Reports an error. If we are tolerant of errors
     * (see {@link mondrian.rolap.RolapConnectionProperties#Ignore}), adds
     * it to the stack, overwise throws. A thrown exception will typically
     * abort the attempt to create the exception.
     *
     * @param message Message
     * @param xmlLocation Location of XML element or attribute that caused
     * the error, or null
     */
    void error(String message, XmlLocation xmlLocation) {
        final RuntimeException ex = new RuntimeException(message);
        if ((this.internalConnection != null)
            && "true".equals(this.internalConnection.getProperty(RolapConnectionProperties.Ignore.name()))) {
            this.warningList.add(ex);
        } else {
            throw ex;
        }
    }

    private NamedSet createNamedSet(MondrianDef.NamedSet xmlNamedSet) {
        final String formulaString = xmlNamedSet.getFormula();
        final Exp exp;
        try {
            exp = this.getInternalConnection().parseExpression(formulaString);
        } catch (final Exception e) {
            throw MondrianResource.instance().NamedSetHasBadFormula.ex(xmlNamedSet.name, e);
        }
        final Formula formula = new Formula(new Id(new Id.NameSegment(xmlNamedSet.name, Id.Quoting.UNQUOTED)), exp);
        return formula.getNamedSet();
    }

    private Role createRole(MondrianDef.Role xmlRole) {
        if (xmlRole.union != null) {
            return this.createUnionRole(xmlRole);
        }

        final RoleImpl role = new RoleImpl();
        for (final MondrianDef.SchemaGrant schemaGrant : xmlRole.schemaGrants) {
            this.handleSchemaGrant(role, schemaGrant);
        }
        role.makeImmutable();
        return role;
    }

    // package-local visibility for testing purposes
    Role createUnionRole(MondrianDef.Role xmlRole) {
        if ((xmlRole.schemaGrants != null) && (xmlRole.schemaGrants.length > 0)) {
            throw MondrianResource.instance().RoleUnionGrants.ex();
        }

        final MondrianDef.RoleUsage[] usages = xmlRole.union.roleUsages;
        final List<Role> roleList = new ArrayList<Role>(usages.length);
        for (final MondrianDef.RoleUsage roleUsage : usages) {
            final Role role = this.mapNameToRole.get(roleUsage.roleName);
            if (role == null) {
                throw MondrianResource.instance().UnknownRole.ex(roleUsage.roleName);
            }
            roleList.add(role);
        }
        return RoleImpl.union(roleList);
    }

    // package-local visibility for testing purposes
    void handleSchemaGrant(RoleImpl role, MondrianDef.SchemaGrant schemaGrant) {
        role.grant(this, this.getAccess(schemaGrant.access, schemaAllowed));
        for (final MondrianDef.CubeGrant cubeGrant : schemaGrant.cubeGrants) {
            this.handleCubeGrant(role, cubeGrant);
        }
    }

    // package-local visibility for testing purposes
    void handleCubeGrant(RoleImpl role, MondrianDef.CubeGrant cubeGrant) {
        final RolapCube cube = this.lookupCube(cubeGrant.cube);
        if (cube == null) {
            throw Util.newError("Unknown cube '" + cubeGrant.cube + "'");
        }
        role.grant(cube, this.getAccess(cubeGrant.access, cubeAllowed));

        final SchemaReader reader = cube.getSchemaReader(null);
        for (final MondrianDef.DimensionGrant grant : cubeGrant.dimensionGrants) {
            final Dimension dimension = this.lookup(cube, reader, Category.Dimension, grant.dimension);
            role.grant(dimension, this.getAccess(grant.access, dimensionAllowed));
        }

        for (final MondrianDef.HierarchyGrant hierarchyGrant : cubeGrant.hierarchyGrants) {
            this.handleHierarchyGrant(role, cube, reader, hierarchyGrant);
        }
    }

    // package-local visibility for testing purposes
    void handleHierarchyGrant(RoleImpl role, RolapCube cube, SchemaReader reader, MondrianDef.HierarchyGrant grant) {
        final Hierarchy hierarchy = this.lookup(cube, reader, Category.Hierarchy, grant.hierarchy);
        final Access hierarchyAccess = this.getAccess(grant.access, hierarchyAllowed);
        final Level topLevel = this.findLevelForHierarchyGrant(cube, reader, hierarchyAccess, grant.topLevel, "topLevel");
        final Level bottomLevel = this.findLevelForHierarchyGrant(cube, reader, hierarchyAccess, grant.bottomLevel, "bottomLevel");

        Role.RollupPolicy rollupPolicy;
        if (grant.rollupPolicy != null) {
            try {
                rollupPolicy = Role.RollupPolicy.valueOf(grant.rollupPolicy.toUpperCase());
            } catch (final IllegalArgumentException e) {
                throw Util.newError("Illegal rollupPolicy value '" + grant.rollupPolicy + "'");
            }
        } else {
            rollupPolicy = Role.RollupPolicy.FULL;
        }
        role.grant(hierarchy, hierarchyAccess, topLevel, bottomLevel, rollupPolicy);

        final boolean ignoreInvalidMembers = MondrianProperties.instance().IgnoreInvalidMembers.get();

        int membersRejected = 0;
        if (grant.memberGrants.length > 0) {
            if (hierarchyAccess != Access.CUSTOM) {
                throw Util.newError("You may only specify <MemberGrant> if " + "<Hierarchy> has access='custom'");
            }

            for (final MondrianDef.MemberGrant memberGrant : grant.memberGrants) {
                final Member member = reader
                    .withLocus()
                        .getMemberByUniqueName(Util.parseIdentifier(memberGrant.member), !ignoreInvalidMembers);
                if (member == null) {
                    // They asked to ignore members that don't exist
                    // (e.g. [Store].[USA].[Foo]), so ignore this grant
                    // too.
                    assert ignoreInvalidMembers;
                    membersRejected++;
                    continue;
                }
                if (member.getHierarchy() != hierarchy) {
                    throw Util.newError("Member '" + member + "' is not in hierarchy '" + hierarchy + "'");
                }
                role.grant(member, this.getAccess(memberGrant.access, memberAllowed));
            }
        }

        if ((membersRejected > 0) && (grant.memberGrants.length == membersRejected)) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER
                    .trace("Rolling back grants of Hierarchy '" + hierarchy.getUniqueName()
                        + "' to NONE, because it contains no "
                        + "valid restricted members");
            }
            role.grant(hierarchy, Access.NONE, null, null, rollupPolicy);
        }
    }

    private <T extends OlapElement> T lookup(RolapCube cube, SchemaReader reader, int category, String id) {
        final List<Id.Segment> segments = Util.parseIdentifier(id);
        //noinspection unchecked
        return (T) reader.lookupCompound(cube, segments, true, category);
    }

    private Level findLevelForHierarchyGrant(RolapCube cube, SchemaReader schemaReader, Access hierarchyAccess, String name, String desc) {
        if (name == null) {
            return null;
        }

        if (hierarchyAccess != Access.CUSTOM) {
            throw Util.newError("You may only specify '" + desc + "' if access='custom'");
        }
        return this.lookup(cube, schemaReader, Category.Level, name);
    }

    private Access getAccess(String accessString, Set<Access> allowed) {
        final Access access = Access.valueOf(accessString.toUpperCase());
        if (allowed.contains(access)) {
            return access; // value is ok
        }
        throw Util.newError("Bad value access='" + accessString + "'");
    }

    @Override
    public Dimension createDimension(Cube cube, String xml) {
        MondrianDef.CubeDimension xmlDimension;
        try {
            final Parser xmlParser = XOMUtil.createDefaultParser();
            final DOMWrapper def = xmlParser.parse(xml);
            final String tagName = def.getTagName();
            if (tagName.equals("Dimension")) {
                xmlDimension = new MondrianDef.Dimension(def);
            } else if (tagName.equals("DimensionUsage")) {
                xmlDimension = new MondrianDef.DimensionUsage(def);
            } else {
                throw new XOMException("Got <" + tagName + "> when expecting <Dimension> or <DimensionUsage>");
            }
        } catch (final XOMException e) {
            throw Util.newError(e, "Error while adding dimension to cube '" + cube + "' from XML [" + xml + "]");
        }
        return ((RolapCube) cube).createDimension(xmlDimension, this.xmlSchema);
    }

    @Override
    public Cube createCube(String xml) {
        RolapCube cube;
        try {
            final Parser xmlParser = XOMUtil.createDefaultParser();
            final DOMWrapper def = xmlParser.parse(xml);
            final String tagName = def.getTagName();
            if (tagName.equals("Cube")) {
                // Create empty XML schema, to keep the method happy. This is
                // okay, because there are no forward-references to resolve.
                final MondrianDef.Schema xmlSchema = new MondrianDef.Schema();
                final MondrianDef.Cube xmlDimension = new MondrianDef.Cube(def);
                cube = new RolapCube(this, xmlSchema, xmlDimension, false);
            } else if (tagName.equals("VirtualCube")) {
                // Need the real schema here.
                final MondrianDef.Schema xmlSchema = this.getXMLSchema();
                final MondrianDef.VirtualCube xmlDimension = new MondrianDef.VirtualCube(def);
                cube = new RolapCube(this, xmlSchema, xmlDimension, false);
            } else {
                throw new XOMException("Got <" + tagName + "> when expecting <Cube>");
            }
        } catch (final XOMException e) {
            throw Util.newError(e, "Error while creating cube from XML [" + xml + "]");
        }
        return cube;
    }

    public static List<RolapSchema> getRolapSchemas() {
        return RolapSchemaPool.instance().getRolapSchemas();
    }

    public static boolean cacheContains(RolapSchema rolapSchema) {
        return RolapSchemaPool.instance().contains(rolapSchema);
    }

    @Override
    public Cube lookupCube(final String cube, final boolean failIfNotFound) {
        final RolapCube mdxCube = this.lookupCube(cube);
        if ((mdxCube == null) && failIfNotFound) {
            throw MondrianResource.instance().MdxCubeNotFound.ex(cube);
        }
        return mdxCube;
    }

    /**
     * Finds a cube called 'cube' in the current catalog, or return null if no
     * cube exists.
     */
    protected RolapCube lookupCube(final String cubeName) {
        return this.mapNameToCube.get(Util.normalizeName(cubeName));
    }

    /**
     * Returns an xmlCalculatedMember called 'calcMemberName' in the
     * cube called 'cubeName' or return null if no calculatedMember or
     * xmlCube by those name exists.
     */
    protected MondrianDef.CalculatedMember lookupXmlCalculatedMember(final String calcMemberName, final String cubeName) {
        for (final MondrianDef.Cube cube : this.xmlSchema.cubes) {
            if (!Util.equalName(cube.name, cubeName)) {
                continue;
            }
            for (final MondrianDef.CalculatedMember xmlCalcMember : cube.calculatedMembers) {
                // FIXME: Since fully-qualified names are not unique, we
                // should compare unique names. Also, the logic assumes that
                // CalculatedMember.dimension is not quoted (e.g. "Time")
                // and CalculatedMember.hierarchy is quoted
                // (e.g. "[Time].[Weekly]").
                if (Util.equalName(this.calcMemberFqName(xmlCalcMember), calcMemberName)) {
                    return xmlCalcMember;
                }
            }
        }
        return null;
    }

    private String calcMemberFqName(MondrianDef.CalculatedMember xmlCalcMember) {
        if (xmlCalcMember.dimension != null) {
            return Util.makeFqName(Util.quoteMdxIdentifier(xmlCalcMember.dimension), xmlCalcMember.name);
        } else {
            return Util.makeFqName(xmlCalcMember.hierarchy, xmlCalcMember.name);
        }
    }

    public List<RolapCube> getCubesWithStar(RolapStar star) {
        final List<RolapCube> list = new ArrayList<RolapCube>();
        for (final RolapCube cube : this.mapNameToCube.values()) {
            if (star == cube.getStar()) {
                list.add(cube);
            }
        }
        return list;
    }

    /**
     * Adds a cube to the cube name map.
     * @see #lookupCube(String)
     */
    protected void addCube(final RolapCube cube) {
        this.mapNameToCube.put(Util.normalizeName(cube.getName()), cube);
    }

    @Override
    public boolean removeCube(final String cubeName) {
        final RolapCube cube = this.mapNameToCube.remove(Util.normalizeName(cubeName));
        return cube != null;
    }

    @Override
    public Cube[] getCubes() {
        final Collection<RolapCube> cubes = this.mapNameToCube.values();
        return cubes.toArray(new RolapCube[cubes.size()]);
    }

    public List<RolapCube> getCubeList() {
        return new ArrayList<RolapCube>(this.mapNameToCube.values());
    }

    @Override
    public Hierarchy[] getSharedHierarchies() {
        final Collection<RolapHierarchy> hierarchies = this.mapSharedHierarchyNameToHierarchy.values();
        return hierarchies.toArray(new RolapHierarchy[hierarchies.size()]);
    }

    RolapHierarchy getSharedHierarchy(final String name) {
        return this.mapSharedHierarchyNameToHierarchy.get(name);
    }

    public NamedSet getNamedSet(String name) {
        return this.mapNameToSet.get(name);
    }

    public NamedSet getNamedSet(IdentifierSegment segment) {
        // FIXME: write a map that efficiently maps segment->value, taking
        // into account case-sensitivity etc.
        for (final Map.Entry<String, NamedSet> entry : this.mapNameToSet.entrySet()) {
            if (Util.matches(segment, entry.getKey())) {
                return entry.getValue();
            }
        }
        return null;
    }

    @Override
    public Role lookupRole(final String role) {
        return this.mapNameToRole.get(role);
    }

    public Set<String> roleNames() {
        return this.mapNameToRole.keySet();
    }

    @Override
    public FunTable getFunTable() {
        return this.funTable;
    }

    @Override
    public Parameter[] getParameters() {
        return this.parameterList.toArray(new Parameter[this.parameterList.size()]);
    }

    /**
     * Defines a user-defined function in this table.
     *
     * <p>If the function is not valid, throws an error.
     *
     * @param name Name of the function.
     * @param className Name of the class which implements the function.
     *   The class must implement {@link mondrian.spi.UserDefinedFunction}
     *   (otherwise it is a user-error).
     */
    private void defineFunction(Map<String, UdfResolver.UdfFactory> mapNameToUdf,
        final String name,
        String className,
        final Scripts.ScriptDefinition script) {
        if ((className == null) && (script == null)) {
            throw Util.newError("Must specify either className attribute or Script element");
        }
        if ((className != null) && (script != null)) {
            throw Util.newError("Must not specify both className attribute and Script element");
        }
        final UdfResolver.UdfFactory udfFactory;
        if (className != null) {
            // Lookup class.
            try {
                final Class<UserDefinedFunction> klass = ClassResolver.INSTANCE.forName(className, true);

                // Instantiate UDF by calling correct constructor.
                udfFactory = new UdfResolver.ClassUdfFactory(klass, name);
            } catch (final ClassNotFoundException e) {
                throw MondrianResource.instance().UdfClassNotFound.ex(name, className);
            }
        } else {
            udfFactory = new UdfResolver.UdfFactory() {
                @Override
                public UserDefinedFunction create() {
                    return Scripts.userDefinedFunction(script, name);
                }
            };
        }
        // Validate function.
        this.validateFunction(udfFactory);
        // Check for duplicate.
        final UdfResolver.UdfFactory existingUdf = mapNameToUdf.get(name);
        if (existingUdf != null) {
            throw MondrianResource.instance().UdfDuplicateName.ex(name);
        }
        mapNameToUdf.put(name, udfFactory);
    }

    /**
     * Throws an error if a user-defined function does not adhere to the
     * API.
     */
    private void validateFunction(UdfResolver.UdfFactory udfFactory) {
        final UserDefinedFunction udf = udfFactory.create();

        // Check that the name is not null or empty.
        final String udfName = udf.getName();
        if ((udfName == null) || udfName.equals("")) {
            throw Util.newInternal("User-defined function defined by class '" + udf.getClass() + "' has empty name");
        }
        // It's OK for the description to be null.
        final String description = udf.getDescription();
        Util.discard(description);
        final Type[] parameterTypes = udf.getParameterTypes();
        for (int i = 0; i < parameterTypes.length; i++) {
            final Type parameterType = parameterTypes[i];
            if (parameterType == null) {
                throw Util.newInternal("Invalid user-defined function '" + udfName + "': parameter type #" + i + " is null");
            }
        }
        // It's OK for the reserved words to be null or empty.
        final String[] reservedWords = udf.getReservedWords();
        Util.discard(reservedWords);
        // Test that the function returns a sensible type when given the FORMAL
        // types. It may still fail when we give it the ACTUAL types, but it's
        // impossible to check that now.
        final Type returnType = udf.getReturnType(parameterTypes);
        if (returnType == null) {
            throw Util.newInternal("Invalid user-defined function '" + udfName + "': return type is null");
        }
        final Syntax syntax = udf.getSyntax();
        if (syntax == null) {
            throw Util.newInternal("Invalid user-defined function '" + udfName + "': syntax is null");
        }
    }

    /**
     * Gets a {@link MemberReader} with which to read a hierarchy. If the
     * hierarchy is shared (<code>sharedName</code> is not null), looks up
     * a reader from a cache, or creates one if necessary.
     *
     * <p>Synchronization: thread safe
     */
    synchronized MemberReader createMemberReader(final String sharedName, final RolapHierarchy hierarchy, final String memberReaderClass) {
        MemberReader reader;
        if (sharedName != null) {
            reader = this.mapSharedHierarchyToReader.get(sharedName);
            if (reader == null) {
                reader = this.createMemberReader(hierarchy, memberReaderClass);
                // share, for other uses of the same shared hierarchy
                if (false) {
                    this.mapSharedHierarchyToReader.put(sharedName, reader);
                }
                /*
                System.out.println("RolapSchema.createMemberReader: "+
                "add to sharedHierName->Hier map"+
                " sharedName=" + sharedName +
                ", hierarchy=" + hierarchy.getName() +
                ", hierarchy.dim=" + hierarchy.getDimension().getName()
                );
                if (mapSharedHierarchyNameToHierarchy.containsKey(sharedName)) {
                System.out.println("RolapSchema.createMemberReader: CONTAINS NAME");
                } else {
                mapSharedHierarchyNameToHierarchy.put(sharedName, hierarchy);
                }
                */
                if (!this.mapSharedHierarchyNameToHierarchy.containsKey(sharedName)) {
                    this.mapSharedHierarchyNameToHierarchy.put(sharedName, hierarchy);
                }
                //mapSharedHierarchyNameToHierarchy.put(sharedName, hierarchy);
            } else {
                //                final RolapHierarchy sharedHierarchy = (RolapHierarchy)
                //                        mapSharedHierarchyNameToHierarchy.get(sharedName);
                //                final RolapDimension sharedDimension = (RolapDimension)
                //                        sharedHierarchy.getDimension();
                //                final RolapDimension dimension =
                //                    (RolapDimension) hierarchy.getDimension();
                //                Util.assertTrue(
                //                        dimension.getGlobalOrdinal() ==
                //                        sharedDimension.getGlobalOrdinal());
            }
        } else {
            reader = this.createMemberReader(hierarchy, memberReaderClass);
        }
        return reader;
    }

    /**
     * Creates a {@link MemberReader} with which to Read a hierarchy.
     */
    private MemberReader createMemberReader(final RolapHierarchy hierarchy, final String memberReaderClass) {
        if (memberReaderClass != null) {
            Exception e2;
            try {
                final Properties properties = null;
                final Class<?> clazz = ClassResolver.INSTANCE.forName(memberReaderClass, true);
                final Constructor<?> constructor = clazz.getConstructor(RolapHierarchy.class, Properties.class);
                final Object o = constructor.newInstance(hierarchy, properties);
                if (o instanceof MemberReader) {
                    return (MemberReader) o;
                } else if (o instanceof MemberSource) {
                    return new CacheMemberReader((MemberSource) o);
                } else {
                    throw Util.newInternal("member reader class " + clazz + " does not implement " + MemberSource.class);
                }
            } catch (final ClassNotFoundException e) {
                e2 = e;
            } catch (final NoSuchMethodException e) {
                e2 = e;
            } catch (final InstantiationException e) {
                e2 = e;
            } catch (final IllegalAccessException e) {
                e2 = e;
            } catch (final InvocationTargetException e) {
                e2 = e;
            }
            throw Util.newInternal(e2, "while instantiating member reader '" + memberReaderClass);
        } else {
            final SqlMemberSource source = new SqlMemberSource(hierarchy);
            final Dimension dimension = hierarchy.getDimension();
            if (dimension.isHighCardinality()) {
                LOGGER.warn(MondrianResource.instance().HighCardinalityInDimension.str(dimension.getUniqueName()));
                LOGGER.debug("High cardinality for " + dimension);
                return new NoCacheMemberReader(source);
            } else {
                LOGGER.debug("Normal cardinality for " + hierarchy.getDimension());
                if (MondrianProperties.instance().DisableCaching.get()) {
                    // If the cell cache is disabled, we can't cache
                    // the members or else we get undefined results,
                    // depending on the functions used and all.
                    return new NoCacheMemberReader(source);
                } else {
                    return new SmartMemberReader(source);
                }
            }
        }
    }

    @Override
    public SchemaReader getSchemaReader() {
        return new RolapSchemaReader(this.defaultRole, this).withLocus();
    }

    /**
     * Creates a {@link DataSourceChangeListener} with which to detect changes
     * to datasources.
     */
    private DataSourceChangeListener createDataSourceChangeListener(Util.PropertyList connectInfo) {
        DataSourceChangeListener changeListener = null;

        // If CatalogContent is specified in the connect string, ignore
        // everything else. In particular, ignore the dynamic schema
        // processor.
        final String dataSourceChangeListenerStr = connectInfo.get(RolapConnectionProperties.DataSourceChangeListener.name());

        if (!Util.isEmpty(dataSourceChangeListenerStr)) {
            try {
                changeListener = ClassResolver.INSTANCE.instantiateSafe(dataSourceChangeListenerStr);
            } catch (final Exception e) {
                throw Util.newError(e, "loading DataSourceChangeListener " + dataSourceChangeListenerStr);
            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER
                    .debug("RolapSchema.createDataSourceChangeListener: " + "create datasource change listener \""
                        + dataSourceChangeListenerStr);
            }
        }
        return changeListener;
    }

    /**
     * Returns the checksum of this schema. Returns
     * <code>null</code> if {@link RolapConnectionProperties#UseContentChecksum}
     * is set to false.
     *
     * @return MD5 checksum of this schema
     */
    public ByteString getChecksum() {
        return this.md5Bytes;
    }

    /**
     * Connection for purposes of parsing and validation. Careful! It won't
     * have the correct locale or access-control profile.
     */
    public RolapConnection getInternalConnection() {
        return this.internalConnection;
    }

    // package-local visibility for testing purposes
    RolapStar makeRolapStar(final MondrianDef.Relation fact) {
        final javax.sql.DataSource dataSource = this.getInternalConnection().getDataSource();
        return new RolapStar(this, dataSource, fact);
    }

    /**
     * <code>RolapStarRegistry</code> is a registry for {@link RolapStar}s.
     */
    public class RolapStarRegistry {
        private final Map<List<String>, RolapStar> stars = new HashMap<List<String>, RolapStar>();

        RolapStarRegistry() {
        }

        /**
         * Looks up a {@link RolapStar}, creating it if it does not exist.
         *
         * <p> {@link RolapStar.Table#addJoin} works in a similar way.
         */
        synchronized RolapStar getOrCreateStar(final MondrianDef.Relation fact) {
            final List<String> rolapStarKey = RolapUtil.makeRolapStarKey(fact);
            RolapStar star = this.stars.get(rolapStarKey);
            if (star == null) {
                star = RolapSchema.this.makeRolapStar(fact);
                this.stars.put(rolapStarKey, star);
                // let cache manager load pending segments
                // from external cache if needed
                MondrianServer
                    .forConnection(RolapSchema.this.internalConnection)
                        .getAggregationManager()
                        .getCacheMgr()
                        .loadCacheForStar(star);
            }
            return star;
        }

        synchronized RolapStar getStar(List<String> starKey) {
            return this.stars.get(starKey);
        }

        synchronized Collection<RolapStar> getStars() {
            return this.stars.values();
        }
    }

    private final RolapStarRegistry rolapStarRegistry = new RolapStarRegistry();

    public RolapStarRegistry getRolapStarRegistry() {
        return this.rolapStarRegistry;
    }

    /**
     * Function table which contains all of the user-defined functions in this
     * schema, plus all of the standard functions.
     */
    static class RolapSchemaFunctionTable extends FunTableImpl {
        private final List<UdfResolver.UdfFactory> udfFactoryList;

        RolapSchemaFunctionTable(Collection<UdfResolver.UdfFactory> udfs) {
            this.udfFactoryList = new ArrayList<UdfResolver.UdfFactory>(udfs);
        }

        @Override
        public void defineFunctions(Builder builder) {
            final FunTable globalFunTable = GlobalFunTable.instance();
            for (final String reservedWord : globalFunTable.getReservedWords()) {
                builder.defineReserved(reservedWord);
            }
            for (final Resolver resolver : globalFunTable.getResolvers()) {
                builder.define(resolver);
            }
            for (final UdfResolver.UdfFactory udfFactory : this.udfFactoryList) {
                builder.define(new UdfResolver(udfFactory));
            }
        }
    }

    public RolapStar getStar(final String factTableName) {
        return this.getStar(RolapUtil.makeRolapStarKey(factTableName));
    }

    public RolapStar getStar(final List<String> starKey) {
        return this.getRolapStarRegistry().getStar(starKey);
    }

    public Collection<RolapStar> getStars() {
        return this.getRolapStarRegistry().getStars();
    }

    final RolapNativeRegistry nativeRegistry = new RolapNativeRegistry();

    RolapNativeRegistry getNativeRegistry() {
        return this.nativeRegistry;
    }

    /**
     * @return Returns the dataSourceChangeListener.
     */
    public DataSourceChangeListener getDataSourceChangeListener() {
        return this.dataSourceChangeListener;
    }

    /**
     * @param dataSourceChangeListener The dataSourceChangeListener to set.
     */
    public void setDataSourceChangeListener(DataSourceChangeListener dataSourceChangeListener) {
        this.dataSourceChangeListener = dataSourceChangeListener;
    }

    /**
     * Location of a node in an XML document.
     */
    private interface XmlLocation {
    }
}

// End RolapSchema.java
