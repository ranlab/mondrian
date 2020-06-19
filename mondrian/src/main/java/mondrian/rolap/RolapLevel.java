/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2018 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.olap4j.impl.UnmodifiableArrayMap;

import mondrian.olap.Annotation;
import mondrian.olap.Dimension;
import mondrian.olap.DimensionType;
import mondrian.olap.Id;
import mondrian.olap.Level;
import mondrian.olap.LevelBase;
import mondrian.olap.MatchType;
import mondrian.olap.Member;
import mondrian.olap.MondrianDef;
import mondrian.olap.OlapElement;
import mondrian.olap.Property;
import mondrian.olap.SchemaReader;
import mondrian.olap.Util;
import mondrian.resource.MondrianResource;
import mondrian.rolap.format.FormatterCreateContext;
import mondrian.rolap.format.FormatterFactory;
import mondrian.spi.Dialect;
import mondrian.spi.PropertyFormatter;

/**
 * <code>RolapLevel</code> implements {@link Level} for a ROLAP database.
 *
 * @author jhyde
 * @since 10 August, 2001
 */
public class RolapLevel extends LevelBase {

    private static final Logger LOGGER = Logger.getLogger(RolapLevel.class);

    /**
     * The column or expression which yields the level's key.
     */
    protected MondrianDef.Expression keyExp;

    /**
     * The column or expression which yields the level's ordinal.
     */
    protected MondrianDef.Expression ordinalExp;

    /**
     * The column or expression which yields the level members' caption.
     */
    protected MondrianDef.Expression captionExp;

    private final Dialect.Datatype datatype;

    private final int flags;

    static final int FLAG_ALL = 0x02;

    /**
     * For SQL generator. Whether values of "column" are unique globally
     * unique (as opposed to unique only within the context of the parent
     * member).
     */
    static final int FLAG_UNIQUE = 0x04;

    private RolapLevel closedPeerLevel;

    protected RolapProperty[] properties;
    private final RolapProperty[] inheritedProperties;

    /**
     * Ths expression which gives the name of members of this level. If null,
     * members are named using the key expression.
     */
    protected MondrianDef.Expression nameExp;
    /** The expression which joins to the parent member in a parent-child
     * hierarchy, or null if this is a regular hierarchy. */
    protected MondrianDef.Expression parentExp;
    /** Value which indicates a null parent in a parent-child hierarchy. */
    private final String nullParentValue;

    /** Condition under which members are hidden. */
    private final HideMemberCondition hideMemberCondition;
    protected final MondrianDef.Closure xmlClosure;
    private final Map<String, Annotation> annotationMap;
    private final SqlStatement.Type internalType; // may be null

    /**
     * Creates a level.
     *
     * @pre parentExp != null || nullParentValue == null
     * @pre properties != null
     * @pre levelType != null
     * @pre hideMemberCondition != null
     */
    RolapLevel(RolapHierarchy hierarchy, String name, String caption, boolean visible, String description, int depth,
        MondrianDef.Expression keyExp, MondrianDef.Expression nameExp, MondrianDef.Expression captionExp, MondrianDef.Expression ordinalExp,
        MondrianDef.Expression parentExp, String nullParentValue, MondrianDef.Closure xmlClosure, RolapProperty[] properties, int flags,
        Dialect.Datatype datatype, SqlStatement.Type internalType, HideMemberCondition hideMemberCondition,
        org.olap4j.metadata.Level.Type levelType, String approxRowCount, Map<String, Annotation> annotationMap) {
        super(hierarchy, name, caption, visible, description, depth, levelType);
        assert annotationMap != null;
        Util.assertPrecondition(properties != null, "properties != null");
        Util.assertPrecondition(hideMemberCondition != null, "hideMemberCondition != null");
        Util.assertPrecondition(levelType != null, "levelType != null");

        if (keyExp instanceof MondrianDef.Column) {
            this.checkColumn((MondrianDef.Column) keyExp);
        }
        this.annotationMap = annotationMap;
        this.approxRowCount = this.loadApproxRowCount(approxRowCount);
        this.flags = flags;
        this.datatype = datatype;
        this.keyExp = keyExp;
        if (nameExp != null) {
            if (nameExp instanceof MondrianDef.Column) {
                this.checkColumn((MondrianDef.Column) nameExp);
            }
        }
        this.nameExp = nameExp;
        if (captionExp != null) {
            if (captionExp instanceof MondrianDef.Column) {
                this.checkColumn((MondrianDef.Column) captionExp);
            }
        }
        this.captionExp = captionExp;
        if (ordinalExp != null) {
            if (ordinalExp instanceof MondrianDef.Column) {
                this.checkColumn((MondrianDef.Column) ordinalExp);
            }
            this.ordinalExp = ordinalExp;
        } else {
            this.ordinalExp = this.keyExp;
        }
        if (parentExp instanceof MondrianDef.Column) {
            this.checkColumn((MondrianDef.Column) parentExp);
        }
        this.parentExp = parentExp;
        if (parentExp != null) {
            Util.assertTrue(!this.isAll(), "'All' level '" + this + "' must not be parent-child");
            Util.assertTrue(this.isUnique(), "Parent-child level '" + this + "' must have uniqueMembers=\"true\"");
        }
        this.nullParentValue = nullParentValue;
        Util.assertPrecondition((parentExp != null) || (nullParentValue == null), "parentExp != null || nullParentValue == null");
        this.xmlClosure = xmlClosure;
        for (final RolapProperty property : properties) {
            if (property.getExp() instanceof MondrianDef.Column) {
                this.checkColumn((MondrianDef.Column) property.getExp());
            }
        }
        this.properties = properties;
        final List<Property> list = new ArrayList<Property>();
        for (Level level = this; level != null; level = level.getParentLevel()) {
            final Property[] levelProperties = level.getProperties();
            for (final Property levelProperty : levelProperties) {
                final Property existingProperty = this.lookupProperty(list, levelProperty.getName());
                if (existingProperty == null) {
                    list.add(levelProperty);
                } else if (existingProperty.getType() != levelProperty.getType()) {
                    throw Util
                        .newError("Property " + this.getName()
                            + "."
                            + levelProperty.getName()
                            + " overrides a "
                            + "property with the same name but different type");
                }
            }
        }
        this.inheritedProperties = list.toArray(new RolapProperty[list.size()]);

        final Dimension dim = hierarchy.getDimension();
        if (dim.getDimensionType() == DimensionType.TimeDimension) {
            if (!levelType.isTime() && !this.isAll()) {
                throw MondrianResource.instance().NonTimeLevelInTimeHierarchy.ex(this.getUniqueName());
            }
        } else if (dim.getDimensionType() == null) {
            // there was no dimension type assigned to the dimension
            // - check later
        } else {
            if (levelType.isTime()) {
                throw MondrianResource.instance().TimeLevelInNonTimeHierarchy.ex(this.getUniqueName());
            }
        }
        this.internalType = internalType;
        this.hideMemberCondition = hideMemberCondition;
    }

    @Override
    public RolapHierarchy getHierarchy() {
        return (RolapHierarchy) this.hierarchy;
    }

    @Override
    public Map<String, Annotation> getAnnotationMap() {
        return this.annotationMap;
    }

    private int loadApproxRowCount(String approxRowCount) {
        final boolean notNullAndNumeric = (approxRowCount != null) && approxRowCount.matches("^\\d+$");
        if (notNullAndNumeric) {
            return Integer.parseInt(approxRowCount);
        } else {
            // if approxRowCount is not set, return MIN_VALUE to indicate
            return Integer.MIN_VALUE;
        }
    }

    @Override
    protected Logger getLogger() {
        return LOGGER;
    }

    String getTableName() {
        String tableName = null;

        final MondrianDef.Expression expr = this.getKeyExp();
        if (expr instanceof MondrianDef.Column) {
            final MondrianDef.Column mc = (MondrianDef.Column) expr;
            tableName = mc.getTableAlias();
        }
        return tableName;
    }

    public MondrianDef.Expression getKeyExp() {
        return this.keyExp;
    }

    public MondrianDef.Expression getOrdinalExp() {
        return this.ordinalExp;
    }

    public MondrianDef.Expression getCaptionExp() {
        return this.captionExp;
    }

    public boolean hasCaptionColumn() {
        return this.captionExp != null;
    }

    public boolean hasOrdinalExp() {
        return !this.getOrdinalExp().equals(this.getKeyExp());
    }

    final int getFlags() {
        return this.flags;
    }

    HideMemberCondition getHideMemberCondition() {
        return this.hideMemberCondition;
    }

    public final boolean isUnique() {
        return (this.flags & FLAG_UNIQUE) != 0;
    }

    public final Dialect.Datatype getDatatype() {
        return this.datatype;
    }

    final String getNullParentValue() {
        return this.nullParentValue;
    }

    /**
     * Returns whether this level is parent-child.
     */
    public boolean isParentChild() {
        return this.parentExp != null;
    }

    MondrianDef.Expression getParentExp() {
        return this.parentExp;
    }

    // RME: this has to be public for two of the DrillThroughTest test.
    public MondrianDef.Expression getNameExp() {
        return this.nameExp;
    }

    private Property lookupProperty(List<Property> list, String propertyName) {
        for (final Property property : list) {
            if (property.getName().equals(propertyName)) {
                return property;
            }
        }
        return null;
    }

    RolapLevel(RolapHierarchy hierarchy, int depth, MondrianDef.Level xmlLevel) {
        this(hierarchy, xmlLevel.name, xmlLevel.caption, xmlLevel.visible, xmlLevel.description, depth, xmlLevel.getKeyExp(),
            xmlLevel.getNameExp(), xmlLevel.getCaptionExp(), xmlLevel.getOrdinalExp(), xmlLevel.getParentExp(), xmlLevel.nullParentValue,
            xmlLevel.closure, createProperties(xmlLevel), (xmlLevel.uniqueMembers ? FLAG_UNIQUE : 0), xmlLevel.getDatatype(),
            toInternalType(xmlLevel.internalType), HideMemberCondition.valueOf(xmlLevel.hideMemberIf), //
            RolapLevel.convertLevelTypeNameToLevelType(xmlLevel.levelType), //
            xmlLevel.approxRowCount, RolapHierarchy.createAnnotationMap(xmlLevel.annotations));

        if (!Util.isEmpty(xmlLevel.caption)) {
            this.setCaption(xmlLevel.caption);
        }

        final FormatterCreateContext memberFormatterContext = new FormatterCreateContext.Builder(this.getUniqueName())
            .formatterDef(xmlLevel.memberFormatter)
                .formatterAttr(xmlLevel.formatter)
                .build();
        this.memberFormatter = FormatterFactory.instance().createRolapMemberFormatter(memberFormatterContext);
    }

    // helper for constructor
    private static RolapProperty[] createProperties(MondrianDef.Level xmlLevel) {
        final List<RolapProperty> list = new ArrayList<RolapProperty>();
        final MondrianDef.Expression nameExp = xmlLevel.getNameExp();

        if (nameExp != null) {
            list
                .add(new RolapProperty(Property.NAME.name, Property.Datatype.TYPE_STRING, nameExp, null, null, null, true,
                    Property.NAME.description));
        }
        for (int i = 0; i < xmlLevel.properties.length; i++) {
            final MondrianDef.Property xmlProperty = xmlLevel.properties[i];

            final FormatterCreateContext formatterContext = new FormatterCreateContext.Builder(xmlProperty.name)
                .formatterDef(xmlProperty.propertyFormatter)
                    .formatterAttr(xmlProperty.formatter)
                    .build();
            final PropertyFormatter formatter = FormatterFactory.instance().createPropertyFormatter(formatterContext);

            list
                .add(new RolapProperty(xmlProperty.name, convertPropertyTypeNameToCode(xmlProperty.type), xmlLevel.getPropertyExp(i),
                    formatter, xmlProperty.caption, xmlLevel.properties[i].dependsOnLevelValue, false, xmlProperty.description));
        }
        return list.toArray(new RolapProperty[list.size()]);
    }

    private static Property.Datatype convertPropertyTypeNameToCode(String type) {
        if (type.equals("String")) {
            return Property.Datatype.TYPE_STRING;
        } else if (type.equals("Numeric")) {
            return Property.Datatype.TYPE_NUMERIC;
        } else if (type.equals("Integer")) {
            return Property.Datatype.TYPE_INTEGER;
        } else if (type.equals("Long")) {
            return Property.Datatype.TYPE_LONG;
        } else if (type.equals("Boolean")) {
            return Property.Datatype.TYPE_BOOLEAN;
        } else if (type.equals("Timestamp")) {
            return Property.Datatype.TYPE_TIMESTAMP;
        } else if (type.equals("Time")) {
            return Property.Datatype.TYPE_TIME;
        } else if (type.equals("Date")) {
            return Property.Datatype.TYPE_DATE;
        } else {
            throw Util.newError("Unknown property type '" + type + "'");
        }
    }

    /**
     * Konvertiert einen Wert aus {@link mondrian.olap.MondrianDef.Level._levelType_values} zu einem
     * LevelType.
     * @param newLevelType obligatorisch, Stringrepräsentation von einem Level-Type
     * @return obligatorisch, Level-Type
     */
    private static org.olap4j.metadata.Level.Type convertLevelTypeNameToLevelType(java.lang.String newLevelType) {
        org.olap4j.metadata.Level.Type levelType;
        switch (newLevelType) {
            case "Regular":
                levelType = org.olap4j.metadata.Level.Type.REGULAR;
                break;
            case "TimeYears":
                levelType = org.olap4j.metadata.Level.Type.TIME_YEARS;
                break;
            case "TimeHalfYears":
            case "TimeHalfYear":
                levelType = org.olap4j.metadata.Level.Type.TIME_HALF_YEAR;
                break;
            case "TimeQuarters":
                levelType = org.olap4j.metadata.Level.Type.TIME_QUARTERS;
                break;
            case "TimeMonths":
                levelType = org.olap4j.metadata.Level.Type.TIME_MONTHS;
                break;
            case "TimeWeeks":
                levelType = org.olap4j.metadata.Level.Type.TIME_WEEKS;
                break;
            case "TimeDays":
                levelType = org.olap4j.metadata.Level.Type.TIME_DAYS;
                break;
            case "TimeHours":
                levelType = org.olap4j.metadata.Level.Type.TIME_HOURS;
                break;
            case "TimeMinutes":
                levelType = org.olap4j.metadata.Level.Type.TIME_MINUTES;
                break;
            case "TimeSeconds":
                levelType = org.olap4j.metadata.Level.Type.TIME_SECONDS;
                break;
            case "TimeUndefined":
                levelType = org.olap4j.metadata.Level.Type.TIME_UNDEFINED;
                break;
            default:
                levelType = org.olap4j.metadata.Level.Type.NULL;
        }
        return levelType;
    }

    private void checkColumn(MondrianDef.Column nameColumn) {
        final RolapHierarchy rolapHierarchy = (RolapHierarchy) this.hierarchy;
        if (nameColumn.table == null) {
            final MondrianDef.Relation table = rolapHierarchy.getUniqueTable();
            if (table == null) {
                throw Util
                    .newError("must specify a table for level " + this.getUniqueName() + " because hierarchy has more than one table");
            }
            nameColumn.table = table.getAlias();
        } else {
            if (!rolapHierarchy.tableExists(nameColumn.table)) {
                throw Util.newError("Table '" + nameColumn.table + "' not found");
            }
        }
    }

    void init(MondrianDef.CubeDimension xmlDimension) {
        if (this.xmlClosure != null) {
            final RolapDimension dimension = ((RolapHierarchy) this.hierarchy)
                .createClosedPeerDimension(this, this.xmlClosure, xmlDimension);
            this.closedPeerLevel = (RolapLevel) dimension.getHierarchies()[0].getLevels()[1];
        }
    }

    @Override
    public final boolean isAll() {
        return (this.flags & FLAG_ALL) != 0;
    }

    @Override
    public boolean areMembersUnique() {
        return (this.depth == 0) || ((this.depth == 1) && this.hierarchy.hasAll());
    }

    public String getTableAlias() {
        return this.keyExp.getTableAlias();
    }

    @Override
    public RolapProperty[] getProperties() {
        return this.properties;
    }

    @Override
    public Property[] getInheritedProperties() {
        return this.inheritedProperties;
    }

    @Override
    public int getApproxRowCount() {
        return this.approxRowCount;
    }

    private static final Map<String, SqlStatement.Type> VALUES = UnmodifiableArrayMap
        .of("int",
            SqlStatement.Type.INT,
            "double",
            SqlStatement.Type.DOUBLE,
            "Object",
            SqlStatement.Type.OBJECT,
            "String",
            SqlStatement.Type.STRING,
            "long",
            SqlStatement.Type.LONG,
            "date",
            SqlStatement.Type.DATE,
            "datetime",
            SqlStatement.Type.TIMESTAMP,
            "timestamp",
            SqlStatement.Type.TIMESTAMP,
            "time",
            SqlStatement.Type.TIME);

    private static SqlStatement.Type toInternalType(String internalTypeName) {
        final SqlStatement.Type type = VALUES.get(internalTypeName);
        if ((type == null) && (internalTypeName != null)) {
            throw Util
                .newError("Invalid value '" + internalTypeName
                    + "' for attribute 'internalType' of element 'Level'. "
                    + "Valid values are: "
                    + VALUES.keySet());
        }
        return type;
    }

    public SqlStatement.Type getInternalType() {
        return this.internalType;
    }

    /**
     * Conditions under which a level's members may be hidden (thereby creating
     * a <dfn>ragged hierarchy</dfn>).
     */
    public enum HideMemberCondition {
        /** A member always appears. */
        Never,

        /** A member doesn't appear if its name is null or empty. */
        IfBlankName,

        /** A member appears unless its name matches its parent's. */
        IfParentsName
    }

    public OlapElement lookupChild(SchemaReader schemaReader, Id.Segment name) {
        return this.lookupChild(schemaReader, name, MatchType.EXACT);
    }

    @Override
    public OlapElement lookupChild(SchemaReader schemaReader, Id.Segment name, MatchType matchType) {
        if (name instanceof Id.KeySegment) {
            final Id.KeySegment keySegment = (Id.KeySegment) name;
            final List<Comparable> keyValues = new ArrayList<Comparable>();
            for (final Id.NameSegment nameSegment : keySegment.getKeyParts()) {
                final String keyValue = nameSegment.name;
                if (RolapUtil.mdxNullLiteral().equalsIgnoreCase(keyValue)) {
                    keyValues.add(RolapUtil.sqlNullValue);
                } else {
                    keyValues.add(keyValue);
                }
            }
            final List<MondrianDef.Expression> keyExps = this.getInheritedKeyExps();
            if (keyExps.size() != keyValues.size()) {
                throw Util
                    .newError("Wrong number of values in member key; " + keySegment
                        + " has "
                        + keyValues.size()
                        + " values, whereas level's key has "
                        + keyExps.size()
                        + " columns "
                        + new AbstractList<String>() {
                            @Override
                            public String get(int index) {
                                return keyExps.get(index).getGenericExpression();
                            }

                            @Override
                            public int size() {
                                return keyExps.size();
                            }
                        }
                        + ".");
            }
            return this.getHierarchy().getMemberReader().getMemberByKey(this, keyValues);
        }
        final List<Member> levelMembers = schemaReader.getLevelMembers(this, true);
        if (levelMembers.size() > 0) {
            final Member parent = levelMembers.get(0).getParentMember();
            return RolapUtil.findBestMemberMatch(levelMembers, (RolapMember) parent, this, name, matchType);
        }
        return null;
    }

    private List<MondrianDef.Expression> getInheritedKeyExps() {
        final List<MondrianDef.Expression> list = new ArrayList<MondrianDef.Expression>();
        for (RolapLevel x = this;; x = (RolapLevel) x.getParentLevel()) {
            final MondrianDef.Expression keyExp1 = x.getKeyExp();
            if (keyExp1 != null) {
                list.add(keyExp1);
            }
            if (x.isUnique()) {
                break;
            }
        }
        return list;
    }

    /**
     * Indicates that level is not ragged and not a parent/child level.
     */
    public boolean isSimple() {
        // most ragged hierarchies are not simple -- see isTooRagged.
        if (this.isTooRagged()) {
            return false;
        }
        if (this.isParentChild()) {
            return false;
        }
        // does not work for measures
        if (this.isMeasure()) {
            return false;
        }
        return true;
    }

    /**
     * Determines whether the specified level is too ragged for native
     * evaluation, which is able to handle one special case of a ragged
     * hierarchy: when the level specified in the query is the leaf level of
     * the hierarchy and HideMemberCondition for the level is IfBlankName.
     * This is true even if higher levels of the hierarchy can be hidden
     * because even in that case the only column that needs to be read is the
     * column that holds the leaf. IfParentsName can't be handled even at the
     * leaf level because in the general case we aren't reading the column
     * that holds the parent. Also, IfBlankName can't be handled for non-leaf
     * levels because we would have to read the column for the next level
     * down for members with blank names.
     *
     * @return true if the specified level is too ragged for native
     *         evaluation.
     */
    private boolean isTooRagged() {
        // Is this the special case of raggedness that native evaluation
        // is able to handle?
        if (this.getDepth() == (this.getHierarchy().getLevels().length - 1)) {
            switch (this.getHideMemberCondition()) {
                case Never:
                case IfBlankName:
                    return false;
                default:
                    return true;
            }
        }
        // Handle the general case in the traditional way.
        return this.getHierarchy().isRagged();
    }

    /**
     * Returns true when the level is part of a parent/child hierarchy and has
     * an equivalent closed level.
     */
    boolean hasClosedPeer() {
        return this.closedPeerLevel != null;
    }

    public RolapLevel getClosedPeer() {
        return this.closedPeerLevel;
    }

    public static RolapLevel lookupLevel(RolapLevel[] levels, String levelName) {
        for (final RolapLevel level : levels) {
            if (level.getName().equals(levelName)) {
                return level;
            }
        }
        return null;
    }

}
// End RolapLevel.java
