/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2012 Pentaho and others
// All Rights Reserved.
*/

package mondrian.olap;

import mondrian.resource.MondrianResource;
import mondrian.spi.MemberFormatter;

/**
 * Skeleton implementation of {@link Level}.
 *
 * @author jhyde
 * @since 6 August, 2001
 */
public abstract class LevelBase extends OlapElementBase implements Level {
    protected final Hierarchy hierarchy;
    protected final String name;
    protected final String uniqueName;
    protected final String description;
    protected final int depth;
    protected final org.olap4j.metadata.Level.Type levelType;
    protected MemberFormatter memberFormatter;
    protected int approxRowCount;

    protected LevelBase(Hierarchy hierarchy, String name, String caption, boolean visible, String description, int depth,
        org.olap4j.metadata.Level.Type levelType) {
        this.hierarchy = hierarchy;
        this.name = name;
        this.caption = caption;
        this.visible = visible;
        this.description = description;
        this.uniqueName = Util.makeFqName(hierarchy, name);
        this.depth = depth;
        this.levelType = levelType;
    }

    /**
     * Sets the approximate number of members in this Level.
     * @see #getApproxRowCount()
     */
    public void setApproxRowCount(int approxRowCount) {
        this.approxRowCount = approxRowCount;
    }

    // from Element
    @Override
    public String getQualifiedName() {
        return MondrianResource.instance().MdxLevelName.str(this.getUniqueName());
    }

    @Override
    public org.olap4j.metadata.Level.Type getLevelType() {
        return this.levelType;
    }

    @Override
    public String getUniqueName() {
        return this.uniqueName;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public Hierarchy getHierarchy() {
        return this.hierarchy;
    }

    @Override
    public Dimension getDimension() {
        return this.hierarchy.getDimension();
    }

    @Override
    public int getDepth() {
        return this.depth;
    }

    @Override
    public Level getChildLevel() {
        final int childDepth = this.depth + 1;
        final Level[] levels = this.hierarchy.getLevels();
        return (childDepth < levels.length) ? levels[childDepth] : null;
    }

    @Override
    public Level getParentLevel() {
        final int parentDepth = this.depth - 1;
        final Level[] levels = this.hierarchy.getLevels();
        return (parentDepth >= 0) ? levels[parentDepth] : null;
    }

    @Override
    public abstract boolean isAll();

    public boolean isMeasure() {
        return this.hierarchy.getName().equals("Measures");
    }

    @Override
    public OlapElement lookupChild(SchemaReader schemaReader, Id.Segment s, MatchType matchType) {
        if (this.areMembersUnique() && (s instanceof Id.NameSegment)) {
            return Util.lookupHierarchyRootMember(schemaReader, this.hierarchy, ((Id.NameSegment) s), matchType);
        } else {
            return null;
        }
    }

    @Override
    public MemberFormatter getMemberFormatter() {
        return this.memberFormatter;
    }
}

// End LevelBase.java
