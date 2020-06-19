/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho and others
// All Rights Reserved.
*/

package mondrian.rolap;

import mondrian.olap.MondrianDef;
import mondrian.olap.Util;
import mondrian.rolap.agg.CellRequest;
import mondrian.rolap.agg.MemberColumnPredicate;
import mondrian.rolap.agg.MemberTuplePredicate;
import mondrian.rolap.agg.RangeColumnPredicate;
import mondrian.rolap.agg.ValueColumnPredicate;
import mondrian.spi.MemberFormatter;

/**
 * RolapCubeLevel wraps a RolapLevel for a specific Cube.
 *
 * @author Will Gorman, 19 October 2007
 */
public class RolapCubeLevel extends RolapLevel {

    private final RolapLevel rolapLevel;
    private RolapStar.Column starKeyColumn = null;
    /**
     * For a parent-child hierarchy with a closure provided by the schema,
     * the equivalent level in the closed hierarchy; otherwise null.
     */
    private RolapCubeLevel closedPeerCubeLevel;
    protected LevelReader levelReader;
    private final RolapCubeHierarchy cubeHierarchy;
    private final RolapCubeDimension cubeDimension;
    private final RolapCube cube;
    private final RolapCubeLevel parentCubeLevel;
    private RolapCubeLevel childCubeLevel;

    public RolapCubeLevel(RolapLevel level, RolapCubeHierarchy cubeHierarchy) {
        super(cubeHierarchy, level.getName(), level.getCaption(), level.isVisible(), level.getDescription(), level.getDepth(),
            level.getKeyExp(), level.getNameExp(), level.getCaptionExp(), level.getOrdinalExp(), level.getParentExp(),
            level.getNullParentValue(), null, level.getProperties(), level.getFlags(), level.getDatatype(), level.getInternalType(),
            level.getHideMemberCondition(), level.getLevelType(), "" + level.getApproxRowCount(), level.getAnnotationMap());

        this.rolapLevel = level;
        this.cubeHierarchy = cubeHierarchy;
        this.cubeDimension = (RolapCubeDimension) cubeHierarchy.getDimension();
        this.cube = this.cubeDimension.getCube();
        this.parentCubeLevel = (RolapCubeLevel) super.getParentLevel();
        if (this.parentCubeLevel != null) {
            this.parentCubeLevel.childCubeLevel = this;
        }
        final MondrianDef.RelationOrJoin hierarchyRel = cubeHierarchy.getRelation();
        this.keyExp = this.convertExpression(level.getKeyExp(), hierarchyRel);
        this.nameExp = this.convertExpression(level.getNameExp(), hierarchyRel);
        this.captionExp = this.convertExpression(level.getCaptionExp(), hierarchyRel);
        this.ordinalExp = this.convertExpression(level.getOrdinalExp(), hierarchyRel);
        this.parentExp = this.convertExpression(level.getParentExp(), hierarchyRel);
        this.properties = this.convertProperties(level.getProperties(), hierarchyRel);
    }

    @Override
    void init(MondrianDef.CubeDimension xmlDimension) {
        if (this.isAll()) {
            this.levelReader = new AllLevelReaderImpl();
        } else if (this.getLevelType() == org.olap4j.metadata.Level.Type.NULL) {
            this.levelReader = new NullLevelReader();
        } else if (this.rolapLevel.xmlClosure != null) {
            final RolapDimension dimension = (RolapDimension) this.rolapLevel.getClosedPeer().getHierarchy().getDimension();

            final RolapCubeDimension cubeDimension = new RolapCubeDimension(this.getCube(), dimension, xmlDimension,
                this.getDimension().getName() + "$Closure", -1, this.getCube().hierarchyList, this.getDimension().isHighCardinality());

            // RME HACK
            //  WG: Note that the reason for registering this usage is so that
            //  when registerDimension is called, the hierarchy is registered
            //  successfully to the star.  This type of hack will go away once
            //  HierarchyUsage is phased out
            if (!this.getCube().isVirtual()) {
                this.getCube().createUsage((RolapCubeHierarchy) cubeDimension.getHierarchies()[0], xmlDimension);
            }
            cubeDimension.init(xmlDimension);
            this.getCube().registerDimension(cubeDimension);
            this.closedPeerCubeLevel = (RolapCubeLevel) cubeDimension.getHierarchies()[0].getLevels()[1];

            if (!this.getCube().isVirtual()) {
                this.getCube().closureColumnBitKey.set(this.closedPeerCubeLevel.starKeyColumn.getBitPosition());
            }

            this.levelReader = new ParentChildLevelReaderImpl(this);
        } else {
            this.levelReader = new RegularLevelReader(this);
        }
    }

    private RolapProperty[] convertProperties(RolapProperty[] properties, MondrianDef.RelationOrJoin rel) {
        if (properties == null) {
            return null;
        }

        final RolapProperty[] convertedProperties = new RolapProperty[properties.length];
        for (int i = 0; i < properties.length; i++) {
            final RolapProperty old = properties[i];
            convertedProperties[i] = new RolapProperty(old.getName(), old.getType(), this.convertExpression(old.getExp(), rel),
                old.getFormatter(), old.getCaption(), old.dependsOnLevelValue(), old.isInternal(), old.getDescription());
        }
        return convertedProperties;
    }

    /**
     * Converts an expression to new aliases if necessary.
     *
     * @param exp the expression to convert
     * @param rel the parent relation
     * @return returns the converted expression
     */
    private MondrianDef.Expression convertExpression(MondrianDef.Expression exp, MondrianDef.RelationOrJoin rel) {
        if (this.getHierarchy().isUsingCubeFact()) {
            // no conversion necessary
            return exp;
        } else if ((exp == null) || (rel == null)) {
            return null;
        } else if (exp instanceof MondrianDef.Column) {
            final MondrianDef.Column col = (MondrianDef.Column) exp;
            if (rel instanceof MondrianDef.Table) {
                return new MondrianDef.Column(((MondrianDef.Table) rel).getAlias(), col.getColumnName());
            } else if ((rel instanceof MondrianDef.Join) || (rel instanceof MondrianDef.Relation)) {
                // need to determine correct name of alias for this level.
                // this may be defined in level
                // col.table
                final String alias = this.getHierarchy().lookupAlias(col.getTableAlias());
                return new MondrianDef.Column(alias, col.getColumnName());
            }
        } else if (exp instanceof MondrianDef.ExpressionView) {
            // this is a limitation, in the future, we may need
            // to replace the table name in the sql provided
            // with the new aliased name
            return exp;
        }
        throw new RuntimeException("conversion of Class " + exp.getClass() + " unsupported at this time");
    }

    public void setStarKeyColumn(RolapStar.Column column) {
        this.starKeyColumn = column;
    }

    /**
     * This is the RolapStar.Column that is related to this RolapCubeLevel
     *
     * @return the RolapStar.Column related to this RolapCubeLevel
     */
    public RolapStar.Column getStarKeyColumn() {
        return this.starKeyColumn;
    }

    LevelReader getLevelReader() {
        return this.levelReader;
    }

    /**
     * this method returns the RolapStar.Column if non-virtual,
     * if virtual, find the base cube level and return it's
     * column
     *
     * @param baseCube the base cube for the specificed virtual level
     * @return the RolapStar.Column related to this RolapCubeLevel
     */
    public RolapStar.Column getBaseStarKeyColumn(RolapCube baseCube) {
        RolapStar.Column column = null;
        if (this.getCube().isVirtual() && (baseCube != null)) {
            final RolapCubeLevel lvl = baseCube.findBaseCubeLevel(this);
            if (lvl != null) {
                column = lvl.getStarKeyColumn();
            }
        } else {
            column = this.getStarKeyColumn();
        }
        return column;
    }

    /**
     * Returns the (non virtual) cube this level belongs to.
     *
     * @return cube
     */
    public final RolapCube getCube() {
        return this.cube;
    }

    // override with stricter return type
    @Override
    public final RolapCubeDimension getDimension() {
        return this.cubeDimension;
    }

    // override with stricter return type
    @Override
    public final RolapCubeHierarchy getHierarchy() {
        return this.cubeHierarchy;
    }

    // override with stricter return type
    @Override
    public final RolapCubeLevel getChildLevel() {
        return this.childCubeLevel;
    }

    // override with stricter return type
    @Override
    public final RolapCubeLevel getParentLevel() {
        return this.parentCubeLevel;
    }

    @Override
    public String getCaption() {
        return this.rolapLevel.getCaption();
    }

    @Override
    public void setCaption(String caption) {
        // Cannot set the caption on the underlying level; other cube levels
        // might be using it.
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the underlying level.
     *
     * @return Underlying level
     */
    public RolapLevel getRolapLevel() {
        return this.rolapLevel;
    }

    public boolean equals(RolapCubeLevel level) {
        if (this == level) {
            return true;
        }
        // verify the levels are part of the same hierarchy
        return super.equals(level) && this.getCube().equals(level.getCube());
    }

    @Override
    boolean hasClosedPeer() {
        return this.closedPeerCubeLevel != null;
    }

    @Override
    public RolapCubeLevel getClosedPeer() {
        return this.closedPeerCubeLevel;
    }

    @Override
    public MemberFormatter getMemberFormatter() {
        return this.rolapLevel.getMemberFormatter();
    }

    /**
     * Encapsulation of the difference between levels in terms of how
     * constraints are generated. There are implementations for 'all' levels,
     * the 'null' level, parent-child levels and regular levels.
     */
    interface LevelReader {

        /**
         * Adds constraints to a cell request for a member of this level.
         *
         * @param member Member to be constrained
         * @param baseCube base cube if virtual level
         * @param request Request to be constrained
         *
         * @return true if request is unsatisfiable (e.g. if the member is the
         * null member)
         */
        boolean constrainRequest(RolapCubeMember member, RolapCube baseCube, CellRequest request);

        /**
         * Adds constraints to a cache region for a member of this level.
         *
         * @param predicate Predicate
         * @param baseCube base cube if virtual level
         * @param cacheRegion Cache region to be constrained
         */
        void constrainRegion(StarColumnPredicate predicate, RolapCube baseCube, RolapCacheRegion cacheRegion);
    }

    /**
     * Level reader for a regular level.
     */
    static final class RegularLevelReader implements LevelReader {
        private final RolapCubeLevel cubeLevel;

        RegularLevelReader(RolapCubeLevel cubeLevel) {
            this.cubeLevel = cubeLevel;
        }

        @Override
        public boolean constrainRequest(RolapCubeMember member, RolapCube baseCube, CellRequest request) {
            assert member.getLevel() == this.cubeLevel;
            final Object memberKey = member.member.getKey();
            if (memberKey == null) {
                if (member == member.getHierarchy().getNullMember()) {
                    // cannot form a request if one of the members is null
                    return true;
                } else {
                    throw Util.newInternal("why is key null?");
                }
            }

            final RolapStar.Column column = this.cubeLevel.getBaseStarKeyColumn(baseCube);

            if (column == null) {
                // This hierarchy is not one which qualifies the starMeasure
                // (this happens in virtual cubes). The starMeasure only has
                // a value for the 'all' member of the hierarchy (or for the
                // default member if the hierarchy has no 'all' member)
                return (member != this.cubeLevel.hierarchy.getDefaultMember()) || this.cubeLevel.hierarchy.hasAll();
            }

            final boolean isMemberCalculated = member.member.isCalculated();

            final StarColumnPredicate predicate;
            if (isMemberCalculated && !member.isParentChildLeaf()) {
                predicate = null;
            } else {
                predicate = new ValueColumnPredicate(column, memberKey);
            }

            // use the member as constraint; this will give us some
            //  optimization potential
            request.addConstrainedColumn(column, predicate);

            if (request.extendedContext && (this.cubeLevel.getNameExp() != null)) {
                final RolapStar.Column nameColumn = column.getNameColumn();

                assert nameColumn != null;
                request.addConstrainedColumn(nameColumn, null);
            }

            if (isMemberCalculated) {
                return false;
            }

            // If member is unique without reference to its parent,
            // no further constraint is required.
            if (this.cubeLevel.isUnique()) {
                return false;
            }

            // Constrain the parent member, if any.
            RolapCubeMember parent = member.getParentMember();
            while (true) {
                if (parent == null) {
                    return false;
                }
                final RolapCubeLevel level = parent.getLevel();
                final LevelReader levelReader = level.levelReader;
                if (levelReader == this) {
                    // We are looking at a parent in a parent-child hierarchy,
                    // for example, we have moved from Fred to Fred's boss,
                    // Wilma. We don't want to include Wilma's key in the
                    // request.
                    parent = parent.getParentMember();
                    continue;
                }
                return levelReader.constrainRequest(parent, baseCube, request);
            }
        }

        @Override
        public void constrainRegion(StarColumnPredicate predicate, RolapCube baseCube, RolapCacheRegion cacheRegion) {
            final RolapStar.Column column = this.cubeLevel.getBaseStarKeyColumn(baseCube);

            if (column == null) {
                // This hierarchy is not one which qualifies the starMeasure
                // (this happens in virtual cubes). The starMeasure only has
                // a value for the 'all' member of the hierarchy (or for the
                // default member if the hierarchy has no 'all' member)
                return;
            }

            if (predicate instanceof MemberColumnPredicate) {
                final MemberColumnPredicate memberColumnPredicate = (MemberColumnPredicate) predicate;
                final RolapMember member = memberColumnPredicate.getMember();
                assert member.getLevel() == this.cubeLevel;
                assert !member.isCalculated();
                assert memberColumnPredicate.getMember().getKey() != null;
                assert memberColumnPredicate.getMember().getKey() != RolapUtil.sqlNullValue;
                assert !member.isNull();

                // use the member as constraint, this will give us some
                //  optimization potential
                cacheRegion.addPredicate(column, predicate);
                return;
            } else if (predicate instanceof RangeColumnPredicate) {
                final RangeColumnPredicate rangeColumnPredicate = (RangeColumnPredicate) predicate;
                final ValueColumnPredicate lowerBound = rangeColumnPredicate.getLowerBound();
                RolapMember lowerMember;
                if (lowerBound == null) {
                    lowerMember = null;
                } else if (lowerBound instanceof MemberColumnPredicate) {
                    final MemberColumnPredicate memberColumnPredicate = (MemberColumnPredicate) lowerBound;
                    lowerMember = memberColumnPredicate.getMember();
                } else {
                    throw new UnsupportedOperationException();
                }
                final ValueColumnPredicate upperBound = rangeColumnPredicate.getUpperBound();
                RolapMember upperMember;
                if (upperBound == null) {
                    upperMember = null;
                } else if (upperBound instanceof MemberColumnPredicate) {
                    final MemberColumnPredicate memberColumnPredicate = (MemberColumnPredicate) upperBound;
                    upperMember = memberColumnPredicate.getMember();
                } else {
                    throw new UnsupportedOperationException();
                }
                final MemberTuplePredicate predicate2 = new MemberTuplePredicate(baseCube, lowerMember,
                    !rangeColumnPredicate.getLowerInclusive(), upperMember, !rangeColumnPredicate.getUpperInclusive());
                // use the member as constraint, this will give us some
                //  optimization potential
                cacheRegion.addPredicate(predicate2);
                return;
            }

            // Unknown type of constraint.
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Level reader for a parent-child level which has a closed peer level.
     */
    // final for performance
    static final class ParentChildLevelReaderImpl implements LevelReader {
        private final RegularLevelReader regularLevelReader;
        private final RolapCubeLevel closedPeerCubeLevel;
        private final RolapLevel closedPeerLevel;
        private final RolapMember wrappedAllMember;
        private final RolapCubeMember allMember;

        ParentChildLevelReaderImpl(RolapCubeLevel cubeLevel) {
            this.regularLevelReader = new RegularLevelReader(cubeLevel);

            // inline a bunch of fields for performance
            this.closedPeerCubeLevel = cubeLevel.closedPeerCubeLevel;
            this.closedPeerLevel = cubeLevel.rolapLevel.getClosedPeer();
            this.wrappedAllMember = (RolapMember) this.closedPeerLevel.getHierarchy().getDefaultMember();
            this.allMember = this.closedPeerCubeLevel.getHierarchy().getDefaultMember();
            assert this.allMember.isAll();
        }

        @Override
        public boolean constrainRequest(RolapCubeMember member, RolapCube baseCube, CellRequest request) {
            // Replace a parent/child level by its closed equivalent, when
            // available; this is always valid, and improves performance by
            // enabling the database to compute aggregates.
            if (member.getDataMember() == null) {
                // Member has no data member because it IS the data
                // member of a parent-child hierarchy member. Leave
                // it be. We don't want to aggregate.
                return this.regularLevelReader.constrainRequest(member, baseCube, request);
            } else if (request.drillThrough) {
                return this.regularLevelReader.constrainRequest(member.getDataMember(), baseCube, request);
            } else {
                // isn't creating a member on the fly a bad idea?
                final RolapMember wrappedMember = new RolapMemberBase(this.wrappedAllMember, this.closedPeerLevel, member.getKey());
                member = new RolapCubeMember(this.allMember, wrappedMember, this.closedPeerCubeLevel);

                return this.closedPeerCubeLevel.getLevelReader().constrainRequest(member, baseCube, request);
            }
        }

        @Override
        public void constrainRegion(StarColumnPredicate predicate, RolapCube baseCube, RolapCacheRegion cacheRegion) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Level reader for the level which contains the 'all' member.
     */
    static final class AllLevelReaderImpl implements LevelReader {
        @Override
        public boolean constrainRequest(RolapCubeMember member, RolapCube baseCube, CellRequest request) {
            // We don't need to apply any constraints.
            return false;
        }

        @Override
        public void constrainRegion(StarColumnPredicate predicate, RolapCube baseCube, RolapCacheRegion cacheRegion) {
            // We don't need to apply any constraints.
        }
    }

    /**
     * Level reader for the level which contains the null member.
     */
    static final class NullLevelReader implements LevelReader {
        @Override
        public boolean constrainRequest(RolapCubeMember member, RolapCube baseCube, CellRequest request) {
            return true;
        }

        @Override
        public void constrainRegion(StarColumnPredicate predicate, RolapCube baseCube, RolapCacheRegion cacheRegion) {
        }
    }

}

// End RolapCubeLevel.java
