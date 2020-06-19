/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2015 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.eigenbase.util.property.Property;
import org.olap4j.mdx.IdentifierSegment;

import mondrian.calc.Calc;
import mondrian.calc.DummyExp;
import mondrian.calc.ExpCompiler;
import mondrian.calc.impl.AbstractCalc;
import mondrian.calc.impl.GenericCalc;
import mondrian.olap.Access;
import mondrian.olap.Cube;
import mondrian.olap.Dimension;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.Hierarchy;
import mondrian.olap.Id;
import mondrian.olap.Level;
import mondrian.olap.Literal;
import mondrian.olap.MatchType;
import mondrian.olap.Member;
import mondrian.olap.MondrianProperties;
import mondrian.olap.NameResolver;
import mondrian.olap.NamedSet;
import mondrian.olap.NativeEvaluator;
import mondrian.olap.OlapElement;
import mondrian.olap.Parameter;
import mondrian.olap.ParameterImpl;
import mondrian.olap.Role;
import mondrian.olap.SchemaReader;
import mondrian.olap.Util;
import mondrian.olap.type.StringType;
import mondrian.rolap.sql.MemberChildrenConstraint;
import mondrian.rolap.sql.TupleConstraint;

/**
 * A <code>RolapSchemaReader</code> allows you to read schema objects while
 * observing the access-control profile specified by a given role.
 *
 * @author jhyde
 * @since Feb 24, 2003
 */
public class RolapSchemaReader implements SchemaReader, RolapNativeSet.SchemaReaderWithMemberReaderAvailable, NameResolver.Namespace {
    protected final Role role;
    private final Map<Hierarchy, MemberReader> hierarchyReaders = new ConcurrentHashMap<Hierarchy, MemberReader>();
    protected final RolapSchema schema;
    private final SqlConstraintFactory sqlConstraintFactory = SqlConstraintFactory.instance();
    private static final Logger LOGGER = Logger.getLogger(RolapSchemaReader.class);

    /**
     * Creates a RolapSchemaReader.
     *
     * @param role Role for access control, must not be null
     * @param schema Schema
     */
    RolapSchemaReader(Role role, RolapSchema schema) {
        assert role != null : "precondition: role != null";
        assert schema != null;
        this.role = role;
        this.schema = schema;
    }

    @Override
    public Role getRole() {
        return this.role;
    }

    @Override
    public List<Member> getHierarchyRootMembers(Hierarchy hierarchy) {
        final Role.HierarchyAccess hierarchyAccess = this.role.getAccessDetails(hierarchy);
        final Level[] levels = hierarchy.getLevels();
        final Level firstLevel;
        if (hierarchyAccess == null) {
            firstLevel = levels[0];
        } else {
            firstLevel = levels[hierarchyAccess.getTopLevelDepth()];
        }
        return this.getLevelMembers(firstLevel, true);
    }

    /**
     * This method uses a double-checked locking idiom to avoid making the
     * method fully synchronized, or potentially creating the same MemberReader
     * more than once.  Double-checked locking can cause issues if
     * a second thread accesses the field without either a shared lock in
     * place or the field being specified as volatile.
     * In this case, hierarchyReaders is a ConcurrentHashMap,
     * which internally uses volatile load semantics for read operations.
     * This assures values written by one thread will be visible when read by
     * others.
     * http://en.wikipedia.org/wiki/Double-checked_locking
     */
    @Override
    public MemberReader getMemberReader(Hierarchy hierarchy) {
        MemberReader memberReader = this.hierarchyReaders.get(hierarchy);
        if (memberReader == null) {
            synchronized (this) {
                memberReader = this.hierarchyReaders.get(hierarchy);
                if (memberReader == null) {
                    memberReader = ((RolapHierarchy) hierarchy).createMemberReader(this.role);
                    this.hierarchyReaders.put(hierarchy, memberReader);
                }
            }
        }
        return memberReader;
    }

    @Override
    public Member substitute(Member member) {
        final MemberReader memberReader = this.getMemberReader(member.getHierarchy());
        return memberReader.substitute((RolapMember) member);
    }

    @Override
    public void getMemberRange(Level level, Member startMember, Member endMember, List<Member> list) {
        this
            .getMemberReader(level.getHierarchy())
                .getMemberRange((RolapLevel) level, (RolapMember) startMember, (RolapMember) endMember, Util.<RolapMember> cast(list));
    }

    @Override
    public int compareMembersHierarchically(Member m1, Member m2) {
        final RolapMember member1 = (RolapMember) m1;
        final RolapMember member2 = (RolapMember) m2;
        final RolapHierarchy hierarchy = member1.getHierarchy();
        Util.assertPrecondition(hierarchy == m2.getHierarchy());
        return this.getMemberReader(hierarchy).compare(member1, member2, true);
    }

    @Override
    public Member getMemberParent(Member member) {
        return this.getMemberReader(member.getHierarchy()).getMemberParent((RolapMember) member);
    }

    @Override
    public int getMemberDepth(Member member) {
        final Role.HierarchyAccess hierarchyAccess = this.role.getAccessDetails(member.getHierarchy());
        if (hierarchyAccess != null) {
            final int memberDepth = member.getLevel().getDepth();
            final int topLevelDepth = hierarchyAccess.getTopLevelDepth();
            return memberDepth - topLevelDepth;
        } else if (((RolapLevel) member.getLevel()).isParentChild()) {
            // For members of parent-child hierarchy, members in the same level
            // may have different depths.
            int depth = 0;
            for (Member m = member.getParentMember(); m != null; m = m.getParentMember()) {
                depth++;
            }
            return depth;
        } else {
            return member.getLevel().getDepth();
        }
    }

    @Override
    public List<Member> getMemberChildren(Member member) {
        return this.getMemberChildren(member, null);
    }

    @Override
    public List<Member> getMemberChildren(Member member, Evaluator context) {
        final MemberChildrenConstraint constraint = this.sqlConstraintFactory.getMemberChildrenConstraint(context);
        final List<RolapMember> memberList = this.internalGetMemberChildren(member, constraint);
        return Util.cast(memberList);
    }

    /**
     * Helper for getMemberChildren.
     *
     * @param member Member
     * @param constraint Constraint
     * @return List of children
     */
    private List<RolapMember> internalGetMemberChildren(Member member, MemberChildrenConstraint constraint) {
        final List<RolapMember> children = new ArrayList<RolapMember>();
        final Hierarchy hierarchy = member.getHierarchy();
        final MemberReader memberReader = this.getMemberReader(hierarchy);
        memberReader.getMemberChildren((RolapMember) member, children, constraint);
        return children;
    }

    @Override
    public void getParentChildContributingChildren(Member dataMember, Hierarchy hierarchy, List<Member> list) {
        final List<RolapMember> rolapMemberList = Util.cast(list);
        list.add(dataMember);
        ((RolapHierarchy) hierarchy).getMemberReader().getMemberChildren((RolapMember) dataMember, rolapMemberList);
    }

    @Override
    public int getChildrenCountFromCache(Member member) {
        final Hierarchy hierarchy = member.getHierarchy();
        final MemberReader memberReader = this.getMemberReader(hierarchy);
        if (memberReader instanceof RolapCubeHierarchy.RolapCubeHierarchyMemberReader) {
            final List<mondrian.rolap.RolapMember> list = ((RolapCubeHierarchy.RolapCubeHierarchyMemberReader) memberReader)
                .getRolapCubeMemberCacheHelper()
                    .getChildrenFromCache((RolapMember) member, null);
            if (list == null) {
                return -1;
            }
            return list.size();
        }

        if (memberReader instanceof SmartMemberReader) {
            final List<mondrian.rolap.RolapMember> list = ((SmartMemberReader) memberReader)
                .getMemberCache()
                    .getChildrenFromCache((RolapMember) member, null);
            if (list == null) {
                return -1;
            }
            return list.size();
        }
        if (!(memberReader instanceof MemberCache)) {
            return -1;
        }
        final List<mondrian.rolap.RolapMember> list = ((MemberCache) memberReader).getChildrenFromCache((RolapMember) member, null);
        if (list == null) {
            return -1;
        }
        return list.size();
    }

    /**
     * Returns number of members in a level,
     * if the information can be retrieved from cache.
     * Otherwise {@link Integer#MIN_VALUE}.
     *
     * @param level Level
     * @return number of members in level
     */
    private int getLevelCardinalityFromCache(Level level) {
        final Hierarchy hierarchy = level.getHierarchy();
        final MemberReader memberReader = this.getMemberReader(hierarchy);
        if (memberReader instanceof RolapCubeHierarchy.RolapCubeHierarchyMemberReader) {
            final MemberCacheHelper cache = ((RolapCubeHierarchy.RolapCubeHierarchyMemberReader) memberReader)
                .getRolapCubeMemberCacheHelper();
            if (cache == null) {
                return Integer.MIN_VALUE;
            }
            final List<RolapMember> list = cache.getLevelMembersFromCache((RolapLevel) level, null);
            if (list == null) {
                return Integer.MIN_VALUE;
            }
            return list.size();
        }

        if (memberReader instanceof SmartMemberReader) {
            final List<RolapMember> list = ((SmartMemberReader) memberReader)
                .getMemberCache()
                    .getLevelMembersFromCache((RolapLevel) level, null);
            if (list == null) {
                return Integer.MIN_VALUE;
            }
            return list.size();
        }

        if (memberReader instanceof MemberCache) {
            final List<RolapMember> list = ((MemberCache) memberReader).getLevelMembersFromCache((RolapLevel) level, null);
            if (list == null) {
                return Integer.MIN_VALUE;
            }
            return list.size();
        }

        return Integer.MIN_VALUE;
    }

    @Override
    public int getLevelCardinality(Level level, boolean approximate, boolean materialize) {
        if (!this.role.canAccess(level)) {
            return 1;
        }

        int rowCount = Integer.MIN_VALUE;
        if (approximate) {
            // See if the schema has an approximation.
            rowCount = level.getApproxRowCount();
        }

        if (rowCount == Integer.MIN_VALUE) {
            // See if the precise row count is available in cache.
            rowCount = this.getLevelCardinalityFromCache(level);
        }

        if (rowCount == Integer.MIN_VALUE) {
            if (materialize) {
                // Either the approximate row count hasn't been set,
                // or they want the precise row count.
                final MemberReader memberReader = this.getMemberReader(level.getHierarchy());
                rowCount = memberReader.getLevelMemberCount((RolapLevel) level);
                // Cache it for future.
                ((RolapLevel) level).setApproxRowCount(rowCount);
            }
        }
        return rowCount;
    }

    @Override
    public List<Member> getMemberChildren(List<Member> members) {
        return this.getMemberChildren(members, null);
    }

    @Override
    public List<Member> getMemberChildren(List<Member> members, Evaluator context) {
        if (members.size() == 0) {
            return Collections.emptyList();
        } else {
            final MemberChildrenConstraint constraint = this.sqlConstraintFactory.getMemberChildrenConstraint(context);
            final Hierarchy hierarchy = members.get(0).getHierarchy();
            final MemberReader memberReader = this.getMemberReader(hierarchy);
            final List<RolapMember> rolapMemberList = Util.cast(members);
            final List<RolapMember> children = new ArrayList<RolapMember>();
            memberReader.getMemberChildren(rolapMemberList, children, constraint);
            return Util.cast(children);
        }
    }

    @Override
    public void getMemberAncestors(Member member, List<Member> ancestorList) {
        Member parentMember = this.getMemberParent(member);
        while (parentMember != null) {
            ancestorList.add(parentMember);
            parentMember = this.getMemberParent(parentMember);
        }
    }

    @Override
    public Cube getCube() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SchemaReader withoutAccessControl() {
        assert this.getClass() == RolapSchemaReader.class : "Subclass " + this.getClass() + " must override";
        if (this.role == this.schema.getDefaultRole()) {
            return this;
        }
        return new RolapSchemaReader(this.schema.getDefaultRole(), this.schema);
    }

    @Override
    public OlapElement getElementChild(OlapElement parent, Id.Segment name) {
        return this.getElementChild(parent, name, MatchType.EXACT);
    }

    @Override
    public OlapElement getElementChild(OlapElement parent, Id.Segment name, MatchType matchType) {
        return parent.lookupChild(this, name, matchType);
    }

    @Override
    public final Member getMemberByUniqueName(List<Id.Segment> uniqueNameParts, boolean failIfNotFound) {
        return this.getMemberByUniqueName(uniqueNameParts, failIfNotFound, MatchType.EXACT);
    }

    @Override
    public Member getMemberByUniqueName(List<Id.Segment> uniqueNameParts, boolean failIfNotFound, MatchType matchType) {
        // In general, this schema reader doesn't have a cube, so we cannot
        // start looking up members.
        return null;
    }

    @Override
    public OlapElement lookupCompound(OlapElement parent, List<Id.Segment> names, boolean failIfNotFound, int category) {
        return this.lookupCompound(parent, names, failIfNotFound, category, MatchType.EXACT);
    }

    @Override
    public final OlapElement lookupCompound(OlapElement parent,
        List<Id.Segment> names,
        boolean failIfNotFound,
        int category,
        MatchType matchType) {
        if (MondrianProperties.instance().SsasCompatibleNaming.get()) {
            return new NameResolver().resolve(parent, Util.toOlap4j(names), failIfNotFound, category, matchType, this.getNamespaces());
        }
        return this.lookupCompoundInternal(parent, names, failIfNotFound, category, matchType);
    }

    public final OlapElement lookupCompoundInternal(OlapElement parent,
        List<Id.Segment> names,
        boolean failIfNotFound,
        int category,
        MatchType matchType) {
        return Util.lookupCompound(this, parent, names, failIfNotFound, category, matchType);
    }

    @Override
    public List<NameResolver.Namespace> getNamespaces() {
        return Collections.<NameResolver.Namespace> singletonList(this);
    }

    @Override
    public OlapElement lookupChild(OlapElement parent, IdentifierSegment segment) {
        return this.lookupChild(parent, segment, MatchType.EXACT);
    }

    @Override
    public OlapElement lookupChild(OlapElement parent, IdentifierSegment segment, MatchType matchType) {
        OlapElement element = this.getElementChild(parent, Util.convert(segment), matchType);
        if (element != null) {
            return element;
        }
        if (parent instanceof Cube) {
            // Named sets defined at the schema level do not, of course, belong
            // to a cube. But if parent is a cube, this indicates that the name
            // has not been qualified.
            element = this.schema.getNamedSet(segment);
        }
        return element;
    }

    @Override
    public Member lookupMemberChildByName(Member parent, Id.Segment childName, MatchType matchType) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("looking for child \"" + childName + "\" of " + parent);
        }
        assert !(parent instanceof RolapHierarchy.LimitedRollupMember);
        try {
            MemberChildrenConstraint constraint;
            if ((childName instanceof Id.NameSegment) && matchType.isExact()) {
                constraint = this.sqlConstraintFactory.getChildByNameConstraint((RolapMember) parent, (Id.NameSegment) childName);
            } else {
                constraint = this.sqlConstraintFactory.getMemberChildrenConstraint(null);
            }
            final List<RolapMember> children = this.internalGetMemberChildren(parent, constraint);
            if (children.size() > 0) {
                return RolapUtil.findBestMemberMatch(children, (RolapMember) parent, children.get(0).getLevel(), childName, matchType);
            }
        } catch (final NumberFormatException e) {
            // this was thrown in SqlQuery#quote(boolean numeric, Object
            // value). This happens when Mondrian searches for unqualified Olap
            // Elements like [Month], because it tries to look up a member with
            // that name in all dimensions. Then it generates for example
            // "select .. from time where year = Month" which will result in a
            // NFE because "Month" can not be parsed as a number. The real bug
            // is probably, that Mondrian looks at members at all.
            //
            // @see RolapCube#lookupChild()
            LOGGER
                .debug("NumberFormatException in lookupMemberChildByName " + "for parent = \""
                    + parent
                    + "\", childName=\""
                    + childName
                    + "\", exception: "
                    + e.getMessage());
        }
        return null;
    }

    @Override
    public List<Member> lookupMemberChildrenByNames(Member parent, List<Id.NameSegment> childNames, MatchType matchType) {
        final MemberChildrenConstraint constraint = this.sqlConstraintFactory
            .getChildrenByNamesConstraint((RolapMember) parent, childNames);
        final List<RolapMember> children = this.internalGetMemberChildren(parent, constraint);
        final List<Member> childMembers = new ArrayList<Member>();
        childMembers.addAll(children);
        return childMembers;
    }

    @Override
    public Member getCalculatedMember(List<Id.Segment> nameParts) {
        // There are no calculated members defined against a schema.
        return null;
    }

    @Override
    public NamedSet getNamedSet(List<Id.Segment> nameParts) {
        if (nameParts.size() != 1) {
            return null;
        }
        if (!(nameParts.get(0) instanceof Id.NameSegment)) {
            return null;
        }
        final String name = ((Id.NameSegment) nameParts.get(0)).name;
        return this.schema.getNamedSet(name);
    }

    @Override
    public Member getLeadMember(Member member, int n) {
        final MemberReader memberReader = this.getMemberReader(member.getHierarchy());
        return memberReader.getLeadMember((RolapMember) member, n);
    }

    @Override
    public List<Member> getLevelMembers(Level level, boolean includeCalculated) {
        List<Member> members = this.getLevelMembers(level, null);
        if (!includeCalculated) {
            members = SqlConstraintUtils.removeCalculatedMembers(members);
        }
        return members;
    }

    @Override
    public List<Member> getLevelMembers(Level level, Evaluator context) {
        final TupleConstraint constraint = this.sqlConstraintFactory.getLevelMembersConstraint(context, new Level[] { level });
        final MemberReader memberReader = this.getMemberReader(level.getHierarchy());
        final List<RolapMember> membersInLevel = memberReader.getMembersInLevel((RolapLevel) level, constraint);
        return Util.cast(membersInLevel);
    }

    @Override
    public List<Dimension> getCubeDimensions(Cube cube) {
        assert cube != null;
        final List<Dimension> dimensions = new ArrayList<Dimension>();
        for (final Dimension dimension : cube.getDimensions()) {
            switch (this.role.getAccess(dimension)) {
                case NONE:
                    continue;
                default:
                    dimensions.add(dimension);
                    break;
            }
        }
        return dimensions;
    }

    @Override
    public List<Hierarchy> getDimensionHierarchies(Dimension dimension) {
        assert dimension != null;
        final List<Hierarchy> hierarchies = new ArrayList<Hierarchy>();
        for (final Hierarchy hierarchy : dimension.getHierarchies()) {
            switch (this.role.getAccess(hierarchy)) {
                case NONE:
                    continue;
                default:
                    hierarchies.add(hierarchy);
                    break;
            }
        }
        return hierarchies;
    }

    @Override
    public List<Level> getHierarchyLevels(Hierarchy hierarchy) {
        assert hierarchy != null;
        final Role.HierarchyAccess hierarchyAccess = this.role.getAccessDetails(hierarchy);
        final Level[] levels = hierarchy.getLevels();
        if (hierarchyAccess == null) {
            return Arrays.asList(levels);
        }
        final Level topLevel = levels[hierarchyAccess.getTopLevelDepth()];
        final Level bottomLevel = levels[hierarchyAccess.getBottomLevelDepth()];
        final List<Level> restrictedLevels = Arrays.asList(levels).subList(topLevel.getDepth(), bottomLevel.getDepth() + 1);
        assert restrictedLevels.size() >= 1 : "postcondition";
        return restrictedLevels;
    }

    @Override
    public Member getHierarchyDefaultMember(Hierarchy hierarchy) {
        assert hierarchy != null;
        // If the whole hierarchy is inaccessible, return the intrinsic default
        // member. This is important to construct a evaluator.
        if (this.role.getAccess(hierarchy) == Access.NONE) {
            return hierarchy.getDefaultMember();
        }
        return this.getMemberReader(hierarchy).getDefaultMember();
    }

    @Override
    public boolean isDrillable(Member member) {
        final RolapLevel level = (RolapLevel) member.getLevel();
        if (level.getParentExp() != null) {
            // This is a parent-child level, so its children, if any, come from
            // the same level.
            //
            // todo: More efficient implementation
            return this.getMemberChildren(member).size() > 0;
        } else {
            // This is a regular level. It has children iff there is a lower
            // level.
            final Level childLevel = level.getChildLevel();
            return (childLevel != null) && (this.role.getAccess(childLevel) != Access.NONE);
        }
    }

    @Override
    public boolean isVisible(Member member) {
        return !member.isHidden() && this.role.canAccess(member);
    }

    @Override
    public Cube[] getCubes() {
        final List<RolapCube> cubes = this.schema.getCubeList();
        final List<Cube> visibleCubes = new ArrayList<Cube>(cubes.size());

        for (final Cube cube : cubes) {
            if (this.role.canAccess(cube)) {
                visibleCubes.add(cube);
            }
        }

        return visibleCubes.toArray(new Cube[visibleCubes.size()]);
    }

    @Override
    public List<Member> getCalculatedMembers(Hierarchy hierarchy) {
        return Collections.emptyList();
    }

    @Override
    public List<Member> getCalculatedMembers(Level level) {
        return Collections.emptyList();
    }

    @Override
    public List<Member> getCalculatedMembers() {
        return Collections.emptyList();
    }

    @Override
    public NativeEvaluator getNativeSetEvaluator(FunDef fun, Exp[] args, Evaluator evaluator, Calc calc) {
        final RolapEvaluator revaluator = (RolapEvaluator) AbstractCalc.simplifyEvaluator(calc, evaluator);
        if (evaluator.nativeEnabled()) {
            return this.schema.getNativeRegistry().createEvaluator(revaluator, fun, args);
        }
        return null;
    }

    @Override
    public Parameter getParameter(String name) {
        // Scan through schema parameters.
        for (final RolapSchemaParameter parameter : this.schema.parameterList) {
            if (Util.equalName(parameter.getName(), name)) {
                return parameter;
            }
        }

        // Scan through mondrian properties.
        final List<Property> propertyList = MondrianProperties.instance().getPropertyList();
        for (final Property property : propertyList) {
            if (property.getPath().equals(name)) {
                return new SystemPropertyParameter(name, false);
            }
        }

        return null;
    }

    @Override
    public DataSource getDataSource() {
        return this.schema.getInternalConnection().getDataSource();
    }

    @Override
    public RolapSchema getSchema() {
        return this.schema;
    }

    @Override
    public SchemaReader withLocus() {
        return RolapUtil.locusSchemaReader(this.schema.getInternalConnection(), this);
    }

    @Override
    public Map<? extends Member, Access> getMemberChildrenWithDetails(Member member, Evaluator evaluator) {
        final MemberChildrenConstraint constraint = this.sqlConstraintFactory.getMemberChildrenConstraint(evaluator);
        final Hierarchy hierarchy = member.getHierarchy();
        final MemberReader memberReader = this.getMemberReader(hierarchy);
        final ArrayList<RolapMember> memberChildren = new ArrayList<RolapMember>();

        return memberReader.getMemberChildren((RolapMember) member, memberChildren, constraint);
    }

    /**
     * Implementation of {@link Parameter} which is sourced from mondrian
     * properties (see {@link MondrianProperties}.
     * <p/>
     * <p>The name of the property is the same as the key into the
     * {@link java.util.Properties} object; for example "mondrian.trace.level".
     */
    private static class SystemPropertyParameter extends ParameterImpl {
        /**
         * true if source is a system property;
         * false if source is a mondrian property.
         */
        private final boolean system;
        /**
         * Definition of mondrian property, or null if system property.
         */
        private final Property propertyDefinition;

        public SystemPropertyParameter(String name, boolean system) {
            super(name, Literal.nullValue, "System property '" + name + "'", new StringType());
            this.system = system;
            this.propertyDefinition = system ? null : MondrianProperties.instance().getPropertyDefinition(name);
        }

        @Override
        public Scope getScope() {
            return Scope.System;
        }

        @Override
        public boolean isModifiable() {
            return false;
        }

        @Override
        public Calc compile(ExpCompiler compiler) {
            return new GenericCalc(new DummyExp(this.getType())) {
                @Override
                public Calc[] getCalcs() {
                    return new Calc[0];
                }

                @Override
                public Object evaluate(Evaluator evaluator) {
                    if (SystemPropertyParameter.this.system) {
                        final String name = SystemPropertyParameter.this.getName();
                        return System.getProperty(name);
                    } else {
                        return SystemPropertyParameter.this.propertyDefinition.stringValue();
                    }
                }
            };
        }
    }
}

// End RolapSchemaReader.java
