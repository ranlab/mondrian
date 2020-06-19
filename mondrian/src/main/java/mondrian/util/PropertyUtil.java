/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

/**
 * Utilities to generate MondrianProperties.java and mondrian.properties
 * from property definitions in MondrianProperties.xml.
 *
 * @author jhyde
 */
public class PropertyUtil {

    private static Iterable<org.w3c.dom.Node> iter(final org.w3c.dom.NodeList nodeList) {
        return new Iterable<org.w3c.dom.Node>() {
            @Override
            public Iterator<org.w3c.dom.Node> iterator() {
                return new Iterator<org.w3c.dom.Node>() {
                    int pos = 0;

                    @Override
                    public boolean hasNext() {
                        return this.pos < nodeList.getLength();
                    }

                    @Override
                    public org.w3c.dom.Node next() {
                        return nodeList.item(this.pos++);
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    /**
     * Generates MondrianProperties.java from MondrianProperties.xml.
     *
     * @param args Arguments
     */
    public static void main(String[] args) {
        try {
            new mondrian.util.PropertyUtil().generate(args);
        } catch (final Throwable e) {
            System.out.println("Error while generating properties files.");
            e.printStackTrace();
        }
    }

    private void generate(String[] args) {
        final String olapDir = args.length > 0 ? args[0] : "src/main/java/mondrian/olap";
        final String outputDir = args.length > 1 ? args[1] : "src/generated/java/mondrian/olap";
        final File xmlFile = new File(olapDir, "MondrianProperties.xml");
        final File javaFile = new File(outputDir, "MondrianProperties.java");
        final File propertiesFile = new File("target/mondrian.properties.template");
        final File htmlFile = new File("target/site/doc", "properties.html");

        SortedMap<String, PropertyDef> propertyDefinitionMap;
        try {
            final DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            dbf.setValidating(false);
            dbf.setExpandEntityReferences(false);
            final DocumentBuilder db = dbf.newDocumentBuilder();
            final org.w3c.dom.Document doc = db.parse(xmlFile);
            final org.w3c.dom.Element documentElement = doc.getDocumentElement();
            assert documentElement.getNodeName().equals("PropertyDefinitions");
            final org.w3c.dom.NodeList propertyDefinitions = documentElement.getChildNodes();
            propertyDefinitionMap = new TreeMap<String, PropertyDef>();
            for (final org.w3c.dom.Node element : iter(propertyDefinitions)) {
                if (element.getNodeName().equals("PropertyDefinition")) {
                    final String name = getChildCdata(element, "Name");
                    final String dflt = getChildCdata(element, "Default");
                    final String type = getChildCdata(element, "Type");
                    final String path = getChildCdata(element, "Path");
                    final String category = getChildCdata(element, "Category");
                    final String core = getChildCdata(element, "Core");
                    final String description = getChildCdata(element, "Description");
                    propertyDefinitionMap
                        .put(name,
                            new PropertyDef(name, path, dflt, category, PropertyType.valueOf(type.toUpperCase()),
                                (core == null) || Boolean.valueOf(core), description));
                }
            }
        } catch (final Throwable e) {
            throw new RuntimeException("Error while parsing " + xmlFile, e);
        }
        this.doGenerate(Generator.JAVA, propertyDefinitionMap, javaFile);
        this.doGenerate(Generator.HTML, propertyDefinitionMap, htmlFile);
        this.doGenerate(Generator.PROPERTIES, propertyDefinitionMap, propertiesFile);
    }

    void doGenerate(Generator generator, SortedMap<String, PropertyDef> propertyDefinitionMap, File file) {
        FileWriter fw = null;
        PrintWriter out = null;
        boolean success = false;
        try {
            System.out.println("Generating " + file);
            if (file.getParentFile() != null) {
                file.getParentFile().mkdirs();
            }
            fw = new FileWriter(file);
            out = new PrintWriter(fw);
            generator.generate(propertyDefinitionMap, file, out);
            out.close();
            fw.close();
            success = true;
        } catch (final Throwable e) {
            throw new RuntimeException("Error while generating " + file, e);
        } finally {
            if (out != null) {
                out.close();
            }
            if (fw != null) {
                try {
                    fw.close();
                } catch (final IOException e) {
                    // ignore
                }
            }
            if (!success) {
                file.delete();
            }
        }
    }

    private static final void printLines(PrintWriter out, String[] lines) {
        for (final String line : lines) {
            out.println(line);
        }
    }

    enum Generator {
        JAVA {
            @Override
            void generate(SortedMap<String, PropertyDef> propertyDefinitionMap, File file, PrintWriter out) {
                out.println("// Generated from MondrianProperties.xml.");
                out.println("package mondrian.olap;");
                out.println();
                out.println("import org.eigenbase.util.property.*;");
                out.println("import java.io.File;");
                out.println();

                printJavadoc(out,
                    "",
                    "Configuration properties that determine the\n" + "behavior of a mondrian instance.\n"
                        + "\n"
                        + "<p>There is a method for property valid in a\n"
                        + "<code>mondrian.properties</code> file. Although it is possible to retrieve\n"
                        + "properties using the inherited {@link java.util.Properties#getProperty(String)}\n"
                        + "method, we recommend that you use methods in this class.</p>\n");
                final String[] lines = { "public class MondrianProperties extends MondrianPropertiesBase {",
                        "    /**",
                        "     * Properties, drawn from {@link System#getProperties},",
                        "     * plus the contents of \"mondrian.properties\" if it",
                        "     * exists. A singleton.",
                        "     */",
                        "    private static final MondrianProperties instance =",
                        "        new MondrianProperties();",
                        "",
                        "    private MondrianProperties() {",
                        "        super(",
                        "            new FilePropertySource(",
                        "                new File(mondrianDotProperties)));",
                        "        populate();",
                        "    }",
                        "",
                        "    /**",
                        "     * Returns the singleton.",
                        "     *",
                        "     * @return Singleton instance",
                        "     */",
                        "    public static MondrianProperties instance() {",
                        "        // NOTE: We used to instantiate on demand, but",
                        "        // synchronization overhead was significant. See",
                        "        // MONDRIAN-978.",
                        "        return instance;",
                        "    }",
                        "", };
                printLines(out, lines);
                for (final PropertyDef def : propertyDefinitionMap.values()) {
                    if (!def.core) {
                        continue;
                    }
                    printJavadoc(out, "    ", def.description);
                    out.println("    public transient final " + def.propertyType.className + " " + def.name + " =");
                    out.println("        new " + def.propertyType.className + "(");
                    out.println("            this, \"" + def.path + "\", " + "" + def.defaultJava() + ");");
                    out.println();
                }
                out.println("}");
                out.println();
                out.println("// End MondrianProperties.java");
            }
        },

        HTML {
            @Override
            void generate(SortedMap<String, PropertyDef> propertyDefinitionMap, File file, PrintWriter out) {
                out.println("<table>");
                out.println("    <tr>");
                out.println("    <td><strong>Property</strong></td>");
                out.println("    <td><strong>Type</strong></td>");
                out.println("    <td><strong>Default value</strong></td>");
                out.println("    <td><strong>Description</strong></td>");
                out.println("    </tr>");

                final SortedSet<String> categories = new TreeSet<String>();
                for (final PropertyDef def : propertyDefinitionMap.values()) {
                    categories.add(def.category);
                }
                for (final String category : categories) {
                    out.println("    <tr>");
                    out.println("      <td colspan='4'><b><br>" + category + "</b" + "></td>");
                    out.println("    </tr>");
                    for (final PropertyDef def : propertyDefinitionMap.values()) {
                        if (!def.category.equals(category)) {
                            continue;
                        }
                        out.println("    <tr>");
                        out
                            .println("<td><code><a href='api/mondrian/olap/MondrianProperties.html#" + def.name
                                + "'>"
                                + this.split(def.path)
                                + "</a></code></td>");
                        out.println("<td>" + def.propertyType.name().toLowerCase() + "</td>");
                        out.println("<td>" + this.split(def.defaultHtml()) + "</td>");
                        out.println("<td>" + this.split(def.description) + "</td>");
                        out.println("    </tr>");
                    }
                }
                out.println("<table>");
            }

            String split(String s) {
                s = s.replaceAll("([,;=.])", "&shy;$1&shy;");
                if (!s.contains("<")) {
                    s = s.replaceAll("(/)", "&shy;$1&shy;");
                }
                return s;
            }
        },

        PROPERTIES {
            @Override
            void generate(SortedMap<String, PropertyDef> propertyDefinitionMap, File file, PrintWriter out) {
                printComments(out,
                    "",
                    "#",
                    wrapText("This software is subject to the terms of the Eclipse Public License v1.0\n"
                        + "Agreement, available at the following URL:\n"
                        + "http://www.eclipse.org/legal/epl-v10.html.\n"
                        + "You must accept the terms of that agreement to use this software.\n"
                        + "\n"
                        + "Copyright (C) 2001-2005 Julian Hyde\n"
                        + "Copyright (C) 2005-2011 Pentaho and others\n"
                        + "All Rights Reserved."));
                out.println();

                final char[] chars = new char[79];
                Arrays.fill(chars, '#');
                final String commentLine = new String(chars);
                for (final PropertyDef def : propertyDefinitionMap.values()) {
                    out.println(commentLine);
                    printComments(out, "", "#", wrapText(stripHtml(def.description)));
                    out.println("#");
                    out.println("#" + def.path + "=" + (def.defaultValue == null ? "" : def.defaultValue));
                    out.println();
                }
                printComments(out, "", "#", wrapText("End " + file.getName()));
            }
        };

        abstract void generate(SortedMap<String, PropertyDef> propertyDefinitionMap, File file, PrintWriter out);
    }

    private static void printJavadoc(PrintWriter out, String prefix, String content) {
        out.println(prefix + "/**");
        printComments(out, prefix, " *", wrapText(content));
        out.println(prefix + " */");
    }

    private static void printComments(PrintWriter out, String offset, String prefix, List<String> strings) {
        for (final String line : strings) {
            if (line.length() > 0) {
                out.println(offset + prefix + " " + line);
            } else {
                out.println(offset + prefix);
            }
        }
    }

    private static String quoteHtml(String s) {
        return s.replaceAll("&", "&amp;").replaceAll(">", "&gt;").replaceAll("<", "&lt;");
    }

    private static String stripHtml(String s) {
        s = s.replaceAll("<li>", "<li>* ");
        s = s.replaceAll("<h3>", "<h3>### ");
        s = s.replaceAll("</h3>", " ###</h3>");
        final String[] strings = { "p", "code", "br", "ul", "li", "blockquote", "h3", "i" };
        for (final String string : strings) {
            s = s.replaceAll("<" + string + "/>", "");
            s = s.replaceAll("<" + string + ">", "");
            s = s.replaceAll("</" + string + ">", "");
        }
        s = replaceRegion(s, "{@code ", "}");
        s = replaceRegion(s, "{@link ", "}");
        s = s.replaceAll("&amp;", "&");
        s = s.replaceAll("&lt;", "<");
        s = s.replaceAll("&gt;", ">");
        return s;
    }

    private static String replaceRegion(String s, String start, String end) {
        int i = 0;
        while ((i = s.indexOf(start, i)) >= 0) {
            final int j = s.indexOf(end, i);
            if (j < 0) {
                break;
            }
            s = s.substring(0, i) + s.substring(i + start.length(), j) + s.substring(j + 1);
            i = j - start.length() - end.length();
        }
        return s;
    }

    private static List<String> wrapText(String description) {
        description = description.trim();
        return Arrays.asList(description.split("\n"));
    }

    private static String getChildCdata(org.w3c.dom.Node element, String name) {
        for (final org.w3c.dom.Node node : iter(element.getChildNodes())) {
            if (node.getNodeName().equals(name)) {
                final StringBuilder buf = new StringBuilder();
                textRecurse(node, buf);
                return buf.toString();
            }
        }
        return null;
    }

    private static void textRecurse(org.w3c.dom.Node node, StringBuilder buf) {
        for (final org.w3c.dom.Node node1 : iter(node.getChildNodes())) {
            if ((node1.getNodeType() == org.w3c.dom.Node.CDATA_SECTION_NODE) || (node1.getNodeType() == org.w3c.dom.Node.TEXT_NODE)) {
                buf.append(quoteHtml(node1.getNodeValue()));
            }
            if (node1.getNodeType() == org.w3c.dom.Node.ELEMENT_NODE) {
                buf.append("<").append(node1.getNodeName()).append(">");
                textRecurse(node1, buf);
                buf.append("</").append(node1.getNodeName()).append(">");
            }
        }
    }

    private static class PropertyDef {
        private final String name;
        private final String defaultValue;
        private final String category;
        private final PropertyType propertyType;
        private final boolean core;
        private final String description;
        private final String path;

        PropertyDef(String name, String path, String defaultValue, String category, PropertyType propertyType, boolean core,
            String description) {
            this.name = name;
            this.path = path;
            this.defaultValue = defaultValue;
            this.category = category == null ? "Miscellaneous" : category;
            this.propertyType = propertyType;
            this.core = core;
            this.description = description;
        }

        public String defaultJava() {
            switch (this.propertyType) {
                case STRING:
                    if (this.defaultValue == null) {
                        return "null";
                    } else {
                        return "\"" + this.defaultValue.replaceAll("\"", "\\\"") + "\"";
                    }
                default:
                    return this.defaultValue;
            }
        }

        public String defaultHtml() {
            if (this.defaultValue == null) {
                return "-";
            }
            switch (this.propertyType) {
                case INT:
                case DOUBLE:
                    return new DecimalFormat("#,###.#").format(new BigDecimal(this.defaultValue));
                default:
                    return this.defaultValue;
            }
        }
    }

    private enum PropertyType {
        INT("IntegerProperty"), STRING("StringProperty"), DOUBLE("DoubleProperty"), BOOLEAN("BooleanProperty");

        public final String className;

        PropertyType(String className) {
            this.className = className;
        }
    }
}

// End PropertyUtil.java
