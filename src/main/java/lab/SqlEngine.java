package lab;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * In-memory deterministic, single-statement SQL subset (keywords ignore case):
 * CREATE TABLE name (column LONG|TEXT [PRIMARY KEY], ...)
 * INSERT INTO name VALUES (literal, ...)
 * SELECT *|column[, ...] FROM name [WHERE primary_key = literal]
 * UPDATE name SET non_key_column = literal WHERE primary_key = literal
 * UPDATE name SET long_column = long_column + integer WHERE primary_key = literal
 * DELETE FROM name WHERE primary_key = literal
 *
 * Exactly one inline primary key is required. Identifiers are ASCII letters or
 * underscores followed by letters, digits or underscores, and ignore case.
 * Literals are signed 64-bit decimal integers or single-quoted text (double a
 * quote to escape it). One trailing semicolon is optional. No NULL, comments,
 * joins, transactions, column-list INSERT, expressions beyond increment, or DDL
 * beyond CREATE. Statements are at most 65,536 UTF-16 characters.
 *
 * Writes return OK CREATE / OK INSERT 1 / OK UPDATE n / OK DELETE n. Reads return
 * OK SELECT followed by JSON {columns:[...],rows:[[...],...]}, with quoted JSON
 * keys and rows sorted by primary key (numeric LONG or Java String TEXT order).
 * Errors return ERR followed by a stable explanation and never mutate state.
 * This class has no persistence; replay the committed log into a fresh instance.
 */
public final class SqlEngine {
    private enum Type { LONG, TEXT }
    private record Column(String name, Type type) {}
    private record Token(String value, boolean quoted) {}
    private static final class SqlError extends RuntimeException {
        SqlError(String message) { super(message); }
    }
    private static final class Table {
        final List<Column> columns;
        final int key;
        final Map<Object, List<Object>> rows;

        Table(List<Column> columns, int key) {
            this.columns = List.copyOf(columns);
            this.key = key;
            rows = new TreeMap<>((a, b) -> a instanceof Long
                    ? Long.compare((Long) a, (Long) b) : ((String) a).compareTo((String) b));
        }

        int column(String name) {
            for (int i = 0; i < columns.size(); i++) {
                if (columns.get(i).name().equals(name)) return i;
            }
            throw new SqlError("unknown column " + name);
        }
    }

    private final Map<String, Table> tables = new LinkedHashMap<>();

    public SqlEngine() {}

    public synchronized String execute(String sql) {
        try {
            Parser p = new Parser(sql);
            if (p.take("CREATE")) return create(p);
            if (p.take("INSERT")) return insert(p);
            if (p.take("SELECT")) return select(p);
            if (p.take("UPDATE")) return update(p);
            if (p.take("DELETE")) return delete(p);
            throw new SqlError("unsupported statement");
        } catch (SqlError e) {
            return "ERR " + e.getMessage();
        } catch (NumberFormatException e) {
            return "ERR integer out of range";
        } catch (ArithmeticException e) {
            return "ERR integer overflow";
        }
    }

    private String create(Parser p) {
        p.require("TABLE");
        String name = p.identifier();
        if (tables.containsKey(name)) throw new SqlError("table already exists " + name);
        p.require("(");
        List<Column> columns = new ArrayList<>();
        int key = -1;
        do {
            String column = p.identifier();
            if (columns.stream().anyMatch(c -> c.name().equals(column))) {
                throw new SqlError("duplicate column " + column);
            }
            Type type;
            if (p.take("LONG")) type = Type.LONG;
            else if (p.take("TEXT")) type = Type.TEXT;
            else throw new SqlError("expected LONG or TEXT");
            if (p.take("PRIMARY")) {
                p.require("KEY");
                if (key != -1) throw new SqlError("multiple primary keys");
                key = columns.size();
            }
            columns.add(new Column(column, type));
        } while (p.take(","));
        p.require(")");
        p.end();
        if (key == -1) throw new SqlError("primary key required");
        tables.put(name, new Table(columns, key));
        return "OK CREATE";
    }

    private Table table(Parser p) {
        String name = p.identifier();
        Table table = tables.get(name);
        if (table == null) throw new SqlError("unknown table " + name);
        return table;
    }

    private String insert(Parser p) {
        p.require("INTO");
        Table table = table(p);
        p.require("VALUES");
        p.require("(");
        List<Object> row = new ArrayList<>();
        for (int i = 0; i < table.columns.size(); i++) {
            if (i > 0) p.require(",");
            row.add(p.literal(table.columns.get(i).type()));
        }
        p.require(")");
        p.end();
        Object key = row.get(table.key);
        if (table.rows.containsKey(key)) throw new SqlError("duplicate primary key");
        table.rows.put(key, row);
        return "OK INSERT 1";
    }

    private Object where(Parser p, Table table) {
        p.require("WHERE");
        if (table.column(p.identifier()) != table.key) {
            throw new SqlError("WHERE requires primary key");
        }
        p.require("=");
        return p.literal(table.columns.get(table.key).type());
    }

    private String select(Parser p) {
        List<String> names = new ArrayList<>();
        boolean star = p.take("*");
        if (!star) {
            do { names.add(p.identifier()); } while (p.take(","));
        }
        p.require("FROM");
        Table table = table(p);
        List<Integer> selected = new ArrayList<>();
        if (star) {
            for (int i = 0; i < table.columns.size(); i++) selected.add(i);
        } else {
            for (String name : names) selected.add(table.column(name));
        }
        Object key = p.is("WHERE") ? where(p, table) : null;
        p.end();
        List<List<Object>> rows = new ArrayList<>();
        if (key == null) rows.addAll(table.rows.values());
        else if (table.rows.containsKey(key)) rows.add(table.rows.get(key));
        StringBuilder out = new StringBuilder("OK SELECT {\"columns\":[");
        for (int i = 0; i < selected.size(); i++) {
            if (i > 0) out.append(',');
            out.append(json(table.columns.get(selected.get(i)).name()));
        }
        out.append("],\"rows\":[");
        for (int r = 0; r < rows.size(); r++) {
            if (r > 0) out.append(',');
            out.append('[');
            for (int c = 0; c < selected.size(); c++) {
                if (c > 0) out.append(',');
                out.append(json(rows.get(r).get(selected.get(c))));
            }
            out.append(']');
        }
        return out.append("]}").toString();
    }

    private String update(Parser p) {
        Table table = table(p);
        p.require("SET");
        int column = table.column(p.identifier());
        if (column == table.key) throw new SqlError("cannot update primary key");
        p.require("=");
        Type type = table.columns.get(column).type();
        boolean increment = p.is(table.columns.get(column).name());
        Object value;
        if (increment) {
            p.identifier();
            p.require("+");
            if (type != Type.LONG) throw new SqlError("increment requires LONG");
            value = p.literal(Type.LONG);
        } else value = p.literal(type);
        Object key = where(p, table);
        p.end();
        List<Object> row = table.rows.get(key);
        if (row == null) return "OK UPDATE 0";
        Object replacement = increment ? Math.addExact((Long) row.get(column), (Long) value) : value;
        row.set(column, replacement);
        return "OK UPDATE 1";
    }

    private String delete(Parser p) {
        p.require("FROM");
        Table table = table(p);
        Object key = where(p, table);
        p.end();
        return "OK DELETE " + (table.rows.remove(key) == null ? "0" : "1");
    }

    private static String json(Object value) {
        if (value instanceof Long) return value.toString();
        String text = (String) value;
        StringBuilder out = new StringBuilder("\"");
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '"' || c == '\\') out.append('\\').append(c);
            else if (c < 32 || Character.isSurrogate(c)) {
                out.append("\\u");
                String hex = Integer.toHexString(c);
                out.append("0".repeat(4 - hex.length())).append(hex);
            } else out.append(c);
        }
        return out.append('"').toString();
    }

    private static final class Parser {
        private final List<Token> tokens = new ArrayList<>();
        private int position;

        Parser(String sql) {
            if (sql == null || sql.length() > 65_536) throw new SqlError("invalid statement size");
            for (int i = 0; i < sql.length();) {
                char c = sql.charAt(i);
                if (Character.isWhitespace(c)) { i++; continue; }
                if (c == '\'') {
                    StringBuilder value = new StringBuilder();
                    boolean closed = false;
                    i++;
                    while (i < sql.length()) {
                        c = sql.charAt(i++);
                        if (c == '\'') {
                            if (i < sql.length() && sql.charAt(i) == '\'') { value.append(c); i++; }
                            else { closed = true; break; }
                        } else value.append(c);
                    }
                    if (!closed) throw new SqlError("unterminated string");
                    tokens.add(new Token(value.toString(), true));
                } else if ("(),=*;+".indexOf(c) >= 0) {
                    tokens.add(new Token(String.valueOf(c), false));
                    i++;
                } else {
                    int start = i++;
                    while (i < sql.length() && !Character.isWhitespace(sql.charAt(i))
                            && "(),=*;+'".indexOf(sql.charAt(i)) < 0) i++;
                    tokens.add(new Token(sql.substring(start, i), false));
                }
            }
        }

        boolean is(String expected) {
            return position < tokens.size() && !tokens.get(position).quoted()
                    && tokens.get(position).value().equalsIgnoreCase(expected);
        }

        boolean take(String expected) {
            if (!is(expected)) return false;
            position++;
            return true;
        }

        void require(String expected) {
            if (!take(expected)) throw new SqlError("expected " + expected);
        }

        Token next() {
            if (position == tokens.size()) throw new SqlError("unexpected end of statement");
            return tokens.get(position++);
        }

        String identifier() {
            Token token = next();
            if (token.quoted() || !token.value().matches("[A-Za-z_][A-Za-z_0-9]*")) {
                throw new SqlError("expected identifier");
            }
            return token.value().toLowerCase(Locale.ROOT);
        }

        Object literal(Type type) {
            boolean positive = take("+");
            Token token = next();
            if (type == Type.TEXT && token.quoted() && !positive) return token.value();
            if (type == Type.LONG && !token.quoted() && token.value().matches("-?[0-9]+")
                    && !(positive && token.value().startsWith("-"))) {
                return Long.parseLong(token.value());
            }
            throw new SqlError("expected " + type + " literal");
        }

        void end() {
            take(";");
            if (position != tokens.size()) throw new SqlError("unexpected trailing input");
        }
    }
}
