package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.Before;
import org.junit.Test;

import cn.edu.thssdb.parser.*;

import java.util.ArrayList;

import static org.junit.Assert.*;

public class QueryTest {
    private SQLLexer lexer;
    private Manager manager;
    private Database db;
    private ArrayList<CharStream> input;

    @Before
    public void init() {
        input = new ArrayList<>();
        input.add(CharStreams.fromString("CREATE TABLE person (name String(256), ID Int not null, PRIMARY KEY(ID));"));
        input.add(CharStreams.fromString("insert into person (name, ID) values ('Bob', 15);"));
        input.add(CharStreams.fromString("insert into person values ('Allen', 22);"));
        input.add(CharStreams.fromString("insert into person (ID) values (23);"));
        input.add(CharStreams.fromString("update person set name = 'Emily' where name = 'Bob';"));
        input.add(CharStreams.fromString("select * from person where name = 'Allen';"));
        input.add(CharStreams.fromString("delete from person where ID = 15;"));
        input.add(CharStreams.fromString("drop table person;"));

        manager = Manager.getInstance();
    }

    @Test
    public void parseTest() {
        try {
            if (!manager.databases.containsKey("testDatabase")) {
                db = manager.createDatabaseIfNotExists("testDatabase");
            }
            else {
                db = manager.databases.get("testDatabase");
            }

            MyVisitor visitor = new MyVisitor();
            for(CharStream stream: input) {
                lexer = new SQLLexer(stream);
                CommonTokenStream tokenStream = new CommonTokenStream(lexer);
                SQLParser parser = new SQLParser(tokenStream);
                ArrayList res = (ArrayList) visitor.visit(parser.parse());
                for (Object statement : res) {
                    ExecResult result = ((AbstractStatement) statement).exec(db);
                }
            }
        } catch (Exception e) {
            System.out.println(e.toString());
            fail();
        }
    }

    @Test
    public void execTest() {
        for(int i = 0; i < 1000000; i++) {
            for(int j = 0; j < 1000000; j++) {

            }
        }
    }
}
