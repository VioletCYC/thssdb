package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import cn.edu.thssdb.parser.*;

import java.util.ArrayList;

public class QueryTest {
    private SQLParser parser;
    private SQLLexer lexer;
    private Manager manager;
    private Database db;

    @Before
    public void init() {
        CharStream input = CharStreams.fromString(
                "CREATE TABLE person (name String(256), ID Int not null, PRIMARY KEY(ID));" +
                        "insert into person (name, ID) values ('Bob', 15);" +
                        "insert into person values ('Allen', 22);" +
                        "insert into person(ID) values(23);" +
                        "update person set ID = 16 where ID = 15;" +
                        "delete from person where col1 = 15;" +
                        "drop table person;");
        lexer = new SQLLexer(input);
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        parser = new SQLParser(tokenStream);

        manager = Manager.getInstance();
    }

    @Test
    public void parseTest() {
        Database db = manager.createDatabaseIfNotExists("testDatabase");
//        MyVisitor visitor = new MyVisitor();
//        ArrayList res = (ArrayList) visitor.visit(parser.parse());
//        for(Object statement: res) {
////            ((AbstractStatement) statement).exec();
//        }
    }

    @Test
    public void execTest() {
        for(int i = 0; i < 1000000; i++) {
            for(int j = 0; j < 1000000; j++) {

            }
        }
    }
}
