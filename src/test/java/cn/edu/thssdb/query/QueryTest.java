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
        input.add(CharStreams.fromString("CREATE TABLE course (stu_name String(256), course_name String(128) not null, PRIMARY KEY(course_name));"));
        input.add(CharStreams.fromString("CREATE TABLE teach (ID Int not null, t_name String(256), course_name String(128) not null, s_name String(128), PRIMARY KEY(ID));"));
        input.add(CharStreams.fromString("insert into person (name, ID) values ('Bob', 15);"));
        input.add(CharStreams.fromString("insert into person values ('Allen', 22);"));
        input.add(CharStreams.fromString("insert into person values ('Nami', 18);"));
        input.add(CharStreams.fromString("insert into person (ID) values (23);"));
        input.add(CharStreams.fromString("insert into course values ('Allen', 'RENZHIDAO');"));
        input.add(CharStreams.fromString("insert into course values ('Allen', 'RUANJIANFENXI');"));
        input.add(CharStreams.fromString("insert into course values ('Bob', 'SHUJUKU');"));
        input.add(CharStreams.fromString("insert into teach values (1, 'JiLiang', 'YIDONG', 'Allen');"));
        input.add(CharStreams.fromString("insert into teach values (2, 'JianMin', 'SHUJUKU', 'Bob');"));
        input.add(CharStreams.fromString("insert into teach values (3, 'JianMin', 'SHUJUKU', 'Zera');"));
        input.add(CharStreams.fromString("insert into teach values (4, 'ChunPing', 'RENZHIDAO', 'Allen');"));
//        input.add(CharStreams.fromString("update person set name = 'Emily' where name = 'Bob';"));
//        input.add(CharStreams.fromString("select * from person where name = 'Allen';"));
//        input.add(CharStreams.fromString("select ID from person join course on person.name=course.stu_name;"));
        input.add(CharStreams.fromString("select person.name, course.course_name from person join course on person.name=course.stu_name join teach on person.name=teach.s_name;"));
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
