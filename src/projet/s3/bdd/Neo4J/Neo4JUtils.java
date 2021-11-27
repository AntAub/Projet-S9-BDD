package projet.s3.bdd.Neo4J;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import projet.s3.bdd.Databases;

import java.util.ArrayList;
import java.util.HashMap;

public class Neo4JUtils {

    public static StatementResult customQuery(Databases dbs, String s) {
        return dbs.getNeo4J().getSession().run(s);
    }

    public static HashMap<Integer, String> getArticlesTitles(Databases dbs) {

        StatementResult queryResult = customQuery(dbs, "MATCH (n:Article) RETURN n.titre, id(n) as id");

        HashMap<Integer, String> titles = new HashMap<>();

        queryResult.forEachRemaining(record -> {
            titles.put(record.get("id").asInt(), record.get("n.titre").asString());
        });

        return titles;
    }

    public static void close(Databases dbs) {

        dbs.getNeo4J().close();
        System.out.println("Fermeture de la connexion à la base Neo4J (" + dbs.getNeo4J().getHostName() + ")");
    }
}
