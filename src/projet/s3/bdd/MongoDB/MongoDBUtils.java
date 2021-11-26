package projet.s3.bdd.MongoDB;

import com.mongodb.client.FindIterable;
import org.bson.Document;
import org.bson.types.BasicBSONList;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import projet.s3.bdd.Databases;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MongoDBUtils {

    /**
     * Crée l'index dans la base MongoDB à partir des articles de la base Neo4J
     * @param dbs Objet d'accès aux base de données
     */
    public static void createIndex(Databases dbs) {

        System.out.println("-- Création de l'index des titres d'article dans MongoDB --");

        dbs.getMongoDB().setCollection("index");

        /* Récupération des articles dans la base Neo4J */
        StatementResult result = dbs.getNeo4J().getSession()
                .run("MATCH (n:Article) RETURN n.titre, id(n) as id");

        int nbArticle = 0;
        while (result.hasNext()) {

            Record record = result.next();

            String title = record.get("n.titre").asString();
            int id = record.get("id").asInt();

            /* Création de la liste des mots clés */
            StringTokenizer tokenizedTitle = new StringTokenizer(title.toLowerCase(), " ,'-:;()+[]{}?!./\\");
            BasicBSONList bson = new BasicBSONList();

            while (tokenizedTitle.hasMoreTokens()) {
                /* Mot suivant et retrait des espaces innutiles */
                String word = tokenizedTitle.nextToken().trim();
                bson.add(word);
            }

            /* Insertion dans la collection MongoDB */
            dbs.getMongoDB().getCollection()
                    .insertOne(new Document("idDocument", id).append("motsCles", bson));

            System.out.print("Nombre de titres d'articles indéxés : " + ++nbArticle + "\r");
        }
        System.out.print("\n");
        System.out.println("-----------------------------------------------------------");
    }

    /**
     * Crée l'index inversé dans la base MongoDB à partir de la collection index
     * @param dbs Objet d'accès aux base de données
     */
    @SuppressWarnings("unchecked")
    public static void createInvertIndex(Databases dbs) {

        System.out.println("-- Création de l'index inversé des mots-clé dans MongoDB --");

        /* Dictionnaire d'association Mot-clés / Articles */
        HashMap<String, ArrayList<Integer>> keywordsBelongs = new HashMap<>();

        /* Récupération des documents dans MongoDB */
        dbs.getMongoDB().setCollection("index");
        FindIterable<Document> documents = dbs.getMongoDB().getCollection().find();

        for (Document document : documents) {
            ArrayList<String> keywords = (ArrayList<String>) document.get("motsCles");
            keywords.forEach((keyword) -> {
                if (keywordsBelongs.containsKey(keyword)) {
                    /* on ajoute l'ID d'article à la liste du mot clé*/
                    keywordsBelongs.get(keyword).add((Integer) document.get("idDocument"));
                } else {
                    //TODO : expliciter ce code
                    ArrayList<Integer> articles = new ArrayList<>();
                    articles.add((Integer) document.get("idDocument"));
                    keywordsBelongs.put(keyword, articles);
                }
            });
        }

        /* Création du nouvel index */
        dbs.getMongoDB().setCollection("indexInverse");

        AtomicInteger nbKeywordIndexed = new AtomicInteger();
        keywordsBelongs.forEach((keyword, articles) ->{
            System.out.print("Nombre de mots-clé indéxés : " + nbKeywordIndexed.incrementAndGet() + "\r");
            BasicBSONList articlesList = new BasicBSONList();
            articlesList.addAll(articles);
            dbs.getMongoDB().getCollection()
                    .insertOne(new Document("mot", keyword).append("documents", articlesList));
        });
        System.out.print("\n");
        System.out.println("-----------------------------------------------------------");
    }
}
