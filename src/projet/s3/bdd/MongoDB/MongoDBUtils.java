package projet.s3.bdd.MongoDB;

import com.mongodb.client.FindIterable;
import org.bson.Document;
import org.bson.types.BasicBSONList;
import projet.s3.bdd.Databases;
import projet.s3.bdd.Neo4J.Neo4JUtils;

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
        HashMap<Integer, String> titles = Neo4JUtils.getArticlesTitles(dbs);

        AtomicInteger nbArticle = new AtomicInteger();

        titles.forEach((id, title) -> {

            /* Création de la liste des mots clés */
            StringTokenizer tokenizedTitle = new StringTokenizer(title.toLowerCase(), " ,'-:;()+[]{}?!./\\");
            BasicBSONList bsonList = new BasicBSONList();

            while (tokenizedTitle.hasMoreTokens()) {
                /* Mot suivant et retrait des espaces innutiles */
                String word = tokenizedTitle.nextToken().trim();
                bsonList.add(word);
            }

            /* Insertion dans la collection MongoDB */
            MongoDBUtils.insertDocument(dbs, new Document("idDocument", id).append("motsCles", bsonList));
            System.out.print("Nombre de titres d'articles indéxés : " + nbArticle.incrementAndGet() + "\r");
        });

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
        System.out.println("-- Récupération des documents dans MongoDB ----------------");
        dbs.getMongoDB().setCollection("index");
        FindIterable<Document> documents = MongoDBUtils.getAllArticles(dbs);

        for (Document document : documents) {

            ArrayList<String> keywords = (ArrayList<String>) document.get("motsCles");
            keywords.forEach((keyword) -> {

                /* Si le mot-clé esrt déjà dans la liste */
                if (keywordsBelongs.containsKey(keyword)) {

                    /* Alors on ajoute l'ID d'article à la liste du mot clé*/
                    keywordsBelongs.get(keyword).add((Integer) document.get("idDocument"));
                } else {

                    /* Sinon on crée un nouvelle liste d'articles associées au mot-clé */
                    ArrayList<Integer> articles = new ArrayList<>();
                    /* Puis on ajoute le document courant */
                    articles.add((Integer) document.get("idDocument"));
                    keywordsBelongs.put(keyword, articles);
                }
            });
        }

        /* Création du nouvel index */
        System.out.println("-- Insertion de l'index inversé des mots-clé dans MongoDB --");
        dbs.getMongoDB().setCollection("indexInverse");

        AtomicInteger nbKeywordIndexed = new AtomicInteger();

        keywordsBelongs.forEach((keyword, articles) ->{

            System.out.print("Nombre de mots-clé indéxés : " + nbKeywordIndexed.incrementAndGet() + "\r");
            BasicBSONList articlesList = new BasicBSONList();
            articlesList.addAll(articles);

            MongoDBUtils.insertDocument(dbs, new Document("mot", keyword).append("documents", articlesList));
        });

        System.out.print("\n");
        System.out.println("-----------------------------------------------------------");
    }

    private static void insertDocument(Databases dbs, Document document) {

        dbs.getMongoDB().insertDocument(document);
    }

    private static FindIterable<Document> getAllArticles(Databases dbs) {

        return dbs.getMongoDB().getCollection().find();
    }

    public static void ensureIndex(Databases dbs, String s) {
        //TODO ensure index
        //dbs.getMongoDB().getCollection().ensureIndex();
    }

    public static void close(Databases dbs) {

        dbs.getMongoDB().close();
        System.out.println("Fermeture de la connexion à la base MongoDB (" + dbs.getMongoDB().getHostName() + ") fermée");
    }
}
