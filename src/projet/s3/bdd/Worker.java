package projet.s3.bdd;

import com.mongodb.Block;
import com.mongodb.client.FindIterable;
import org.bson.Document;
import org.bson.types.BasicBSONList;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import projet.s3.bdd.MongoDB.MongoDBUtils;
import projet.s3.bdd.Neo4J.Neo4JUtils;

public class Worker {

    private final MongoDBUtils mongoDBUtils;
    private final Neo4JUtils neo4jUtils;

    public Worker() throws Exception {
        System.out.println("-----------------------------------------------------------------");
        Databases dbs = new Databases();

        this.mongoDBUtils = new MongoDBUtils(dbs.getMongoDB());
        this.neo4jUtils = new Neo4JUtils(dbs.getNeo4J());

        /* Nettoyage de la base MongoDB : base propre même en cas de crash*/
        this.clear();
    }

    /**
     * Crée l'index dans la base MongoDB à partir des articles de la base Neo4J
     */
    public void createIndex() {
        System.out.println("-- 3.1. Création de l'index des titres d'article dans MongoDB ---");

        this.mongoDBUtils.setCollection("index");

        /* Récupération des articles dans la base Neo4J */
        HashMap<Integer, String> titles = this.neo4jUtils.getAllArticlesTitles();

        AtomicInteger nbArticle = new AtomicInteger(0);

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
            this.mongoDBUtils.insertDocument(new Document("idDocument", id).append("motsCles", bsonList));
            System.out.print("Nombre de titres d'articles indéxés : " + nbArticle.incrementAndGet() + "\r");
        });

        System.out.print("\n");
        System.out.println("-----------------------------------------------------------------");
    }

    /**
     * Met en place un index par ordre croissant sur les valeurs du tableau mots-clé
     */
    public void ensureKeywordsIndex() {

        System.out.println("-- 3.2. Création index sur le tableau de mots-clé dans MongoDB --");

        /* Création de l'index sur valeurs du tableau mots-clé */
        this.mongoDBUtils.ensureAscendingIndex("motsCles");

        System.out.println("-----------------------------------------------------------------");
    }

    /**
     * Crée l'index inversé dans la base MongoDB à partir de la collection index
     */
    @SuppressWarnings("unchecked")
    public void createInvertIndex() {

        System.out.println("-- 3.3. Création de l'index inversé des mots-clé dans MongoDB ---");

        /* Récupération des documents dans MongoDB */
        this.mongoDBUtils.setCollection("index");
        System.out.println("-- Récupération des documents dans MongoDB ----------------------");

        FindIterable<Document> articles = this.mongoDBUtils.getAllArticles();

        this.mongoDBUtils.setCollection("indexInverse");

        AtomicInteger nbKeyword = new AtomicInteger(0);

        for (Document article : articles) {

            ArrayList<String> keywords = (ArrayList<String>) article.get("motsCles");

            keywords.forEach(keyword ->{

                if(this.mongoDBUtils.addArticleIdToKeyword(keyword, (Integer) article.get("idDocument")))
                    System.out.print("Nombre de mots-clé indéxés : " + nbKeyword.incrementAndGet() + "\r");
            });
        }

        System.out.print("\n");
        System.out.println("-----------------------------------------------------------------");
    }

    /**
     * Rechercher dans indexInverse un mot dans MongoDB et
     * afficher par ordre alphabétique les titres des documents dans Neo4J
     * @param word Mot à rechercher dans l'index inversé
     */
    public void searchInvertIndex(String word) {

        System.out.println("-- 3.4. Recherche d'un mot dans l'index inversé -----------------");

        this.mongoDBUtils.setCollection("indexInverse");

        ArrayList<Integer> articlesIdsList = this.mongoDBUtils.getArticlesIdsListFromWord(word);

        ArrayList<String> articlesTitlesList = this.neo4jUtils.getArticlesTitlesFromArticlesIds(articlesIdsList);

        AtomicInteger count = new AtomicInteger(0);
        System.out.println("Liste des articles contenant le mot \" " + word + "\" (" + articlesTitlesList.size() + " résultat(s)) : ");
        articlesTitlesList.forEach((title) ->{
            //System.out.println("-- Résultat " + String.format("%02d", count.incrementAndGet()) + " : " + this.abbreviate(title, 48));
            System.out.println("-- Résultat " + String.format("%02d", count.incrementAndGet()) + " : " + title);
        });

        System.out.println("-----------------------------------------------------------------");
    }

    /**
     * Affiche, par ordre décroissant de nombre d’articles écrits puis
     * par ordre croissant de nom les 10 auteurs ayant écrit le plus d’articles
     */
    public void get10AuthorsWithMostArticles() {

        this.mongoDBUtils.setCollection("indexInverse");

        System.out.println("-- 3.5. Auteurs ayant écrit le plus d’articles ------------------");

        ArrayList<String> authorList = this.neo4jUtils.getAuthorsWithMostArticles(10);

        AtomicInteger count = new AtomicInteger(0);
        System.out.println("Liste des 10 auteurs ayant écrit le plus d’articles : ");
        authorList.forEach((author) ->{
            System.out.println("-- Auteur " + String.format("%02d", count.incrementAndGet()) + " : " + author);
        });

        System.out.println("-----------------------------------------------------------------");
    }

    /**
     * Recherche dans indexInverse plusieurs mots et afficher par ordre alphabétique les titres des documents dans Neo4J.
     * Les documents sont triés par nombre décroissant de mots de la requête contenus dans les documents.
     * @param words Tablea des mots à rechercher
     */
    @SuppressWarnings("unchecked")
    public void searchInvertIndexAdvanced(List<String> words) {

        this.mongoDBUtils.setCollection("indexInverse");

        System.out.println("-- 3.6. Recherche de documents avancée --------------------------");

        ArrayList<Object> articles = this.mongoDBUtils.getArticlesWithWordsInTitle(words, 10);

        ArrayList<Integer> listIdsArticles = (ArrayList<Integer>) articles.get(0);
        ArrayList<Integer> listNbOccurrences = (ArrayList<Integer>) articles.get(1);

        System.out.println("Liste des articles contenant les mots recherchés (" + listIdsArticles.size() + ") :");
        for(int i = 0; i < listIdsArticles.size(); i++){
            String title = this.neo4jUtils.getArticleTitleFromId(listIdsArticles.get(i));
            System.out.println(this.abbreviate(listIdsArticles.get(i) + " " + title + " ", 63) + listNbOccurrences.get(i));
        }

        System.out.println("-----------------------------------------------------------------");
    }

    /**
     * Créer un abbreviation d'une chaine de caractère en limitant sa longueur et ajoutant "..." à la fin
     * @param str Chaine de caractère à raccourcir
     * @param maxLength Taille maximum de la chaine de caractère
     * @return Chaine de caractère raccourcie
     */
    private String abbreviate(String str, int maxLength) {
        return ( str.length () > maxLength ) ? str.substring ( 0 , maxLength - 3 ).concat ( "... " ) : str;
    }

    /**
     * Ferme à la connexion à la toutes les bases de données (MongoDB, Neo4J)
     */
    public void closeConnections() {

        this.mongoDBUtils.close();
        this.neo4jUtils.close();
        System.out.println("-----------------------------------------------------------------");
    }

    /**
     * Supprime les index de la base de données MongoDB
     */
    public void clear() {

        this.mongoDBUtils.dropIndex("indexInverse");
        this.mongoDBUtils.dropIndex("index");
        System.out.println("-----------------------------------------------------------------");
    }
}
