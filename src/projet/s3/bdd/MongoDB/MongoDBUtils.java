package projet.s3.bdd.MongoDB;

import com.mongodb.Block;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.neo4j.driver.v1.StatementResult;
import projet.s3.bdd.Neo4J.Neo4JUtils;

import java.util.ArrayList;
import java.util.List;

public class MongoDBUtils {

    private final MongoDB mongodb;

    public MongoDBUtils(MongoDB mongodb) {
        this.mongodb = mongodb;
    }

    /**
     * Choix de la collection
     * @param index Nom de la collection
     */
    public void setCollection(String index) {
        this.mongodb.setCollection(index);
    }

    /**
     * Insertion d'un document dans une base de données MongoDB
     * @param document Document à insèrer
     */
    public void insertDocument(Document document) {

        this.mongodb.insertDocument(document);
    }

    /**
     * Retourne la totalités des articles en
     * @return FindIterable<Document> List de documents
     */
    public FindIterable<Document> getAllArticles() {

        return this.mongodb.getCollection().find();
    }

    /**
     * Met en place un index par ordre croissant sur les valeurs des champs choisis
     * @param fielNames Nom des champs sur lequels ils faut crée un Index
     */
    public void ensureAscendingIndex(String fielNames) {

        //ensureIndex() deprecated : utilisation de createIndex()
        this.ensureIndex(Indexes.ascending(fielNames));
    }

    /**
     * Crée un index sur les champs choisis
     * @param order Ordre de l'index Indexes.ascending(fielNames)
     */
    private void ensureIndex(Bson order) {

        this.mongodb.getCollection().createIndex(order);
    }

    /**
     * Liste les articles où leur titre contient le mot recherché
     * @param word Mot recherché
     * @return Liste des articles où leur titre contient le mot recherché
     */
    @SuppressWarnings("unchecked")
    public ArrayList<Integer> getArticlesIdsListFromWord(String word) {

        Document document = (Document) this.mongodb.getCollection().find(Filters.eq("mot", word)).first();

        return (ArrayList<Integer>) document.get("documents");
    }

    /**
     * Recupère la liste des IDs d'articles où les mots apparaissent ainsi que leur nombre d'occurence dans chaque titre
     * @param words Liste des mots à rechercher
     * @param limit Nombre maximum de résultats
     * @return L'index 0 est la liste des id d'articles. L'index 1 est le nombre d'occurances des mots recherchés.
     */
    @SuppressWarnings("unchecked")
    public ArrayList<Object> getArticlesWithWordsInTitle(List<String> words, int limit) {

        /*
        * On formatte la liste des mots en ajoutant des guillemets pour permettre la recherche
        * et evité les injections de code
        */
        ArrayList<String> wordsLocal = new ArrayList<>();
        for (String word : words)
            wordsLocal.add("'" + word + "'");

        /* On requete mongoDB */
        AggregateIterable<Document> documents = this.mongodb.getCollection().aggregate(java.util.Arrays.asList(
                Document.parse("{$match : { mot : {$in : " + wordsLocal + "}}}"),
                Document.parse("{$sort :{mot : -1}}"),
                Document.parse("{$unwind : \"$documents\" }"),
                Document.parse("{$group : {_id: \"$documents\", nbOccurances : { $sum: 1}}}"),
                Document.parse("{$sort :{ nbOccurances : -1}}"),
                Document.parse("{$limit :" + limit + "}")
        ));

        ArrayList<Object> articles = new ArrayList<>();
        articles.add(new ArrayList<Integer>()); //Liste des IDs d'article
        articles.add(new ArrayList<Integer>()); //Liste du nombre d'occurances

        for (Document document : documents) {
            ((ArrayList<Integer>) articles.get(0)).add((Integer) document.get("_id"));
            ((ArrayList<Integer>) articles.get(1)).add((Integer) document.get("nbOccurances"));
        }

        return articles;
    }

    /**
     * Supprime un index de la base de données MongoDB
     * @param index Nom de l'index à supprimer
     */
    public void dropIndex(String index){

        this.mongodb.setCollection(index);
        this.mongodb.getCollection().drop();
        System.out.println("Index " + index + " supprimé");
    }

    /**
     * Ferme à la connexion à la base de données MongoDB
     */
    public void close() {

        this.mongodb.close();
        System.out.println("Fermeture de la connexion à la base Mongo (" + this.mongodb.getHostName() + ")");
    }
}
