package projet.s3.bdd.MongoDB;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.*;

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

        /* ensureIndex() deprecated : utilisation de createIndex() */
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

        Document document = this.mongodb.getCollection().find(Filters.eq("mot", word)).first();

        return (ArrayList<Integer>) document.get("documents");
    }

    /**
     * Recupère la liste des IDs d'articles où les mots apparaissent ainsi que leur nombre d'occurence dans chaque titre
     * @param words Liste des mots à rechercher
     * @param limit Nombre maximum de résultats
     * @return liste des id d'articles trié par nombre d'occurances des mots cherchés dans leur titre
     */
    @SuppressWarnings("unchecked")
    public LinkedHashMap<Integer, ArrayList<Integer>> getArticlesWithWordsInTitle(List<String> words, int limit) {

        /*
        * On formatte la liste des mots en ajoutant des guillemets pour permettre la recherche
        * et evité les injections de code
        */
        ArrayList<String> wordsLocal = new ArrayList<>();
        for (String word : words)
            wordsLocal.add("'" + word + "'");

        /* On requete mongoDB */
        AggregateIterable<Document> documents =
                this.mongodb.getCollection().aggregate(java.util.Arrays.asList(
                        //Liste de id d'articles contenant les mots recherchés
                        Document.parse("{$match : { mot : {$in : "+wordsLocal+"}}}"),
                        //Tri par ordre alphabétique
                        Document.parse("{$sort :{mot : -1}}"),
                        //Liste des mots associés à l'id d'articles
                        Document.parse("{$unwind : \"$documents\" }"),
                        //On groupe par id d'article pour compter le nombre d'occurances du mot par article
                        Document.parse("{$group : { _id: \"$documents\", nbOccurances : { $sum: 1 }}}"),
                        //On tri par nombre d'occurence des mots dans le titre
                        Document.parse("{$sort :{ nbOccurances : -1 }}"),
                        //On limite au N premier article
                        Document.parse("{$limit : " + limit + "}"),
                        //On groupe par nombre d'occurance
                        Document.parse("{$group : { _id : \"$nbOccurances\", documents: { $addToSet : \"$_id\" }}}"),
                        //On tri par nombre décroissant d'occurance
                        Document.parse("{$sort :{ _id : -1}}")
                ));

        LinkedHashMap<Integer, ArrayList<Integer>> articleList = new LinkedHashMap<>();

        for (Document document : documents) {
            articleList.put((Integer) document.get("_id"), (ArrayList<Integer>) document.get("documents"));
        }

        return articleList;
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

    /**
     * Ajoute l'id d'article à un mot clé existant sinon elle crée le mot clé
     * @param keyword Mot clé
     * @param articleId Id de l'article associé au mot clé
     * @return vrai si un nouveau mot clé est indexé, faux sinon
     */
    @SuppressWarnings("unchecked")
    public Boolean addArticleIdToKeyword(String keyword, Integer articleId) {

        Document keywordDocument = this.mongodb.getCollection().find(Filters.eq("mot", keyword)).first();

        if(keywordDocument == null){

            ArrayList<Integer> articles = new ArrayList<>();
            articles.add(articleId);
            this.insertDocument(new Document("mot", keyword).append("documents", articles));
            return true;
        }
        else{

            ((ArrayList<Integer>) keywordDocument.get("documents")).add(articleId);
            this.mongodb.getCollection().replaceOne(Filters.eq("mot",keyword), keywordDocument);
            return false;
        }
    }
}
