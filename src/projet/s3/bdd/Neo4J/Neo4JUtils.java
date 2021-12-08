package projet.s3.bdd.Neo4J;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class Neo4JUtils {

    private final Neo4J neo4J;

    public Neo4JUtils(Neo4J neo4J) {
        this.neo4J = neo4J;
    }

    /**
     * Exécute une requête personnalisée
     * @param s Requête au format d'intérrogation Neo4J
     * @return Resultat de la requête
     */
    public StatementResult customQuery(String s) {
        return this.neo4J.getSession().run(s);
    }

    /**
     * Retour les tous articles (id, titre)
     * @return Articles (id, titre)
     */
    public HashMap<Integer, String> getAllArticlesTitles() {

        StatementResult queryResult = customQuery("MATCH (n:Article) RETURN n.titre, id(n) as id");

        HashMap<Integer, String> titles = new HashMap<>();

        queryResult.forEachRemaining(record ->
                titles.put(record.get("id").asInt(), record.get("n.titre").asString())
        );

        return titles;
    }

    /**
     * Retour la liste des titres des articles dont l'id est passé en paramètre
     * @param articlesIdsList Liste des Id's d'articles
     * @return Liste des titres des articles
     */
    public ArrayList<String> getArticlesTitlesFromArticlesIds(ArrayList<Integer> articlesIdsList) {

        StatementResult queryResult = this.customQuery("MATCH (n:Article) WHERE id(n) in " + articlesIdsList + " RETURN n.titre ORDER BY n.titre ASC");
        
        ArrayList<String> titles = new ArrayList<>();
        
        queryResult.forEachRemaining(title -> titles.add(title.get("n.titre").asString()));
        
        return titles;
    }

    /**
     * Retourne, par ordre décroissant de nombre d’articles écrits puis
     * par ordre croissant de nom les auteurs ayant écrit le plus d’articles
     * @param limit Nombre d'auteurs maximum à afficher
     * @return Liste des auteurs
     */
    public ArrayList<String> getAuthorsWithMostArticles(int limit) {

        StatementResult queryResult = this.customQuery("MATCH (n:Auteur)-[e:Ecrire]->(a:Article) RETURN n.nom, count(a) as nbArticles ORDER BY nbArticles DESC, n.nom ASC LIMIT " + limit);

        ArrayList<String> authors = new ArrayList<>();

        queryResult.forEachRemaining(record ->
                authors.add(record.get("nbArticles").asInt() + " - " + record.get("n.nom").asString())
        );

        return authors;

    }

    /**
     * Récupère le titre d'un article à partir de son Id
     * @param id Id de l'article
     * @return Titre de l'article
     */
    public String getArticleTitleFromId(Integer id) {

        return this.customQuery("MATCH (n:Article) WHERE id(n) = "+ id +" RETURN n.titre ORDER BY n.title ASC").next().get("n.titre").asString();
    }

    public LinkedHashMap<Integer, String> getArticleTitleFromId(ArrayList<Integer> ids, Boolean ascOrder){

        String order = (ascOrder) ? "ASC" : "DESC";
        StatementResult result = this.customQuery("MATCH (n:Article) WHERE id(n) in " + ids + " RETURN id(n) as id, n.titre as title ORDER BY title " + order);

        LinkedHashMap<Integer, String> articleTitles = new LinkedHashMap<>();

        while (result.hasNext()) {
            Record record = result.next();
            articleTitles.put(record.get("id").asInt(), record.get("title").asString());
        }

        return articleTitles;
    }

    /**
     * Ferme à la connexion à la base de données Neo4J
     */
    public void close() {

        this.neo4J.close();
        System.out.println("Fermeture de la connexion à la base Neo4J (" + this.neo4J.getHostName() + ")");
    }

}
