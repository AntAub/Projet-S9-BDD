/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package projet.s3.test;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.types.BasicBSONList;
import org.neo4j.driver.v1.*;

import java.util.*;

public class ProjetS3BDD {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        boolean isEqual = false;

        String uri = "mongodb://cloud.antonylaget.com:1027";
        MongoClientURI connectionString = new MongoClientURI(uri);
        MongoClient mongoClient = new MongoClient(connectionString);

        Driver driver = GraphDatabase.driver("bolt://cloud.antonylaget.com:7687");
        Session session = driver.session();

        script(mongoClient, session);

        mongoClient.close();

        session.close();
        driver.close();
    }

    public static void script(MongoClient mongoClient, Session session) {
        MongoDatabase bdd = mongoClient.getDatabase("dbDocuments");
        bdd.drop();
        bdd.createCollection("index");

        MongoCollection<Document> index = bdd.getCollection("index");
        creerIndex(session, index);
        index.createIndex(Indexes.ascending("motCles"));
        listerDocuments(index);
        System.out.println(index.count());

        MongoCollection<Document> indexInverse = bdd.getCollection("indexInverse");
        creerIndexInverse(bdd, index);
        indexInverse.createIndex(Indexes.ascending("mot"));
        listerDocuments(indexInverse);

        rechercherDansIndexInverse("with", indexInverse);

    }

    // Fonctions exercices

    public static void rechercherDansIndexInverse(String mot, MongoCollection<Document> indexInverse) {
        Document doc = indexInverse.find(Filters.eq("mot", mot)).first();
        ArrayList documents = (ArrayList) doc.get("documents");
        System.out.println(documents.size());
    }

    public static void creerIndex(Session session, MongoCollection<Document> col) {
        StatementResult result = session.run("MATCH (n:Article) RETURN n.titre, id(n) as id");

        while (result.hasNext()) {
            Record record = result.next();
            String titre = record.get("n.titre").asString();
            int id = record.get("id").asInt();

            // Création de la liste de mot clef
            StringTokenizer titreToken = new StringTokenizer(titre.toLowerCase(), " ,'-:;()+[]{}?!./\\");

            BasicBSONList bson = new BasicBSONList();
            while (titreToken.hasMoreTokens()) {
                // Récupération du mot suivant
                String mot = titreToken.nextToken().trim();
                bson.add(mot);
            }

            // Création et ajout de l'article à la liste des articles
            col.insertOne(new Document("idDocument", id).append("motsCles", bson));
        }
    }

    @SuppressWarnings("unchecked")
    public static void creerIndexInverse(MongoDatabase bdd, MongoCollection<Document> collection) {
        FindIterable<Document> documents = collection.find();

        bdd.createCollection("indexInverse");
        MongoCollection<Document> indexInverse = bdd.getCollection("indexInverse");
        Map<String, ArrayList<Integer>> map = new HashMap<String, ArrayList<Integer>>();
        for (Document doc : documents){
            @SuppressWarnings("unchecked")
            ArrayList<String> a = (ArrayList<String>) doc.get("motsCles");
            a.forEach((motCle) -> {
                if(map.containsKey(motCle)) {
                    map.get(motCle).add((Integer) doc.get("idDocument"));
                }
                else {
                    ArrayList<Integer> listeArticles = new ArrayList<Integer>();
                    listeArticles.add((Integer) doc.get("idDocument"));
                    map.put(motCle, listeArticles);
                }
            });

        }

        Iterator iterator = map.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry mapentry = (Map.Entry) iterator.next();
            BasicBSONList docs = new BasicBSONList();
            docs.addAll((ArrayList<Integer>) mapentry.getValue());
            indexInverse.insertOne(new Document("mot", mapentry.getKey()).append("documents", docs));

        }


    }

    public static void creerIndexInverseObsolete(MongoDatabase bdd, MongoCollection<Document> index) {
        FindIterable<Document> articles = index.find();

        bdd.createCollection("indexInverse");
        MongoCollection<Document> indexInverse = bdd.getCollection("indexInverse");

        Document motCle;
        int i = 0;
        float avancementDiscret = 0;
        String barre = "";

        int indexCount = (int) index.count();

        // Pour chaque document de la collection index
        for (Document article : articles){
            int idDocument = (int) article.get("idDocument");
            // Pour chaque mots clés du document
            for(String mot : (ArrayList<String>) article.get("motsCles")) {
                motCle = indexInverse.find(Filters.eq("mot", mot)).first();
                // Si
                if (motCle != null) {
                    UpdateResult result = indexInverse.updateOne(Filters.eq("mot", mot), Updates.push("documents", idDocument));
                }
                else {
                    BasicBSONList bson = new BasicBSONList();
                    bson.add(idDocument);
                    indexInverse.insertOne(new Document("mot", mot).append("documents", bson));
                }
            }

            float avancement = (float) i / indexCount * 100;

            if(avancement >= avancementDiscret + 1) {
                barre = barre + ".";
                avancementDiscret += 1;
            }

            //System.out.println((float) i / indexCount * 100 + "%");
            System.out.println(barre);
            System.out.println("----------------------------------------------------------------------------------------------------");
            System.out.flush();
            i++;
        }
    }





    // Fonctions utiles BDD
    public static void listerBDD(MongoClient mongoClient) {
        MongoIterable<String> databases = mongoClient.listDatabaseNames();
        for (String db : databases) {
            System.out.println(db);
        }
    }

    public static void listerCollec(MongoDatabase db) {
        for (String collec : db.listCollectionNames()) {
            System.out.println(collec);
        }
    }

    public static void supprimerBDD(MongoClient mongoClient) {
        MongoIterable<String> databases = mongoClient.listDatabaseNames();
        for (String db : databases) {
            mongoClient.dropDatabase(db);
        }

    }

    public static void listerDocuments(MongoCollection<Document> collection) {
        FindIterable<Document> documents = collection.find();
        for (Document doc : documents) {
            System.out.println(doc.toJson());
        }
    }

}
