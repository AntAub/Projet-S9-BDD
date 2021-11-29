package projet.s3.bdd;

import projet.s3.bdd.MongoDB.MongoDB;
import projet.s3.bdd.Neo4J.Neo4J;

import java.util.logging.Level;
import java.util.logging.Logger;

public class Databases {

    private MongoDB mongoDB;
    private Neo4J neo4J;

    public Databases() throws Exception {
        /* On retire les messages d'information du drive MongoDB */
        Logger mongoLogger = Logger.getLogger( "org.mongodb.driver" );
        mongoLogger.setLevel(Level.WARNING);

        this.establishMongoDBConnection();
        this.establishNeo4JConnection();

        System.out.println("-----------------------------------------------------------------");
    }

    /**
     * Etablissement de la connexion à la base de données MongoDB
     */
    private void establishMongoDBConnection() throws Exception {

        try {
            /* Connexion à la base de données MongoDB */
            this.mongoDB = new MongoDB(Config.mongoDBhostAddress, Config.mongoDBhostport);

            /* Choix de la base de données */
            mongoDB.setDatabase(Config.mongoDBdbName);
        }
        catch (Exception e){

            throw new Exception("La connexion à la base MongoDB à échouée :"  + e.getMessage());
        }
    }

    /**
     * Etablissement de la connexion à la base de données Neo4J
     */
    private void establishNeo4JConnection() throws Exception {
        try{
            /* Connexion à la base de données Neo4J */
            this.neo4J = new Neo4J(Config.neo4JhostAddress,Config.neo4Jhostport);
        }
        catch (Exception e){
            throw new Exception("La connexion à la base Neo4J à échouée : " + e.getMessage());
        }
    }

    /**
     * Récupere la
     * @return Objet de type MongoDB pour manipulation de la base de données MongoDB
     */
    public MongoDB getMongoDB() {
        return mongoDB;
    }

    public Neo4J getNeo4J() {
        return neo4J;
    }
}
