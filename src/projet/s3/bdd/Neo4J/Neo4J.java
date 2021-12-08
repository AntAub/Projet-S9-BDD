package projet.s3.bdd.Neo4J;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;

public class Neo4J {

    private final String hostName;

    private Driver driver;
    private final Session session;

    /**
     * Création d'une connexion à une base de données Neo4J
     * @param host Nom du serveur hôte de la base de données
     * @param port Port du service de la base de données
     * @throws Exception Exeception levée en cas d'échec de la connexion
     */
    public Neo4J(String host, int port) throws Exception {

        this.hostName = host;

        try{
            this.driver = GraphDatabase.driver("bolt://" + host + ":" + port);
            this.session = driver.session();
            System.out.println("Connexion établie à Neo4J : " + this.hostName);
        }
        catch (Exception e){
            driver.close();
            throw new Exception("Connexion impossible à " + this.hostName);
        }

    }

    /**
     * Retourne la session Neo4J permetant l'execution de requetes
     * @return Session Neo4J
     */
    public Session getSession() {
        return session;
    }

    /**
     * Fermeture de la connexion à la base de données Neo4J
     */
    public void close() {

        this.session.close();
    }

    /**
     * Nom de la serveur qui héberge la base de données Neo4J
     * @return Nom du serveur
     */
    public String getHostName() {

        return this.hostName;
    }
}
