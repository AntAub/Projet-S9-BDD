package projet.s3.bdd.Neo4J;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;

public class Neo4J {

    private final String hostName;

    private Driver driver;
    private Session session;

    public Neo4J(String host, int port) throws Exception {

        this.hostName = host;

        try{
            this.driver = GraphDatabase.driver("bolt://" + host + ":" + port);
            this.session = driver.session();
            System.out.println("Connexion établie à Neo4J : " + this.hostName);
        }
        catch (Exception e){
            session.close();
            driver.close();
            throw new Exception("Connexion impossible à " + this.hostName + "\n" + e.getMessage());
        }

    }

    public Session getSession() {
        return session;
    }
}
