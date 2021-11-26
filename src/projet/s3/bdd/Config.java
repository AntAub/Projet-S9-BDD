package projet.s3.bdd;

import java.util.ResourceBundle;

public class Config {

    private static final ResourceBundle bundle = ResourceBundle.getBundle("projet.s3.config");

    public final static String mongoDBhostAddress = bundle.getString("mongoDB.host.address");
    public final static int mongoDBhostport = Integer.parseInt(bundle.getString("mongoDB.host.port"));
    public final static String mongoDBdbName = bundle.getString("mongoDB.host.db.name");

    public final static String neo4JhostAddress = bundle.getString("neo4J.host.address");
    public final static int neo4Jhostport = Integer.parseInt(bundle.getString("neo4J.host.port"));
}
