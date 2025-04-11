package com.memorydb.system;

import com.memorydb.distribution.ClusterManager;
import io.quarkus.runtime.StartupEvent;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import java.net.InetAddress;

/**
 * Service qui initialise le système au démarrage de l'application
 */
@ApplicationScoped
public class BootstrapService {

    private static final Logger logger = LoggerFactory.getLogger(BootstrapService.class);

    @Inject
    ClusterManager clusterManager;

    @ConfigProperty(name = "memorydb.node.id", defaultValue = "default-node")
    String nodeId;

    @ConfigProperty(name = "quarkus.http.host", defaultValue = "0.0.0.0")
    String httpHost;

    @ConfigProperty(name = "quarkus.http.port", defaultValue = "8093")
    int httpPort;

    /**
     * Méthode appelée au démarrage de l'application
     */
    void onStart(@Observes StartupEvent ev) {
        try {
            logger.info("Initialisation du système MemoryDB...");
            
            // Utilise l'adresse IP locale si l'hôte HTTP est 0.0.0.0
            String nodeAddress = httpHost;
            if ("0.0.0.0".equals(httpHost)) {
                try {
                    nodeAddress = InetAddress.getLocalHost().getHostAddress();
                } catch (Exception e) {
                    logger.warn("Impossible de résoudre l'adresse IP locale, utilisation de 'localhost'");
                    nodeAddress = "localhost";
                }
            }
            
            logger.info("Initialisation du gestionnaire de cluster avec ID: {}, Adresse: {}, Port: {}", 
                    nodeId, nodeAddress, httpPort);
            
            // Initialise le gestionnaire de cluster
            clusterManager.initialize(nodeId, nodeAddress, httpPort);
            
            logger.info("Système MemoryDB initialisé avec succès sur le port {}", httpPort);
        } catch (Exception e) {
            logger.error("Erreur lors de l'initialisation du système: {}", e.getMessage(), e);
        }
    }
} 