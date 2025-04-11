package com.memorydb;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;

/**
 * Gestionnaire du cycle de vie de l'application
 */
@ApplicationScoped
public class ApplicationLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(ApplicationLifecycle.class);
    
    /**
     * Méthode appelée au démarrage de l'application
     * @param event L'événement de démarrage
     */
    void onStart(@Observes StartupEvent event) {
        logger.info("MemoryDB démarré");
        // Initialization is handled by BootstrapService
    }
    
    /**
     * Méthode appelée à l'arrêt de l'application
     * @param event L'événement d'arrêt
     */
    void onStop(@Observes ShutdownEvent event) {
        logger.info("MemoryDB s'arrête...");
    }
} 