package com.memorydb.distribution;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Classe qui gère la découverte automatique des nœuds du cluster
 */
@ApplicationScoped
public class NodeDiscovery {

    private static final Logger logger = LoggerFactory.getLogger(NodeDiscovery.class);
    private static final int MULTICAST_PORT = 4446;
    private static final String DISCOVERY_MESSAGE_PREFIX = "MEMORYDB_NODE:";
    private static final long HEARTBEAT_INTERVAL_MS = 5000;
    private static final long NODE_TIMEOUT_MS = 15000;
    private static final int MAX_MULTICAST_ERRORS = 3;
    
    @ConfigProperty(name = "memorydb.discovery.multicast.address", defaultValue = "230.0.0.1")
    private String multicastGroupAddress;
    
    @ConfigProperty(name = "memorydb.node.id", defaultValue = "default-node")
    private String nodeId;
    
    @ConfigProperty(name = "quarkus.profile", defaultValue = "dev")
    private String profile;
    
    @ConfigProperty(name = "memorydb.discovery.multicast.enabled", defaultValue = "true")
    private boolean multicastEnabled;
    
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private ClusterManager clusterManager;
    private boolean isRunning = false;
    private final AtomicBoolean multicastErrorReported = new AtomicBoolean(false);
    private int multicastErrorCount = 0;
    
    /**
     * Démarre l'écoute des annonces multicast
     */
    private void startMulticastListener() {
        Thread listenerThread = new Thread(() -> {
            try (MulticastSocket socket = new MulticastSocket(MULTICAST_PORT)) {
                if (!isValidMulticastAddress(multicastGroupAddress)) {
                    logger.error("L'adresse '{}' n'est pas une adresse multicast valide. Utilisation de l'adresse par défaut '230.0.0.1'", multicastGroupAddress);
                    multicastGroupAddress = "230.0.0.1"; // Fallback to a valid multicast address
                }
                
                InetAddress group = InetAddress.getByName(multicastGroupAddress);
                socket.joinGroup(group);
                
                byte[] buffer = new byte[1024];
                
                logger.info("Écoute des annonces multicast sur {}:{}", multicastGroupAddress, MULTICAST_PORT);
                
                while (isRunning) {
                    try {
                        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                        socket.receive(packet);
                        
                        String message = new String(packet.getData(), 0, packet.getLength());
                        if (message.startsWith(DISCOVERY_MESSAGE_PREFIX)) {
                            processDiscoveryMessage(message, packet.getAddress().getHostAddress());
                        }
                    } catch (Exception e) {
                        if (isRunning) {
                            logger.error("Erreur lors de la réception d'un paquet multicast: {}", e.getMessage());
                        }
                    }
                }
                
                try {
                    socket.leaveGroup(group);
                } catch (Exception e) {
                    logger.warn("Erreur lors de la sortie du groupe multicast: {}", e.getMessage());
                }
            } catch (Exception e) {
                if (isRunning) {
                    if ("dev".equals(profile)) {
                        logger.warn("Erreur dans l'écoute multicast (mode développement): {}. La découverte des nœuds sera simulée localement.", e.getMessage());
                        // En mode développement, nous simulons un environnement à un seul nœud
                        useLocalDiscoveryFallback();
                    } else {
                        logger.error("Erreur critique dans l'écoute multicast: {}", e.getMessage());
                    }
                }
            }
        });
        
        listenerThread.setDaemon(true);
        listenerThread.start();
    }
    
    /**
     * Vérifie si l'adresse est une adresse multicast valide
     * @param address l'adresse à vérifier
     * @return true si l'adresse est multicast
     */
    private boolean isValidMulticastAddress(String address) {
        try {
            InetAddress inetAddress = InetAddress.getByName(address);
            return inetAddress.isMulticastAddress();
        } catch (Exception e) {
            return false;
        }
    }
    
    /**
     * Utilise une découverte locale en mode développement
     */
    private void useLocalDiscoveryFallback() {
        logger.info("Utilisation du mode de découverte locale (fallback) pour le développement");
        // Nous n'avons qu'un seul nœud, donc rien à faire de spécial
    }
    
    /**
     * Démarre l'envoi périodique d'annonces multicast
     */
    private void startHeartbeatSender() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                if (!isRunning) {
                    return;
                }
                
                // Validate multicast address
                InetAddress group = InetAddress.getByName(multicastGroupAddress);
                if (!group.isMulticastAddress()) {
                    logger.error("Adresse invalide pour multicast heartbeat: {}, doit être dans la plage 224.0.0.0 à 239.255.255.255", multicastGroupAddress);
                    return;
                }
                
                NodeInfo localNode = clusterManager.getLocalNode();
                String message = String.format("%s%s:%s:%d", 
                        DISCOVERY_MESSAGE_PREFIX, 
                        localNode.getId(), 
                        localNode.getAddress(), 
                        localNode.getPort());
                
                try (DatagramSocket socket = new DatagramSocket()) {
                    byte[] buffer = message.getBytes();
                    DatagramPacket packet = new DatagramPacket(
                            buffer, buffer.length, group, MULTICAST_PORT);
                    socket.send(packet);
                    logger.debug("Heartbeat envoyé: {}", message);
                    
                    // Réinitialiser le compteur d'erreurs si l'envoi a réussi
                    if (multicastErrorCount > 0) {
                        multicastErrorCount = 0;
                        multicastErrorReported.set(false);
                        logger.info("Communication multicast rétablie");
                    }
                }
            } catch (Exception e) {
                if (++multicastErrorCount >= MAX_MULTICAST_ERRORS) {
                    if (!multicastErrorReported.getAndSet(true)) {
                        if ("dev".equals(profile)) {
                            logger.warn("Problème de communication multicast en mode développement (après {} tentatives): {}. " + 
                                    "Ceci est probablement normal si vous n'avez pas configuré multicast sur votre réseau.", 
                                    multicastErrorCount, e.getMessage());
                        } else {
                            logger.error("Erreur persistante lors de l'envoi du battement de cœur (après {} tentatives): {}", 
                                    multicastErrorCount, e.getMessage());
                        }
                    }
                } else if (multicastErrorCount == 1) {
                    // Log only the first few errors to avoid log spam
                    logger.error("Erreur lors de l'envoi du battement de cœur: {}", e.getMessage());
                }
            }
        }, 0, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Traite un message de découverte reçu
     */
    private void processDiscoveryMessage(String message, String senderIp) {
        try {
            String payload = message.substring(DISCOVERY_MESSAGE_PREFIX.length());
            String[] parts = payload.split(":");
            if (parts.length == 3) {
                String nodeId = parts[0];
                String nodeAddress = parts[1];
                int nodePort = Integer.parseInt(parts[2]);
                
                // Ignore le nœud local
                if (!nodeId.equals(clusterManager.getLocalNode().getId())) {
                    NodeInfo nodeInfo = new NodeInfo(nodeId, nodeAddress, nodePort);
                    clusterManager.addNode(nodeInfo);
                    logger.debug("Nœud découvert: {}", nodeInfo);
                }
            }
        } catch (Exception e) {
            logger.error("Erreur lors du traitement du message de découverte: {}", e.getMessage());
        }
    }
    
    /**
     * Démarre le service de découverte
     */
    public void startDiscovery(ClusterManager clusterManager) {
        if (isRunning) {
            logger.warn("Le service de découverte est déjà en cours d'exécution");
            return;
        }
        
        this.clusterManager = clusterManager;
        isRunning = true;
        
        logger.info("Démarrage du service de découverte de nœuds avec ID: {}", nodeId);
        
        if (multicastEnabled) {
            // Démarrer l'écoute multicast
            startMulticastListener();
            
            // Démarrer l'envoi de heartbeats
            startHeartbeatSender();
            
            logger.info("Mode multicast activé pour la découverte des nœuds");
        } else {
            logger.info("Mode multicast désactivé (memorydb.discovery.multicast.enabled=false). " +
                    "Fonctionnement en mode nœud unique.");
            useLocalDiscoveryFallback();
        }
        
        // Planifier la vérification des nœuds expirés
        scheduler.scheduleAtFixedRate(() -> {
            try {
                long now = System.currentTimeMillis();
                for (NodeInfo node : new ArrayList<>(clusterManager.getAllNodes())) {
                    if (!node.getId().equals(clusterManager.getLocalNode().getId()) &&
                            now - node.getLastSeen() > NODE_TIMEOUT_MS) {
                        logger.info("Nœud expiré: {}", node);
                        clusterManager.removeNode(node.getId());
                    }
                }
            } catch (Exception e) {
                logger.error("Erreur lors de la vérification des nœuds expirés: {}", e.getMessage());
            }
        }, NODE_TIMEOUT_MS, NODE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        
        logger.info("Service de découverte démarré");
    }
    
    /**
     * Arrête le service de découverte
     */
    public void stopDiscovery() {
        if (!isRunning) {
            return;
        }
        
        isRunning = false;
        scheduler.shutdown();
        logger.info("Service de découverte arrêté");
    }
} 