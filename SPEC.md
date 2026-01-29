# Spécifications : Portage de MLFlow sur Hadoop (Hadoop-MLFlow)

Ce document décrit les spécifications techniques pour la création d'un service compatible MLFlow optimisé pour l'écosystème Hadoop.

## 1. Introduction
L'objectif est de fournir une plateforme de gestion du cycle de vie du Machine Learning (MLFlow) capable de s'exécuter nativement sur un cluster Hadoop, en utilisant HDFS pour le stockage des artefacts et HBase pour le stockage des métadonnées et métriques, tout en étant orchestré par YARN.

## 2. Architecture Globale
Le système est composé de trois éléments principaux :
*   **Back-end (API) :** Un service Spring Boot agissant comme une façade compatible avec l'API REST de MLFlow.
*   **Front-end (UI) :** Une interface web permettant de visualiser les expériences, les runs et les modèles (reprise ou adaptation de l'UI MLFlow).
*   **Application YARN :** Un package permettant le déploiement et la gestion du cycle de vie des services sur le cluster.

## 3. Spécifications Techniques

### 3.1. Back-end (API)
Le back-end implémente les interfaces REST de MLFlow (v2.0+).

*   **Compatibilité :** Doit supporter les clients MLFlow standards (Python, Java, R).
*   **Gestion des Métadonnées (Tracking Server) :**
    *   **Stockage :** HBase.
    *   **Entités :** Expériences, Runs, Paramètres, Métriques, Tags.
    *   **Schéma HBase :**
        *   **Expériences :** Table dédiée avec les métadonnées de l'expérience (nom, artifact_location, etc.).
        *   **Runs/Métriques :** Utilisation de colonnes dynamiques pour les métriques et paramètres afin de garantir une scalabilité maximale.
        *   **Metric History :** Table `mlflow_metric_history` pour stocker l'historique temporel des métriques (row key: `runId_metricKey_reversedTimestamp`).
        *   **Model Registry :** Tables `mlflow_registered_models` et `mlflow_model_versions` pour la gestion des modèles.
*   **Prompt Registry :** Table `mlflow_prompts` pour le versionnage et la gestion des templates de prompts.
*   **Scoring Service :** Endpoints `/invocations`, `/ping`, `/version`, `/metadata` pour servir les modèles loggés.
*   **Scorer Management :** Endpoints `/api/2.0/mlflow/scorers` (`/list`, `/versions`, `/register`, `/get`, `/delete`) pour gérer les entités scorers associées aux expériences.
*   **AI Gateway :** Routes `/api/2.0/mlflow/gateway` pour centraliser les accès aux modèles LLM.
*   **Gestion des Artefacts :**
    *   **Stockage :** HDFS.
    *   **Opérations :** Upload, download et listage des fichiers de modèles, logs et graphiques.
*   **Sécurité et Exposition :**
    *   Exposition via **Apache Knox** pour la centralisation des accès et l'authentification (Fait).
    *   Découverte de service via **Zookeeper** pour permettre à Knox de router les requêtes vers les instances actives.

### 3.2. Front-end
L'interface utilisateur doit offrir une expérience similaire à l'UI MLFlow d'origine.

*   **Visualisation :** Tableaux de bord pour comparer les runs, graphiques de progression des métriques.
*   **Gestion des Modèles :** Registre de modèles permettant le versionnage et le changement d'état (Staging, Production).
*   **Communication :** Appels exclusifs au Back-end via le gateway Knox.

### 3.3. Déploiement YARN
Le service doit être packagé pour être déployé en tant qu'application YARN (via Apache Slider ou un framework custom comme YARN Service API).

*   **Composants :**
    *   Un container pour l'API (Back-end).
    *   Un container pour le Front-end (éventuellement servi par le back-end).
*   **Haute Disponibilité :** Support de plusieurs instances de l'API avec enregistrement dans Zookeeper.
*   **Isolation :** Utilisation des Control Groups (cgroups) de YARN pour limiter les ressources CPU/Mémoire.

## 4. Flux de Données
1.  **Client MLFlow** envoie une requête (ex: `log_metric`) à l'URL Knox.
2.  **Knox** authentifie la requête et la transfère à une instance de l'**API Hadoop-MLFlow** (découverte via Zookeeper).
3.  L'**API** écrit la métrique dans **HBase**.
4.  Pour les fichiers lourds, le client envoie l'artefact à l'API qui le stocke directement sur **HDFS**.

## 5. Intégration avec le Code Existant (`wrapper-mlflow`)
Le projet s'appuiera sur les composants suivants de `wrapper-mlflow` :
*   `RestApiConfig` : Pour la définition des routes et la sérialisation Protobuf.
*   `Handlers` (`RunHandler`, `ExperimentHandler`, etc.) : À adapter pour utiliser des DAO pointant vers HBase/HDFS au lieu des clients proxy actuels.
*   `ProtoUtils` : Pour la manipulation des objets MLFlow.

## 6. Roadmap Technique
1.  **Phase 1 :** Implémentation des DAO HBase pour le tracking (Expériences/Runs).
2.  **Phase 2 :** Implémentation du stockage d'artefacts sur HDFS.
3.  **Phase 3 :** Intégration Zookeeper pour le service discovery.
4.  **Phase 4 :** Création de la topologie Knox (Terminé) et du package de déploiement YARN.
