# Utilisation de MLFlow avec Kirka via Apache Knox

Ce document explique comment configurer un client MLFlow (principalement en Python) pour interagir avec Kirka en passant par la passerelle Apache Knox.

## 1. Prérequis

*   Un client MLFlow installé (`pip install mlflow`).
*   L'adresse de votre gateway Knox (ex: `https://knox-gateway:8443`).
*   Le nom de la topologie Knox configurée pour Kirka (généralement `sandbox` ou une topologie dédiée).
*   Vos identifiants d'authentification pour Knox.

## 2. Configuration du Tracking URI

Le client MLFlow doit pointer vers l'URL exposée par Knox. Le chemin configuré dans Knox pour Kirka est `/kirka/api/2.0/mlflow/`.

L'URL complète à utiliser comme `MLFLOW_TRACKING_URI` est :
`https://<knox-host>:<knox-port>/gateway/<topology-name>/kirka`

### Exemple en Python

```python
import mlflow
import os

# Configuration de l'URI de tracking via Knox
knox_uri = "https://knox-gateway:8443/gateway/sandbox/kirka"
mlflow.set_tracking_uri(knox_uri)

# Si Knox requiert une authentification (Basic Auth)
# Vous pouvez configurer les variables d'environnement suivantes :
os.environ['MLFLOW_TRACKING_USERNAME'] = 'votre_utilisateur'
os.environ['MLFLOW_TRACKING_PASSWORD'] = 'votre_mot_de_passe'

# Optionnel : Ignorer la vérification SSL si certificat auto-signé
os.environ['MLFLOW_TRACKING_INSECURE_TLS'] = 'true'

# Utilisation standard de MLFlow
with mlflow.start_run():
    mlflow.log_param("param1", 5)
    mlflow.log_metric("foo", 1.0)
    mlflow.log_metric("foo", 2.0)
    mlflow.log_metric("foo", 3.0)
```

## 3. Configuration via variables d'environnement

Vous pouvez également configurer votre environnement sans modifier le code :

```bash
export MLFLOW_TRACKING_URI=https://knox-gateway:8443/gateway/sandbox/kirka
export MLFLOW_TRACKING_USERNAME=admin
export MLFLOW_TRACKING_PASSWORD=admin-password
export MLFLOW_TRACKING_INSECURE_TLS=true
```

## 4. Fonctionnement de l'intermédiation Knox

1.  **Authentification** : Knox intercepte la requête du client MLFlow et vérifie les informations d'authentification (LDAP, AD, etc.).
2.  **Routage** : Grâce aux règles de réécriture dans `knox/rewrite.xml`, Knox transforme l'URL entrante :
    `.../gateway/sandbox/kirka/api/2.0/mlflow/runs/create`
    vers l'URL interne de Kirka :
    `http://kirka-backend:8080/api/2.0/mlflow/runs/create`
3.  **Réponse** : Kirka traite la demande (HBase/HDFS) et renvoie la réponse via Knox au client.

## 5. Cas des Artefacts

Kirka gère le stockage des artefacts sur HDFS. Lors de la création d'un run, Kirka renvoie une `artifact_uri` qui pointe également vers l'API de Kirka. Le client MLFlow utilisera donc la même configuration Knox pour uploader ou downloader les artefacts.
