## 🚀 Real-Time Crypto Intelligence Pipeline

Conception et implémentation d’une plateforme Data Engineering temps réel combinant ingestion streaming et traitement batch dans une architecture distribuée orientée événements.

Le pipeline permet de collecter, normaliser, sérialiser et distribuer des données financières et macroéconomiques vers Kafka pour une consommation analytique en temps réel.

Architecture du pipeline

Le système repose sur deux *types de flux de données* :

### Streaming temps réel

Nous utilisons Apache Kafka comme backbone de streaming pour ingérer des données instantanées provenant de :

- NewsAPI → actualités financières
- Binance WebSocket → transactions crypto en direct

Ces sources produisent des événements en continu qui sont :
- normalisés en Python
- sérialisés en Avro
- validés via Schema Registry
- publiés dans Kafka

### Batch orchestré (données macroéconomiques)

Les données de FRED API ne sont pas mises à jour en temps réel.
Nous avons donc mis en place un pipeline batch orchestré avec Apache Airflow.

Fonctionnement :

- Airflow déclenche un DAG mensuel
- Les indicateurs économiques sont récupérés
- Les données sont transformées et sérialisées en Avro
- Les messages sont publiés dans Kafka

**Pipeline technique :**
Python → Avro Serializer → Schema Registry → Kafka

## Stack technologique

- **Apache Kafka** → transport des événements
- **Confluent Cloud** → infrastructure Kafka managée
- **Apache Airflow** → orchestration batch
- **Apache Spark Structured Streaming** → traitement distribué
- **Python** → ingestion et transformation
- **Avro** → format de sérialisation
- **Schema Registry** → gouvernance des schémas

## Structure du projet
```
cryptopipeline/
│
├── fred_airflow_docker/
│   ├── dags/
│   ├── logs/
│   ├── Dockerfile
│   ├── docker-compose.yml
│   └── requirements.txt
│
├── news_avro_producer.py
├── trades_avro_producer.py
```
## Installation et Configuration
### Cloner le projet

```
git clone https://github.com/safiyadaoudi01/Real-Time_Crypto_Intelligence_Pipeline.git
cd Real-Time_Crypto_Intelligence_Pipeline
```
### Prérequis

Assure-toi d’avoir installé :
- Docker
- Docker Compose
- Python 3.8+
- Un compte Confluent Cloud avec :
    - cluster Kafka
    - Schema Registry activé
    - API Key + Secret

### Lancer Apache Airflow avec Docker
Le pipeline batch FRED est orchestré via Apache Airflow exécuté dans un conteneur Docker.

Se placer dans le dossier Airflow :
```
cd cryptopipeline/fred_airflow_docker
```
Construire l’image :
```
docker compose build
```
Démarrer le service :
```
docker compose up
```
### Accès à l’interface Airflow

Une fois le conteneur démarré, Airflow initialise automatiquement la base de données et crée un utilisateur admin.

👉 Le username et le password sont affichés directement dans le terminal après le lancement du conteneur.

Exemple dans les logs :
```
Airflow standalone is ready
Login with username: admin
Password: xxxxxxxxxxxx
```
Ouvrir ensuite dans le navigateur :
```
http://localhost:8080
```
### Configuration Kafka
Le pipeline envoie les données vers Apache Kafka via Confluent Cloud.

Dans les producteurs Python, renseigner :

- bootstrap.servers
- SASL username
- SASL password
- Schema Registry URL
- Schema Registry API key

Ces paramètres proviennent du dashboard Confluent Cloud.

### Démarrage du pipeline
**✔ Pipeline batch (FRED)**
Dans l’interface Airflow :
- Activer le DAG fred_dag
- Déclencher manuellement ou attendre l’exécution planifiée mensuelle
- Les données macroéconomiques seront envoyées vers Kafka

**✔ Producteurs streaming temps réel**
Depuis la racine du projet :
```
python news_avro_producer.py
python trades_avro_producer.py
```

Ces scripts :
- récupèrent les données en temps réel
- sérialisent en Avro
- envoient vers Kafka avec validation Schema Registry

### Vérification du pipeline
Dans Confluent Cloud :
- Ouvrir le topic Kafka
- Vérifier la réception des messages
- Contrôler le schéma Avro enregistré
