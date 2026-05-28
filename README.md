# Pipeline de Data Engineering pour les données CAN Bus sur GCP

## Table des matières

1. [Présentation du projet](#1-présentation-du-projet)
2. [Contexte et motivation](#2-contexte-et-motivation)
3. [Dataset : X-CANIDS](#3-dataset--x-canids)
4. [Architecture globale](#4-architecture-globale)
5. [Architecture Medallion](#5-architecture-medallion)
6. [Organisation Data Mesh](#6-organisation-data-mesh)
7. [Schéma BigQuery — Toutes les tables](#7-schéma-bigquery--toutes-les-tables)
8. [Structure GCS](#8-structure-gcs)
9. [Documentation des scripts](#9-documentation-des-scripts)
10. [Orchestration](#10-orchestration)
11. [Monitoring et gouvernance](#11-monitoring-et-gouvernance)
12. [Infrastructure — Services GCP](#12-infrastructure--services-gcp)
13. [Guide de déploiement](#13-guide-de-déploiement)
14. [Exécution du pipeline](#14-exécution-du-pipeline)
15. [Référence de configuration](#15-référence-de-configuration)


---


## 1. Présentation du projet

**Titre du projet :** Mise en place d'un pipeline de Data Engineering scalable et traçable pour la préparation et l'exposition des données de trafic CAN Bus à destination des data analysts et data scientists

**Contexte :** Stage de fin d'études (PFE) au sein d'ALTEN Delivery Center Maroc — département IT Factory Enterprise Services.

**Périmètre :** Ce projet couvre uniquement la partie Data Engineering — ingestion, transformation, stockage, orchestration, contrôle qualité et exposition. Il ne couvre pas le développement de modèles de détection d'intrusion.

**Type de pipeline :** Batch ELT (Extract, Load, Transform)

**Déclenchement :** Quotidien à 02h00 UTC via Cloud Scheduler

**Projet GCP :** `project-e6de9b55-41d5-4f13-ae0`

**Région :** `europe-southwest1`


---


## 2. Contexte et motivation

Le secteur automobile connaît une évolution importante avec l'intégration croissante des systèmes embarqués et des véhicules connectés. Le réseau CAN Bus constitue l'épine dorsale de communication interne des véhicules modernes, permettant l'échange de données en temps réel entre les calculateurs électroniques. Cette connectivité accrue expose également les systèmes automobiles à des menaces de cybersécurité telles que les attaques fuzzy, les attaques replay, les attaques masquerade et les injections de messages.

Avant ce projet, l'équipe travaillait directement sur des fichiers Parquet bruts téléchargés localement depuis IEEE Dataport sans aucun pipeline standardisé. Les analyses étaient réalisées dans Power BI sans modélisation de données, et les modèles de Machine Learning étaient entraînés directement sur des fichiers Parquet non transformés dans des notebooks Jupyter. Il n'existait aucune infrastructure partagée, aucune traçabilité, aucun contrôle qualité et aucune garantie de reproductibilité.

Ce projet répond à ces limitations en construisant un pipeline de Data Engineering automatisé, scalable et traçable sur Google Cloud Platform, déployable en production.


---


## 3. Dataset : X-CANIDS

**Source :** IEEE Dataport

**Description :** Captures de trafic CAN Bus labellisées couvrant six scénarios.

**Types d'attaques :**

| Code   | Nom de l'attaque  | Sévérité | Description |
|--------|-------------------|----------|-------------|
| benign | Trafic normal     | AUCUNE   | Trafic CAN sans injection malveillante |
| fuzz   | Fuzzy Attack      | HAUTE    | Injection de trames avec payload aléatoire à haute fréquence |
| fabr   | Fabrication       | CRITIQUE | Création de trames avec des identifiants absents du profil normal |
| masq   | Masquerade        | CRITIQUE | Usurpation d'identité d'un ECU légitime par un dispositif malveillant |
| susp   | Suspension        | MOYENNE  | Manipulation des signaux du système de suspension |
| repl   | Replay            | HAUTE    | Rejeu de trames CAN légitimes préalablement capturées |

**Représentations :**

- **RAW** : trames CAN brutes — `timestamp`, `arbitration_id`, `dlc`, `data` (payload hexadécimal), `label`
- **SIGNAL** : valeurs physiques décodées — vitesse, régime moteur, pression de freinage, angle de direction, ... + `label`

**Note importante sur le timestamp :** Le champ timestamp dans X-CANIDS est un timedelta relatif (ex. `0 days 00:24:41.684751`) représentant le temps écoulé depuis le début de la session de capture, et non une heure absolue. Il est normalisé en `elapsed_seconds` dans la couche Silver.


---


## 4. Architecture globale

```

```


---


## 5. Architecture Medallion

Le pipeline est organisé en trois couches de qualité progressive suivant le paradigme de l'Architecture Medallion.

### Couche Bronze

Données brutes ingérées depuis la zone d'atterrissage GCS dans BigQuery sans aucune modification. Cette couche garantit :
- La traçabilité complète des données sources
- La rejouabilité totale du pipeline depuis le début
- La préservation du schéma original y compris toutes les anomalies (colonnes dupliquées, schémas hétérogènes)

Les tables suivent le schéma de nommage : `{représentation}_valid_{type_attaque}`

### Couche Silver

Données nettoyées, dédupliquées et enrichies. Transformations appliquées :
- **Déduplication** par `event_id` (hash MD5 des champs clés)
- **Normalisation des types** (LongType pour timestamp_us, IntegerType pour label, DoubleType pour les signaux)
- **Calcul d'elapsed_seconds** : `timestamp - min(timestamp)` par groupe `(source_file, dump_id)`
- **Booléen is_attack** dérivé du label
- **Gestion des colonnes dupliquées** via `deduplicate_columns()` pour les fichiers SIGNAL
- **Gestion du label absent** : label=0 par défaut si absent

Les tables suivent le schéma de nommage : `{représentation}_clean_{type_attaque}`

### Couche Gold

Données agrégées et structurées prêtes pour la consommation finale. Deux représentations indépendantes sont produites, une par domaine.

**Domaine Analytics** — Star Schema avec fenêtres temporelles de 5 minutes :
- `fact_window_5min_wide` : table de faits centrale avec moyennes des signaux et métriques d'attaque par fenêtre de 5 minutes
- `dim_attack` : dimension type d'attaque
- `dim_capture` : dimension fichier source
- `dim_window` : dimension fenêtre temporelle avec phase de capture

**Domaine ML** — One Big Table :
- `signal_big_table` : tous les signaux physiques décodés + label + attack_type + ml_split déterministe


---


## 6. Organisation Data Mesh

Le pipeline suit le paradigme Data Mesh avec deux domaines indépendants.

### Domaine Analytics

**Responsable :** Data Analyst

**Consommation :** Tableaux de bord Power BI

**Objectif :** Analyse comportementale des intrusions CAN Bus — évolution du taux d'attaque, impact sur les signaux par type d'attaque, analyse des patterns temporels

**Produit de données :** Tables Gold Star Schema dans le dataset `can_ids_bqnative_gold_analytics`

### Domaine ML

**Responsable :** Data Scientist

**Consommation :** Entraînement de modèles Vertex AI

**Objectif :** Entraînement de modèles de classification binaire (benign vs attaque) et multiclasse (6 types d'attaques)

**Produit de données :** One Big Table dans le dataset `can_ids_bqnative_gold_ml`

Chaque domaine possède ses propres datasets BigQuery, ses propres scripts Spark et ses propres contrôles qualité. Les domaines sont totalement indépendants — une modification du pipeline Analytics n'impacte pas le pipeline ML.


---


## 7. Schéma BigQuery — Toutes les tables

### Datasets

| Dataset | Couche | Domaine | Description |
|---------|--------|---------|-------------|
| `can_ids_bqnative_bronze_analytics` | Bronze | Analytics | Données brutes ingérées pour le domaine Analytics |
| `can_ids_bqnative_bronze_ml` | Bronze | ML | Données brutes ingérées pour le domaine ML |
| `can_ids_bqnative_silver_analytics` | Silver | Analytics | Données nettoyées pour le domaine Analytics |
| `can_ids_bqnative_silver_ml` | Silver | ML | Données nettoyées pour le domaine ML |
| `can_ids_bqnative_gold_analytics` | Gold | Analytics | Star Schema pour Power BI |
| `can_ids_bqnative_gold_ml` | Gold | ML | One Big Table pour Vertex AI |
| `can_ids_bqnative_audit` | Audit | Tous | Traçabilité et contrôles qualité du pipeline |

---

### Tables Bronze

**Schéma de nommage :** `{base}_{type_attaque}` où base = `raw_valid` ou `signal_valid`

**Exemples :** `raw_valid_benign`, `signal_valid_fuzz`, `raw_valid_masq`

**Schéma Bronze RAW :**

| Colonne | Type | Description |
|---------|------|-------------|
| timestamp_us | INT64 | Timestamp en microsecondes |
| arbitration_id | INT64 | Identifiant de la trame CAN |
| dlc | INT64 | Data Length Code (0-8 octets) |
| data | STRING | Payload hexadécimal |
| label | INT64 | 0=benign, 1=attaque |
| source_file | STRING | Nom du fichier Parquet source |
| domain | STRING | Domaine (analytics ou ml) |
| representation | STRING | raw ou signal |
| attack_type | STRING | benign, fuzz, fabr, masq, susp, repl |
| dump_id | STRING | Identifiant de la session de capture |
| dataset_type | STRING | xcanids |
| ingested_at | TIMESTAMP | Horodatage de l'ingestion |

**Schéma Bronze SIGNAL :**

| Colonne | Type | Description |
|---------|------|-------------|
| timestamp | FLOAT64 | Timestamp relatif (timedelta) |
| label | INT64 | 0=benign, 1=attaque |
| [colonnes signal] | FLOAT64 | Valeurs physiques décodées (vitesse, régime, ...) |
| source_file | STRING | Nom du fichier Parquet source |
| domain | STRING | Domaine (analytics ou ml) |
| representation | STRING | raw ou signal |
| attack_type | STRING | Type d'attaque |
| dump_id | STRING | Identifiant de la session de capture |
| ingested_at | TIMESTAMP | Horodatage de l'ingestion |

---

### Tables Silver

**Schéma de nommage :** `{base}_{type_attaque}` où base = `raw_clean` ou `signal_clean`

**Colonnes ajoutées par rapport au Bronze :**

| Colonne | Type | Description |
|---------|------|-------------|
| event_id | STRING | Clé de déduplication MD5 |
| elapsed_seconds | FLOAT64 | Temps écoulé normalisé depuis le début de la capture |
| is_attack | BOOLEAN | Dérivé du label (label == 1) |

---

### Tables Gold Analytics

**fact_window_5min_wide**

| Colonne | Type | Description |
|---------|------|-------------|
| capture_id | STRING | MD5 de (source_file, dump_id) |
| window_id | STRING | MD5 de (source_file, dump_id, window_5min) |
| attack_id | STRING | MD5 de (attack_type, attack_parameter, target_aid) |
| source_file | STRING | Fichier source |
| dump_id | STRING | Session de capture |
| window_5min | INT64 | Index de la fenêtre de 5 minutes |
| window_start_s | FLOAT64 | Début de la fenêtre en secondes |
| window_end_s | FLOAT64 | Fin de la fenêtre en secondes |
| attack_type | STRING | Type d'attaque |
| total_snapshot_count | INT64 | Nombre total de trames dans la fenêtre |
| attack_snapshot_count | INT64 | Nombre de trames d'attaque dans la fenêtre |
| normal_snapshot_count | INT64 | Nombre de trames normales dans la fenêtre |
| attack_rate_pct | FLOAT64 | Pourcentage de trames d'attaque |
| is_attack_window | BOOLEAN | Vrai si la fenêtre contient des attaques |
| avg_{signal} | FLOAT64 | Moyenne par colonne signal |

**dim_attack**

| Colonne | Type | Description |
|---------|------|-------------|
| attack_id | STRING | Clé primaire |
| attack_type | STRING | Code du type d'attaque |
| attack_parameter | STRING | Paramètre de l'attaque |
| target_aid | STRING | Identifiant CAN ciblé |
| fuzz_rate | INT64 | Taux de fuzzing (fuzz uniquement) |
| replay_start_sec | FLOAT64 | Début du replay (repl uniquement) |
| replay_end_sec | FLOAT64 | Fin du replay (repl uniquement) |
| severity | STRING | LOW / MEDIUM / HIGH / CRITICAL |

**dim_capture**

| Colonne | Type | Description |
|---------|------|-------------|
| capture_id | STRING | Clé primaire |
| source_file | STRING | Nom du fichier Parquet source |
| dump_id | STRING | Identifiant de la session de capture |
| dataset_type | STRING | benign ou intrusion |
| representation | STRING | signal |
| duration_seconds | FLOAT64 | Durée de la capture en secondes |
| row_count | INT64 | Nombre total de trames |
| label_0_count | INT64 | Nombre de trames normales |
| label_1_count | INT64 | Nombre de trames d'attaque |

**dim_window**

| Colonne | Type | Description |
|---------|------|-------------|
| window_id | STRING | Clé primaire |
| window_5min | INT64 | Index de la fenêtre |
| window_start_s | FLOAT64 | Début de la fenêtre en secondes |
| window_end_s | FLOAT64 | Fin de la fenêtre en secondes |
| capture_phase | STRING | pre_attack / during_attack / post_attack / no_attack |

---

### Table Gold ML

**signal_big_table**

| Colonne | Type | Description |
|---------|------|-------------|
| event_id | STRING | Identifiant unique de la trame (MD5) |
| source_file | STRING | Nom du fichier Parquet source |
| attack_type | STRING | Type d'attaque |
| label | INT64 | 0=benign, 1=attaque |
| is_attack | BOOLEAN | Dérivé du label |
| elapsed_seconds | FLOAT64 | Temps écoulé normalisé |
| ml_split | STRING | train / validation / test (déterministe) |
| [colonnes signal] | FLOAT64 | Ensemble des valeurs physiques décodées |

**Calcul du ml_split :**
```
split_bucket = abs(hash(event_id)) % 100
0-69   -> train
70-84  -> validation
85-99  -> test
```

---

### Tables Audit

**file_processing_status**

| Colonne | Type | Description |
|---------|------|-------------|
| layer | STRING | bronze / silver / gold |
| domain | STRING | analytics / ml |
| representation | STRING | raw / signal |
| source_file | STRING | Chemin du fichier traité |
| status | STRING | SUCCESS / FAILED |
| rows_written | INT64 | Nombre de lignes écrites |
| error_message | STRING | Détail de l'erreur si échec |
| processed_at | TIMESTAMP | Horodatage du traitement |

**data_quality_results**

| Colonne | Type | Description |
|---------|------|-------------|
| check_id | STRING | Identifiant unique du contrôle |
| table_name | STRING | Chemin complet de la table vérifiée |
| check_name | STRING | Type de contrôle |
| status | STRING | PASS / WARN / FAIL |
| row_count | INT64 | Nombre de lignes au moment du contrôle |
| error_message | STRING | Détail de l'erreur si échec |
| checked_at | TIMESTAMP | Horodatage du contrôle |


---


## 8. Structure GCS

```
gs://can-ids-data-bqnative/
|
|-- landing/
|   |-- raw/
|   |   |-- benign_001.parquet
|   |   |-- fuzz_001.parquet
|   |   |-- fabr_001.parquet
|   |   |-- masq_001.parquet
|   |   |-- susp_001.parquet
|   |   `-- repl_001.parquet
|   `-- signal/
|       |-- benign_001.parquet
|       |-- fuzz_001.parquet
|       `-- ...
|
|-- spark_jobs/
|   |-- common_bq.py
|   |-- 00_create_bigquery_native_schema.py
|   |-- 01_bronze_analytics_ingestion.py
|   |-- 01_bronze_ml_ingestion.py
|   |-- 02_silver_analytics_clean.py
|   |-- 02_silver_ml_clean.py
|   |-- 03_gold_analytics_window_5min_wide.py
|   |-- 03_gold_ml_signal_big_table.py
|   `-- 05_quality_checks_datamesh.py
|
|-- config/
|   `-- config-native-bigquery.yml
|
`-- google-cloud-dataproc-metainfo/    <-- Logs Dataproc (généré automatiquement)
```

**Convention de nommage des fichiers :** Chaque nom de fichier Parquet doit contenir le mot-clé du type d'attaque (`benign`, `fuzz`, `fabr`, `masq`, `susp`, `repl`) pour que la fonction `detect_attack()` dans `common_bq.py` puisse détecter automatiquement le type d'attaque.


---


## 9. Documentation des scripts

### common_bq.py

Bibliothèque partagée importée par tous les scripts Spark. Contient tous les utilitaires réutilisables.

**Fonctions principales :**

| Fonction | Description |
|----------|-------------|
| `load_yaml(path)` | Charger la configuration YAML depuis un chemin local ou une URI GCS |
| `create_spark(name)` | Créer une SparkSession avec les paramètres optimisés |
| `bq_table(cfg, dataset_key, table_name)` | Construire le chemin BigQuery complet d'une table |
| `table_exists(cfg, dataset_key, table_name)` | Vérifier l'existence d'une table BigQuery |
| `read_bq(spark, cfg, dataset_key, table_name)` | Lire une table BigQuery dans un DataFrame Spark |
| `read_bq_by_attack(spark, cfg, dataset_key, base_table)` | Lire et unifier toutes les tables d'attaques (unionByName allowMissingColumns) |
| `write_bq(df, cfg, dataset_key, table_name, mode)` | Écrire un DataFrame Spark dans BigQuery (writeMethod=direct) |
| `processed_sources(cfg, layer, domain, representation)` | Retourner l'ensemble des fichiers sources déjà traités depuis la table d'audit |
| `write_audit_status(cfg, layer, domain, representation, source_file, status, ...)` | Journaliser le statut de traitement dans la table d'audit |
| `delete_source_rows(cfg, dataset_key, table_name, source_file)` | Supprimer les lignes d'un fichier source (idempotence) |
| `delete_sources_rows(cfg, dataset_key, table_name, sources)` | Supprimer les lignes de plusieurs fichiers sources |
| `sanitize_bq_column_name(name)` | Normaliser un nom de colonne pour la compatibilité BigQuery |
| `make_unique_columns(cols)` | Rendre les noms de colonnes uniques (déduplication insensible à la casse) |
| `deduplicate_columns(df)` | Appliquer la déduplication des colonnes à un DataFrame Spark |
| `detect_attack(source_file)` | Détecter le type d'attaque à partir du nom du fichier |
| `list_parquet_files(prefix_uri)` | Lister tous les fichiers Parquet sous un préfixe GCS |
| `read_parquet_resilient(spark, gcs_uri)` | Lire un Parquet avec repli sur PyArrow en cas de colonnes dupliquées |
| `add_metadata(df, source_file, rep, domain)` | Ajouter les colonnes de métadonnées du pipeline au DataFrame |
| `normalize_raw(df)` | Caster les colonnes RAW vers les types corrects |
| `normalize_signal(df)` | Caster les colonnes SIGNAL vers les types corrects |

---

### 00_create_bigquery_native_schema.py

**Couche :** Configuration initiale (exécution unique)

**Objectif :** Créer les 7 datasets BigQuery et les 2 tables d'audit.

**Usage :**
```bash
python 00_create_bigquery_native_schema.py \
  --config_path gs://can-ids-data-bqnative/config/config-native-bigquery.yml
```

**Ce que fait ce script :**
- Crée tous les datasets définis dans le fichier de configuration YAML
- Crée la table `audit.file_processing_status`
- Crée la table `audit.data_quality_results`
- Utilise `exists_ok=True` — peut être exécuté plusieurs fois sans risque

---

### 01_bronze_analytics_ingestion.py

**Couche :** Bronze

**Domaine :** Analytics

**Objectif :** Ingestion incrémentale des fichiers Parquet RAW et SIGNAL dans les tables BigQuery Bronze Analytics.

**Usage :**
```bash
python 01_bronze_analytics_ingestion.py \
  --config_path gs://can-ids-data-bqnative/config/config-native-bigquery.yml
```

**Ce que fait ce script :**
1. Liste tous les fichiers Parquet dans `landing/raw/` et `landing/signal/`
2. Filtre les fichiers déjà traités via la vérification d'audit `processed_sources()`
3. Pour chaque nouveau fichier : lecture avec `read_parquet_resilient()`, normalisation, ajout des métadonnées
4. Écriture dans `bronze_analytics.raw_valid_{attaque}` ou `bronze_analytics.signal_valid_{attaque}`
5. Journalise chaque fichier comme SUCCESS ou FAILED dans `audit.file_processing_status`

---

### 01_bronze_ml_ingestion.py

**Couche :** Bronze

**Domaine :** ML

**Objectif :** Identique à `01_bronze_analytics_ingestion.py` mais pour le domaine ML.

**Écrit dans :** `bronze_ml.raw_valid_{attaque}` et `bronze_ml.signal_valid_{attaque}`

---

### 02_silver_analytics_clean.py

**Couche :** Silver

**Domaine :** Analytics

**Objectif :** Nettoyer, dédupliquer et enrichir les données Bronze Analytics.

**Usage :**
```bash
python 02_silver_analytics_clean.py \
  --config_path gs://can-ids-data-bqnative/config/config-native-bigquery.yml
```

**Ce que fait ce script :**
1. Lit toutes les tables Bronze Analytics (union de tous les types d'attaques) via `read_bq_by_attack()`
2. Filtre les nouveaux fichiers sources non encore présents dans l'audit Silver
3. Applique `transform_raw()` ou `transform_signal()` :
   - Cast des types
   - Calcul d'`elapsed_seconds` = (timestamp - timestamp_min) par (source_file, dump_id)
   - Calcul d'`event_id` = hash MD5 des champs clés
   - Ajout du booléen `is_attack`
   - Déduplication par `event_id`
4. Dispatch de la sortie par attack_type : écriture dans `silver_analytics.raw_clean_{attaque}` ou `signal_clean_{attaque}`
5. Journalise le statut d'audit par fichier source

---

### 02_silver_ml_clean.py

**Couche :** Silver

**Domaine :** ML

**Objectif :** Nettoyer et enrichir les données Bronze ML. Supporte deux modes d'exécution.

**Usage :**
```bash
# Mode rapide — toutes les attaques en un seul job Spark
python 02_silver_ml_clean.py --config_path ... --representation all --attack_type all

# Mode reprise partielle — un seul type d'attaque
python 02_silver_ml_clean.py --config_path ... --representation signal --attack_type fuzz
```

**Modes d'exécution :**

| Mode | Commande | Description |
|------|---------|-------------|
| Rapide (tout) | `--attack_type all` | Un seul job Spark, lit toutes les tables Bronze ML, dispatch par type d'attaque. Même performance que silver_analytics. |
| Reprise partielle | `--attack_type fuzz` | Un seul type d'attaque. Utilisé pour relancer une étape échouée sans retraiter l'ensemble. |

**Écrit dans :** `silver_ml.raw_clean_{attaque}` et `silver_ml.signal_clean_{attaque}`

---

### 03_gold_analytics_window_5min_wide.py

**Couche :** Gold

**Domaine :** Analytics

**Objectif :** Construire le Star Schema Gold Analytics à partir des données Silver Analytics SIGNAL.

**Usage :**
```bash
python 03_gold_analytics_window_5min_wide.py --config_path ... --attack_type all
```

**Ce que fait ce script :**
1. Lit les tables `silver_analytics.signal_clean_{attaque}`
2. Applique `normalize_time_in_gold()` : normalise le timestamp en secondes (gère les unités µs/ms/s)
3. Calcule les clés de fenêtre : `window_5min = floor(elapsed_seconds / 300)`
4. Calcule les identifiants de dimension : `capture_id`, `window_id`, `attack_id` (hashes MD5)
5. Construit `fact_window_5min_wide` via agrégation groupBy (moyenne par signal, comptages d'attaques)
6. Construit `dim_attack`, `dim_capture`, `dim_window`
7. Supprime les lignes existantes par capture_id/window_id/attack_id avant écriture (idempotence)
8. Écrit les 4 tables dans `gold_analytics`

---

### 03_gold_ml_signal_big_table.py

**Couche :** Gold

**Domaine :** ML

**Objectif :** Construire la One Big Table Gold ML à partir des données Silver ML SIGNAL.

**Usage :**
```bash
python 03_gold_ml_signal_big_table.py --config_path ...
```

**Ce que fait ce script :**
1. Lit et unifie toutes les tables `silver_ml.signal_clean_{attaque}` via `unionByName(allowMissingColumns=True)`
2. Filtre les nouveaux fichiers sources non encore présents dans l'audit Gold ML
3. Applique `add_ml_split()` :
   - `split_bucket = abs(hash(event_id)) % 100`
   - 0-69 -> train, 70-84 -> validation, 85-99 -> test
4. Supprime les lignes existantes pour les nouvelles sources (idempotence)
5. Écrit dans `gold_ml.signal_big_table`

---

### 05_quality_checks_datamesh.py

**Couche :** Audit

**Domaine :** Tous

**Objectif :** Contrôles qualité automatisés sur toutes les tables Gold après chaque exécution du pipeline.

**Usage :**
```bash
python 05_quality_checks_datamesh.py --config_path ...
```

**Contrôles effectués par table :**
- La table existe (404 -> WARN, pas FAIL)
- La table n'est pas vide (0 lignes -> WARN)
- Les colonnes requises sont présentes (colonne manquante -> FAIL)

**Tables contrôlées :**

| Table | Colonnes requises |
|-------|------------------|
| `gold_ml.signal_big_table` | event_id, source_file, attack_type, label, ml_split |
| `gold_analytics.fact_window_5min_wide` | capture_id, window_id, attack_id, source_file, attack_type |
| `gold_analytics.dim_attack` | attack_id, attack_type |
| `gold_analytics.dim_capture` | capture_id, source_file |
| `gold_analytics.dim_window` | window_id, window_5min |

**Valeurs de statut :** PASS / WARN / FAIL

**WARN** n'arrête pas le pipeline (table pas encore créée). **FAIL** lève une exception et arrête le pipeline.

---

### orchestrate_pipeline.py

**Objectif :** Orchestrateur Cloud Run Job. Gère le cycle de vie du cluster Dataproc et soumet les jobs Spark dans le bon ordre.

**Usage :**
```bash
# Pipeline complet
python orchestrate_pipeline.py --run-pipeline

# Étape unique
python orchestrate_pipeline.py --run-pipeline --step gold-ml
```

**Étapes du pipeline dans l'ordre :**

| Étape | Script |
|-------|--------|
| bronze-analytics | 01_bronze_analytics_ingestion.py |
| silver-analytics | 02_silver_analytics_clean.py |
| gold-analytics | 03_gold_analytics_window_5min_wide.py |
| bronze-ml | 01_bronze_ml_ingestion.py |
| silver-ml | 02_silver_ml_clean.py |
| gold-ml | 03_gold_ml_signal_big_table.py |
| quality-checks | 05_quality_checks_datamesh.py |

**Variables d'environnement :**

| Variable | Valeur par défaut | Description |
|----------|------------------|-------------|
| PROJECT_ID | project-e6de9b55-41d5-4f13-ae0 | Identifiant du projet GCP |
| REGION | europe-southwest1 | Région GCP |
| CLUSTER_NAME | can-ids-spark-cluster | Nom du cluster Dataproc |
| BUCKET_NAME | can-ids-data-bqnative | Bucket GCS |
| DELETE_CLUSTER_AT_END | true | Supprimer le cluster après l'exécution |
| NUM_WORKERS | 2 | Nombre de workers Dataproc (min 2) |
| RUN_BRONZE_ANALYTICS | true | Activer/désactiver chaque étape |
| RUN_SILVER_ANALYTICS | true | |
| RUN_GOLD_ANALYTICS | true | |
| RUN_BRONZE_ML | true | |
| RUN_SILVER_ML | true | |
| RUN_GOLD_ML | true | |
| RUN_QUALITY_CHECKS | true | |
| ATTACK_TYPES | benign,fabr,fuzz,masq,repl,susp | Types d'attaques à traiter |

---

### setup_scheduler.py

**Objectif :** Créer, mettre à jour ou supprimer le job Cloud Scheduler qui déclenche le pipeline quotidiennement.

**Usage :**
```bash
python setup_scheduler.py --deploy        # Créer ou mettre à jour
python setup_scheduler.py --status        # Afficher le statut actuel
python setup_scheduler.py --trigger-now   # Déclencher immédiatement sans attendre le cron
python setup_scheduler.py --delete        # Supprimer le scheduler
```

**Configuration :**

| Paramètre | Valeur |
|-----------|--------|
| Nom du job | can-ids-daily-trigger |
| Cron | 0 2 * * * (quotidien à 02h00 UTC) |
| Fuseau horaire | UTC |
| Localisation Scheduler | europe-west1 |
| Cible | Cloud Run Job can-ids-pipeline via HTTP POST |

---

### setup_monitoring.py

**Objectif :** Créer les politiques d'alerte Cloud Monitoring, les canaux de notification et les descripteurs de métriques personnalisées.

**Usage :**
```bash
python setup_monitoring.py --deploy        # Créer les alertes et le canal email
python setup_monitoring.py --status        # Lister les alertes actives
python setup_monitoring.py --delete        # Supprimer toutes les alertes du pipeline
python setup_monitoring.py --test-metric   # Pousser des métriques de test
```

**Politiques d'alerte créées :**

| Alerte | Condition de déclenchement |
|--------|---------------------------|
| Échec du pipeline | Métrique custom pipeline_failure > 0 |
| Échec d'une étape | Métrique custom step_failure > 0 |
| Durée du pipeline > 2h | Métrique custom pipeline_duration_seconds > 7200 |
| Tâche Cloud Run Job échouée | run.googleapis.com/job/completed_task_attempt_count avec result=failed |

**Métriques personnalisées :**

| Métrique | Description |
|----------|-------------|
| `custom.googleapis.com/can_ids/pipeline_success` | Pipeline terminé avec succès |
| `custom.googleapis.com/can_ids/pipeline_failure` | Pipeline en échec |
| `custom.googleapis.com/can_ids/pipeline_duration_seconds` | Durée totale du pipeline |
| `custom.googleapis.com/can_ids/step_success` | Étape individuelle terminée avec succès |
| `custom.googleapis.com/can_ids/step_failure` | Étape individuelle en échec |
| `custom.googleapis.com/can_ids/step_duration_seconds` | Durée d'une étape individuelle |


---


## 10. Orchestration

Le pipeline est orchestré par un container Cloud Run Job qui gère le cycle de vie complet du cluster Dataproc.

```

```

**Configuration du cluster :**
- Master : 1x e2-standard-4 (4 vCPU, 16 Go RAM)
- Workers : 2x e2-standard-4 (minimum requis par Dataproc)
- Image : 2.1-debian11
- Éphémère : créé au démarrage du pipeline, supprimé à la fin


---


## 11. Monitoring et gouvernance

### Traçabilité des traitements

Chaque fichier source traité par le pipeline est journalisé dans `audit.file_processing_status` avec :
- La couche, le domaine, la représentation
- Le nom du fichier source
- Le statut de traitement (SUCCESS/FAILED)
- Le nombre de lignes écrites
- Le message d'erreur si échec
- L'horodatage

Cette table d'audit permet :
- **Le traitement incrémental** : les fichiers déjà marqués SUCCESS sont ignorés lors de la prochaine exécution
- **L'idempotence** : relancer le pipeline ne produit pas de doublons
- **Le débogage** : identifier quels fichiers ont échoué et pourquoi

### Contrôles qualité

Après chaque écriture en couche Gold, `05_quality_checks_datamesh.py` vérifie toutes les tables Gold et écrit les résultats dans `audit.data_quality_results`.

### Alertes de monitoring

7 politiques d'alerte actives dans Cloud Monitoring envoient des notifications par email à l'équipe responsable en cas de :
- Échec du pipeline ou d'une étape
- Durée du pipeline dépassant 2 heures
- Échec d'une tâche Cloud Run Job


---


## 12. Infrastructure — Services GCP

| Service | Nom de la ressource | Rôle |
|---------|-------------------|------|
| Cloud Storage | can-ids-data-bqnative | Data Lake — zone d'atterrissage + scripts + configuration |
| BigQuery | 7 datasets | Data Warehouse — Bronze/Silver/Gold/Audit |
| Dataproc | can-ids-spark-cluster | Cluster Spark éphémère pour le compute |
| Cloud Run Jobs | can-ids-pipeline | Orchestrateur du pipeline sans serveur |
| Cloud Scheduler | can-ids-daily-trigger | Déclenchement cron quotidien (02h00 UTC) |
| Artifact Registry | can-ids-containers | Registre des images Docker |
| Cloud Monitoring | — | 7 politiques d'alerte + 6 métriques personnalisées |
| Cloud Build | — | Construction et publication de l'image Docker |

**Compte de service :** `can-ids-orchestrator-sa@project-e6de9b55-41d5-4f13-ae0.iam.gserviceaccount.com`

**Rôles IAM requis :**
- roles/bigquery.admin
- roles/bigquery.readSessionUser
- roles/dataproc.admin
- roles/dataproc.worker
- roles/storage.objectAdmin
- roles/run.developer
- roles/monitoring.metricWriter


---


## 13. Guide de déploiement

### Prérequis

```bash
# Installation du SDK Google Cloud
gcloud auth login
gcloud auth application-default login
gcloud config set project project-e6de9b55-41d5-4f13-ae0
```

### Étape 1 — Activer les APIs GCP

```bash
gcloud services enable \
  run.googleapis.com \
  dataproc.googleapis.com \
  bigquery.googleapis.com \
  cloudscheduler.googleapis.com \
  monitoring.googleapis.com \
  storage.googleapis.com \
  artifactregistry.googleapis.com
```

### Étape 2 — Créer le bucket GCS

```bash
gsutil mb -l europe-southwest1 gs://can-ids-data-bqnative
```

### Étape 3 — Uploader les fichiers

```bash
# Scripts Spark
gsutil -m cp spark_jobs/*.py gs://can-ids-data-bqnative/spark_jobs/

# Configuration
gsutil cp config-native-bigquery.yml gs://can-ids-data-bqnative/config/

# Données X-CANIDS
gsutil -m cp data/raw/*.parquet gs://can-ids-data-bqnative/landing/raw/
gsutil -m cp data/signal/*.parquet gs://can-ids-data-bqnative/landing/signal/
```

### Étape 4 — Créer le schéma BigQuery

```bash
python 00_create_bigquery_native_schema.py \
  --config_path gs://can-ids-data-bqnative/config/config-native-bigquery.yml
```

### Étape 5 — Construire et déployer l'image Docker

```bash
gcloud builds submit \
  --tag=europe-southwest1-docker.pkg.dev/project-e6de9b55-41d5-4f13-ae0/can-ids-containers/orchestrator:latest \
  --region=europe-southwest1 \
  .
```

### Étape 6 — Déployer le Cloud Run Job

```bash
gcloud run jobs deploy can-ids-pipeline \
  --image=europe-southwest1-docker.pkg.dev/project-e6de9b55-41d5-4f13-ae0/can-ids-containers/orchestrator:latest \
  --region=europe-southwest1 \
  --service-account=can-ids-orchestrator-sa@project-e6de9b55-41d5-4f13-ae0.iam.gserviceaccount.com \
  --task-timeout=10800 \
  --max-retries=1 \
  --set-env-vars="PROJECT_ID=project-e6de9b55-41d5-4f13-ae0,REGION=europe-southwest1,NUM_WORKERS=2,DELETE_CLUSTER_AT_END=true"
```

### Étape 7 — Configurer le Monitoring et le Scheduler

```bash
export NOTIFICATION_EMAIL="hiba.sekkah@exemple.com"
export SCHEDULER_LOCATION="europe-west1"

python setup_monitoring.py --deploy
python setup_scheduler.py --deploy
```


---


## 14. Exécution du pipeline

### Lancer le pipeline complet manuellement

```bash
# Via Cloud Run Job
gcloud run jobs execute can-ids-pipeline \
  --region=europe-southwest1 \
  --project=project-e6de9b55-41d5-4f13-ae0 \
  --wait

# Via le Scheduler (sans attendre le cron)
python setup_scheduler.py --trigger-now

# En local
python orchestrate_pipeline.py --run-pipeline
```

### Lancer une étape unique

```bash
gcloud run jobs execute can-ids-pipeline \
  --region=europe-southwest1 \
  --update-env-vars="RUN_BRONZE_ANALYTICS=false,RUN_SILVER_ANALYTICS=false,RUN_GOLD_ANALYTICS=false,RUN_BRONZE_ML=false,RUN_SILVER_ML=false,RUN_GOLD_ML=true,RUN_QUALITY_CHECKS=false"
```

### Surveiller l'exécution

```bash
# Voir les exécutions
gcloud run jobs executions list \
  --job=can-ids-pipeline \
  --region=europe-southwest1

# Voir les logs en temps réel
gcloud logging read \
  'resource.type="cloud_run_job" AND resource.labels.job_name="can-ids-pipeline"' \
  --format="value(textPayload)" \
  --limit=100 \
  --freshness=1h

# Vérifier la table d'audit
bq query --use_legacy_sql=false \
"SELECT layer, domain, status, COUNT(*) as n
 FROM \`can_ids_bqnative_audit.file_processing_status\`
 GROUP BY 1,2,3"
```


---


## 15. Référence de configuration

**Structure de config-native-bigquery.yml :**

```yaml
bigquery:
  project_id: project-e6de9b55-41d5-4f13-ae0
  location: europe-southwest1
  datasets:
    bronze_analytics: can_ids_bqnative_bronze_analytics
    bronze_ml: can_ids_bqnative_bronze_ml
    silver_analytics: can_ids_bqnative_silver_analytics
    silver_ml: can_ids_bqnative_silver_ml
    gold_analytics: can_ids_bqnative_gold_analytics
    gold_ml: can_ids_bqnative_gold_ml
    audit: can_ids_bqnative_audit

gcs_paths:
  landing:
    raw: gs://can-ids-data-bqnative/landing/raw
    signal: gs://can-ids-data-bqnative/landing/signal
  spark_jobs: gs://can-ids-data-bqnative/spark_jobs

dataproc:
  cluster_name: can-ids-spark-cluster
  region: europe-southwest1
  zone: europe-southwest1-a
  num_workers: 2
  master_machine_type: e2-standard-4
  worker_machine_type: e2-standard-4
```


---


## Structure du dépôt

```
.
|-- spark_jobs/
|   |-- common_bq.py
|   |-- 00_create_bigquery_native_schema.py
|   |-- 01_bronze_analytics_ingestion.py
|   |-- 01_bronze_ml_ingestion.py
|   |-- 02_silver_analytics_clean.py
|   |-- 02_silver_ml_clean.py
|   |-- 03_gold_analytics_window_5min_wide.py
|   |-- 03_gold_ml_signal_big_table.py
|   `-- 05_quality_checks_datamesh.py
|
|-- orchestration/
|   |-- orchestrate_pipeline.py
|   |-- setup_scheduler.py
|   |-- setup_monitoring.py
|   `-- Dockerfile
|
|-- config/
|   `-- config-native-bigquery.yml
|
`-- README.md
```
