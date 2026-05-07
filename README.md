# 🚗 CAN IDS — Cloud Data Pipeline

> Pipeline de données cloud pour la **détection d'intrusions sur le bus CAN** des véhicules automobiles.

---

## Table des matières

- [Contexte](#contexte)
- [Dataset](#dataset)
- [Architecture](#architecture)
- [Services GCP](#services-gcp)
- [Couches du pipeline](#couches-du-pipeline)
  - [Bronze](#bronze)
  - [Silver](#silver)
  - [Gold](#gold)
- [Profil CAN ID](#profil-can-id)
- [Vue analyste](#vue-analyste)
- [Audit & Qualité](#audit--qualité)
- [Arborescence du projet](#arborescence-du-projet)
- [Description des composants](#description-des-composants)

---

## Contexte

Le protocole **CAN (Controller Area Network)** est le réseau de communication principal des véhicules modernes. Il permet aux calculateurs embarqués (**ECU**) d'échanger des informations critiques : vitesse, régime moteur, freinage, direction, etc.

⚠️ **Ce protocole ne dispose d'aucun mécanisme natif d'authentification ou de chiffrement.** Tout équipement connecté au bus peut injecter ou rejouer des messages CAN, exposant le véhicule à plusieurs types d'attaques :

| Type d'attaque | Description |
|---|---|
| Fuzzing | Injection de messages aléatoires |
| Fabrication | Création de messages frauduleux |
| Suspension | Interruption de messages légitimes |
| Replay | Rejeu de messages enregistrés |
| Masquerade | Usurpation d'identité d'un ECU |

Ce projet met en place un pipeline de données cloud permettant de :

- 📥 Ingérer les messages CAN bruts
- 🧹 Nettoyer et enrichir les données
- 📊 Calculer des indicateurs comportementaux
- 🔍 Produire des features exploitables pour un IDS
- ✅ Auditer les traitements et contrôler la qualité
- ⚙️ Automatiser l'exécution du pipeline à coût maîtrisé

---

## Dataset

Le pipeline s'appuie sur le dataset **X-CANIDS** (IEEE 2024), composé de messages CAN réels capturés sur un véhicule Hyundai.

### Schéma des messages

| Champ | Type | Description |
|---|---|---|
| `timestamp_us` | integer | Horodatage en microsecondes |
| `arbitration_id` | integer | Identifiant CAN |
| `aid_hex` | string | Identifiant CAN au format hexadécimal |
| `dlc` | integer | Longueur du payload |
| `data` | bytes | Payload brut |
| `label` | string | `normal` / `attaque` |
| `source_file` | string | Fichier source |
| `session_id` | string | Session de capture |

### Localisation des fichiers

```
gs://can-ids-data/raw/          # Fichiers Parquet
gs://can-ids-data/dbc/hyundai_2015_ccan.dbc   # Fichier DBC
```

---

## Architecture

Le pipeline suit une architecture **Medallion** sur Google Cloud Platform.

```
                        ┌─────────────────┐
                        │  Cloud Storage  │
                        │  (fichiers raw) │
                        └────────┬────────┘
                                 │
                        ┌────────▼────────┐
                        │     Bronze      │
                        │  (données brutes)│
                        └────────┬────────┘
                                 │
                        ┌────────▼────────┐
                        │     Silver      │
                        │  (nettoyage +   │
                        │   enrichissement)│
                        └────────┬────────┘
                                 │
                        ┌────────▼────────┐
                        │      Gold       │
                        │  (features IDS) │
                        └────────┬────────┘
                                 │
                        ┌────────▼────────┐
                        │ Audit / Quality │
                        │    Reports      │
                        └─────────────────┘
```

### Orchestration

```
Cloud Scheduler
      │  (déclenchement planifié)
      ▼
Cloud Run Job
      │  (orchestre le pipeline)
      ▼
Dataproc (cluster éphémère)
      │  (exécute les jobs Spark)
      ▼
BigQuery
      (stockage des couches Medallion)
```

> 💡 Le cluster Dataproc est créé **uniquement lorsqu'un nouveau traitement est nécessaire** et supprimé automatiquement en fin d'exécution.

---

## Services GCP

| Service | Rôle |
|---|---|
| **Cloud Storage** | Stockage des fichiers Parquet et DBC |
| **BigQuery** | Entrepôt de données (Bronze / Silver / Gold / Audit) |
| **Dataproc** | Exécution des jobs Spark |
| **Cloud Run Job** | Orchestrateur du pipeline |
| **Cloud Scheduler** | Déclenchement planifié |
| **Cloud Build** | CI/CD et build de l'image Docker |
| **Artifact Registry** | Stockage de l'image Docker |
| **Cloud Logging** | Journalisation des exécutions |
| **IAM** | Gestion des permissions |

---

## Couches du pipeline

### Bronze

> Ingestion des données brutes depuis Cloud Storage.

**Tables :**

```
can_ids_bronze.messages_raw
can_ids_bronze.messages_rejected
```

**Objectifs :**

- Conserver les données brutes sans transformation
- Tracer le fichier source de chaque message
- Gérer l'incrémentalité (éviter les doublons)
- Alimenter la table d'audit

---

### Silver

> Nettoyage, normalisation et enrichissement des données.

**Tables :**

```
can_ids_silver.messages_clean
can_ids_silver.messages_with_iat
can_ids_silver.dbc_messages_reference
```

**Transformations appliquées :**

- Nettoyage et validation des colonnes
- Normalisation des types de données
- Extraction du payload octet par octet
- Calcul du ratio de zéros (`zeros_ratio`)
- Calcul de l'**Inter-Arrival Time (IAT)**
- Enrichissement via le fichier DBC

---

### Gold

> Calcul des features comportementales pour l'IDS.

**Tables et vues :**

```
can_ids_gold.gold_features_window
can_ids_gold.can_id_profile
can_ids_gold.vw_gold_features_analyst
```

**Features calculées :**

| Feature | Utilité IDS |
|---|---|
| Fréquence des messages | Détection d'injections massives |
| IAT mean / std / CV | Détection de perturbations temporelles |
| Entropie du payload | Détection de fuzzing |
| Distribution du DLC | Détection d'anomalies structurelles |
| Ratio moyen de zéros | Détection de messages suspects |

**Types d'attaques détectables :**

- 💉 Injections massives
- 🎲 Fuzzing
- 🚫 Suspensions de CAN ID
- 🎭 Usurpation de CAN ID

---

## Profil CAN ID

**Table :** `can_ids_gold.can_id_profile`

| Champ | Description |
|---|---|
| `arbitration_id` | Identifiant CAN numérique |
| `aid_hex` | Identifiant CAN hexadécimal |
| `message_name` | Nom issu du DBC |
| `signals_count` | Nombre de signaux associés |
| `signal_names` | Liste des signaux |
| `messages_count` | Nombre total de messages observés |
| `uniq_data_count` | Nombre de payloads distincts |
| `interval_mean` | Intervalle moyen entre messages |
| `interval_std` | Écart-type de l'intervalle |
| `uniq_dlc` | Nombre de DLC distincts |
| `computed_at` | Timestamp de calcul |

> Les CAN IDs absents du DBC sont conservés sous la forme `UNKNOWN_0xXXX`.

---

## Vue analyste

**Vue :** `can_ids_gold.vw_gold_features_analyst`

Jointure entre `gold_features_window` et `can_id_profile`.

Destinée à l'analyse exploratoire et à la data science.

---

## Audit & Qualité

### Tables

```
can_ids_audit.file_processing_status
can_ids_audit.pipeline_runs
can_ids_audit.quality_reports
```

### `file_processing_status`

Suivi fichier par fichier du statut de traitement :

- Statuts Bronze / Silver / Gold
- Gestion des erreurs
- Support de la reprise incrémentale

### `pipeline_runs`

Suivi des exécutions du pipeline :

- Dates de début et fin
- Durée d'exécution
- Statut (succès / échec)
- Volumes traités

### `quality_reports`

Contrôles qualité automatisés :

- ✅ Présence de données dans Bronze, Silver, Gold
- ✅ Absence d'IAT négatifs
- ✅ Cohérence des fenêtres temporelles
- ✅ Présence des profils CAN ID
- ✅ Absence de fichiers en erreur ou en attente
- ✅ Détection des `session_id` vides

---

## Arborescence du projet

```
Intrusion-Detection-in-CAN-Networks/
├── config/
│   └── config.yml                          # Configuration globale du pipeline
│
├── spark_jobs/
│   ├── bronze_ingestion_spark.py           # Ingestion Bronze
│   ├── silver_clean_spark.py               # Nettoyage Silver
│   ├── silver_iat_spark.py                 # Calcul IAT
│   ├── dbc_messages_reference_spark.py     # Extraction DBC
│   ├── gold_features_window_spark.py       # Features Gold
│   ├── can_id_profile_spark.py             # Profil CAN ID
│   └── quality_reports_spark.py            # Contrôles qualité
│
├── orchestrator/
│   ├── orchestrate_pipeline.py             # Orchestrateur principal
│   ├── requirements.txt                    # Dépendances Python
│   └── Dockerfile                          # Image Cloud Run Job
│
├── init-actions/
│   └── install_python_packages.sh          # Init action Dataproc
│
├── scripts/
│   ├── create_or_update_cloud_run_job.ps1      # Déploiement Cloud Run
│   ├── create_or_update_cloud_scheduler.ps1    # Déploiement Scheduler
│   ├── create_dataproc_cluster.py              # Création cluster Dataproc
│   ├── delete_dataproc_cluster.py              # Suppression cluster Dataproc
│   ├── submit_bronze_ingestion_job.py
│   ├── submit_silver_clean_job.py
│   ├── submit_silver_iat_job.py
│   ├── submit_gold_features_window_job.py
│   ├── submit_can_id_profile_job.py
│   └── submit_quality_reports_job.py
│
├── docs/                                   # Documentation technique
├── tests/                                  # Tests unitaires et de qualité
├── README.md
└── .gitignore
```

---

## Description des composants

### `config/config.yml`

Fichier de configuration central du projet. Contient :

- L'identifiant du projet GCP
- Les buckets Cloud Storage
- Les noms des datasets BigQuery
- Les noms des tables Bronze, Silver, Gold et Audit
- Les paramètres utilisés par les jobs Spark

---

### `spark_jobs/`

Jobs PySpark exécutés sur le cluster Dataproc.

#### `bronze_ingestion_spark.py`

- Détecte les nouveaux fichiers dans `gs://can-ids-data/raw/`
- Lit les fichiers Parquet CAN
- Écrit dans `can_ids_bronze.messages_raw`
- Enregistre les lignes rejetées dans `messages_rejected`
- Met à jour `file_processing_status`
- Fonctionne en mode incrémental

#### `silver_clean_spark.py`

- Lit les données Bronze
- Nettoie et normalise les colonnes
- Génère `aid_hex`
- Extrait les octets du payload
- Calcule `zero_byte_count` et `zeros_ratio`
- Écrit dans `can_ids_silver.messages_clean`

#### `silver_iat_spark.py`

- Lit `messages_clean`
- Ordonne les messages par `session_id`, `arbitration_id`, `timestamp_us`
- Calcule l'IAT entre deux messages consécutifs du même CAN ID
- Génère `seq_num`, `previous_timestamp_us` et `iat_us`
- Écrit dans `can_ids_silver.messages_with_iat`

#### `dbc_messages_reference_spark.py`

- Lit le fichier DBC
- Extrait les messages CAN connus et leurs signaux associés
- Calcule le nombre de signaux par message
- Écrit dans `can_ids_silver.dbc_messages_reference`

#### `gold_features_window_spark.py`

- Lit `messages_with_iat`
- Agrège par fenêtre temporelle, session et CAN ID
- Calcule les features IDS (fréquence, IAT stats, entropie, DLC, zéros)
- Écrit dans `can_ids_gold.gold_features_window`

#### `can_id_profile_spark.py`

- Lit `messages_with_iat`
- Agrège les statistiques globales par `arbitration_id`
- Joint avec la référence DBC
- Génère les labels `UNKNOWN_0xXXX` pour les CAN IDs inconnus
- Écrit dans `can_ids_gold.can_id_profile`

#### `quality_reports_spark.py`

- Vérifie la présence de données dans toutes les couches
- Vérifie les `session_id` vides, les IAT négatifs, les valeurs Gold invalides
- Vérifie la présence des profils CAN ID
- Écrit les résultats dans `can_ids_audit.quality_reports`

---

### `orchestrator/`

#### `orchestrate_pipeline.py`

Script principal d'orchestration :

1. Détecte les nouveaux fichiers dans Cloud Storage
2. Vérifie les traitements en attente dans `file_processing_status`
3. Crée un cluster Dataproc si nécessaire
4. Soumet les jobs Spark dans le bon ordre
5. Attend la fin de chaque job
6. Crée ou met à jour la vue analyste
7. Lance les rapports qualité
8. **Supprime le cluster Dataproc à la fin, même en cas d'erreur**

#### `Dockerfile`

- Part d'une image Python
- Installe les dépendances depuis `requirements.txt`
- Copie `orchestrate_pipeline.py`
- Définit la commande de démarrage

---

### `init-actions/install_python_packages.sh`

Script exécuté au démarrage du cluster Dataproc :

- Crée un wheelhouse local
- Copie les packages Python depuis Cloud Storage
- Installe les dépendances offline (dont `cantools`)
- Teste les imports nécessaires aux jobs Spark

---

### `scripts/`

#### Scripts PowerShell de déploiement

| Script | Rôle |
|---|---|
| `create_or_update_cloud_run_job.ps1` | Crée ou met à jour le Cloud Run Job (image, variables d'env, service account, timeout) |
| `create_or_update_cloud_scheduler.ps1` | Crée ou met à jour le Cloud Scheduler (fréquence, appel HTTP, service account) |

#### Scripts Python utilitaires

| Script | Rôle |
|---|---|
| `create_dataproc_cluster.py` | Crée manuellement un cluster Dataproc temporaire |
| `delete_dataproc_cluster.py` | Supprime manuellement le cluster |
| `submit_*.py` | Soumission manuelle d'un job Spark spécifique (debug / relance) |

---

### `docs/`

Documentation technique du projet :

- Diagrammes d'architecture
- Schémas de pipeline
- Captures d'écran
- Notes de conception et d'exploitation

### `tests/`

Dossier de tests :

- Tests unitaires des jobs Spark
- Tests de qualité des features
- Tests de non-régression
- Scripts de validation

---

*Pipeline construit sur Google Cloud Platform — Architecture Medallion — Orchestration Dataproc éphémère*