# CAN IDS — Data Pipeline

## Description

Pipeline de données pour la **détection d'intrusions sur le bus CAN** des véhicules automobiles.

Le protocole CAN (Controller Area Network) est le réseau de communication principal des véhicules modernes. Il permet aux calculateurs embarqués (ECU) d'échanger des informations critiques : vitesse, régime moteur, freinage, direction, etc. Le problème : ce protocole, conçu dans les années 1980, ne dispose d'**aucun mécanisme d'authentification natif**. N'importe quel appareil connecté au bus peut envoyer des messages au nom de n'importe quel capteur, ce qui expose le véhicule à des cyberattaques.

Ce projet construit le pipeline de données qui **collecte, prépare et structure** les messages CAN afin de fournir les indicateurs comportementaux nécessaires à un système de détection d'intrusions (IDS).

## Dataset

**X-CANIDS** (Korea University, IEEE 2024) — messages CAN réels capturés sur une Hyundai LF Sonata 2017. Le dataset contient 150+ fichiers Parquet totalisant 5.5 Go, avec 688 signaux décodables via fichier DBC, une résolution à la microseconde, et 5 scénarios d'attaque : fuzzing, fabrication, suspension, masquerade et replay.

Chaque message CAN contient :
- **timestamp** : horodatage en microsecondes (index monotone, unique par dump)
- **arbitration_id** : identifiant du capteur émetteur (11 bits, 0–2047)
- **dlc** : longueur du payload (0–8 octets)
- **data** : contenu brut du message (payload binaire)
- **label** : 0 = trafic normal (benign), 1 = attaque (intrusion)

Les dumps 1 à 7 contiennent uniquement du trafic normal. Les dumps suivants contiennent des scénarios d'attaque mixtes.

## Architecture

Le pipeline suit l'architecture **Medallion** en 3 couches sur SQL Server 2022 :

- **Bronze (TABLE)** — messages CAN bruts, immuables, aucune transformation. C'est la source de vérité du pipeline.
- **Silver (VUES SQL)** — signaux décodés depuis les octets bruts, ratio de zéros calculé, et Inter-Arrival Time (IAT) mesuré entre chaque message du même capteur. Données nettoyées et enrichies.
- **Gold (VUES SQL)** — features comportementales agrégées par capteur par session. Indicateurs exploitables par l'IDS.

### Features Gold (5 indicateurs par capteur)

- **Statistiques IAT** — mesure la régularité du rythme d'émission. Détecte les fabrications (trop rapide) et les suspensions (trop lent).
- **Fréquence par capteur** — mesure le nombre de messages par seconde. Détecte les injections massives.
- **Entropie Shannon** — mesure le degré d'aléatoire du payload. Détecte le fuzzing (octets aléatoires).
- **Distribution DLC** — mesure la constance de la structure du message. Détecte les trames forgées avec un mauvais DLC.
- **Ratio de zéros** — mesure la cohérence structurelle du payload. Un changement dans ce ratio peut indiquer qu'un attaquant usurpe l'identité d'un capteur.

## Prérequis

- **SQL Server 2022** Developer Edition (installé localement)
- **SSMS** (SQL Server Management Studio)
- **Python 3.11+**
- **ODBC Driver 18** for SQL Server
- **Git**

## Structure du dépôt

```
can-ids-data/
├── scripts/          # Scripts SQL et Python du pipeline
├── tests/            # Tests unitaires et de performance
├── docs/             # Diagrammes draw.io et documentation
├── data/
│   └── raw/          # Fichiers Parquet X-CANIDS (non committé)
├── .gitignore
├── .env.example
├── .python-version
├── requirements.txt
└── README.md
```

## Stack technique

- **Data Warehouse** — SQL Server 2022 Developer (local)
- **Ingestion** — Python 3.11 (pyarrow + pyodbc)
- **Traitement** — SQL Server (vues, window functions, CTE)
- **Versioning** — Git / GitHub
- **Gestion projet** — Jira / Confluence
