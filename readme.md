# Projet Final Data Integration 
Projet réalisé par :
- Othmane Machrouh
- Ayoub Larouji
- Sabry Ben Aissa

# Documentation

## Retraitement des Données en Cas d'Inconsistance

### Architecture de Retraitement
Notre système suit l'architecture Lambda (comme présenté dans les slides du cours) qui permet à la fois le traitement en temps réel et le retraitement historique. En cas d'inconsistance dans les données, voici le processus de retraitement :

1. **Sauvegarde des Données Brutes**
   - Toutes les données reçues de Kafka sont stockées au format Parquet avec horodatage
   - Chaque lot de données contient un identifiant de lot (`batch_id`)
   - Les données sont conservées dans `RAW_DATA_PATH` pour permettre le retraitement

2. **Processus de Retraitement**
   - Utilisation de la classe `DataReprocessor`
   - Possibilité de retraiter une plage temporelle spécifique
   - Chaque retraitement crée une nouvelle version des données
   - Versionnement des résultats pour suivre les différents retraitements

3. **Étapes du Retraitement**
```python
# Exemple de retraitement
processor = DataReprocessor()
processor.reprocess_time_range(
    start_time=start_date,
    end_time=end_date,
    version_id="reprocessing_v1"
)
```

## Métriques Calculées

### Métriques Temporelles
1. **Moyennes Horaires**
   - pH moyen (`avg_ph`)
   - Écart-type du pH (`std_ph`)
   - Concentration moyenne de sulfate (`avg_sulfate`)
   - Concentration moyenne de calcium (`avg_calcium`)
   - Capacité de neutralisation des acides moyenne (`avg_anc`)

2. **Métriques de Volume**
   - Nombre d'échantillons par période (`sample_count`)
   - Nombre de sites uniques (`unique_sites`)

### Métriques par Type de Plan d'Eau
- Analyses groupées par `WATERBODY_TYPE`
- Comparaisons entre les différents types de plans d'eau

## Choix de Base de Données

Pour ce projet, nous recommandons **TimescaleDB** pour les raisons suivantes :

1. **Avantages**
   - Extension PostgreSQL optimisée pour les séries temporelles
   - Supporte nativement les requêtes SQL standard
   - Excellentes performances pour les données horodatées
   - Capacités d'agrégation temporelle automatique
   - Possibilité de compression des données historiques

2. **Structure Proposée**
```sql
-- Table principale des mesures
CREATE TABLE water_measurements (
    time TIMESTAMPTZ NOT NULL,
    site_id TEXT,
    waterbody_type TEXT,
    ph DOUBLE PRECISION,
    sulfate DOUBLE PRECISION,
    calcium DOUBLE PRECISION,
    anc DOUBLE PRECISION,
    batch_id INTEGER
);

-- Conversion en table hypertable
SELECT create_hypertable('water_measurements', 'time');
```

3. **Justification**
   - Basé sur PostgreSQL, donc mature et fiable
   - Parfait pour les données de surveillance environnementale
   - Supporte les requêtes complexes et l'analyse temporelle
   - Facilite la création de tableaux de bord en temps réel
   - Bonne intégration avec les outils d'analyse et de visualisation

### Alternative Considérée
InfluxDB pourrait être une alternative, mais TimescaleDB a été préféré pour :
- Sa compatibilité SQL complète
- Sa facilité d'intégration avec l'écosystème PostgreSQL
- Ses capacités de jointure plus sophistiquées

## Conclusion
Cette architecture permet :
- Un retraitement flexible des données en cas d'inconsistance
- Un calcul fiable des métriques clés
- Un stockage optimisé pour l'analyse temporelle
- Une maintenance et une évolution simplifiées du système