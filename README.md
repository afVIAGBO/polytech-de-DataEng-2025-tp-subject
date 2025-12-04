# Sujet de travaux pratiques "Introduction à la data ingénierie"

Le but de ce projet est de créer un pipeline ETL d'ingestion, de transformation et de stockage de données pour mettre en pratique les connaissances acquises lors du cours d'introduction à la data ingénierie. Ce sujet présenté propose d'utiliser les données d'utilisation des bornes de vélos open-sources et "temps réel" dans les grandes villes de France.

Le sujet propose une base qui est un pipeline ETL complet qui couvre la récupération, le stockage et la transformation d'une partie des données de la ville de Paris.

Le but du sujet de travaux pratiques est d'ajouter à ce pipeline des données de consolidation, de dimensions et de faits pour la ville de Paris, ainsi que les données provenant d'autres grandes villes de France. Ces données sont disponibles pour les villes de Nantes... Il faudra aussi enrichir ces données avec les données descriptives des villes de France, via une API de l'État français open-source.

### Comment faire fonctionner ce projet?

Pour faire fonctionner ce sujet, c'est assez simple:

```bash 

curl https://install.duckdb.org | sh

git clone https://github.com/afVIAGBO/polytech-de-DataEng-2025-tp-subject.git

cd polytech-de-101-2025-tp-subject

python3 -m venv .venv

source .venv/bin/activate

pip install -r requirements.txt

python src/main.py
```

Vous pouvez utiliser la commande './duckdb data/duckdb/mobility_analysis.duckdb' pour ouvrir l'invite de commande DuckDB. 
et réaliser les requêtes SQL suivantes sur la base de données DuckDB :


```sql

-- Nb d'emplacements disponibles de vélos dans une ville
SELECT dm.NAME, tmp.SUM_BICYCLE_DOCKS_AVAILABLE
FROM DIM_CITY dm INNER JOIN (
    SELECT CITY_ID, SUM(BICYCLE_DOCKS_AVAILABLE) AS SUM_BICYCLE_DOCKS_AVAILABLE
    FROM FACT_STATION_STATEMENT
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION)
    GROUP BY CITY_ID
) tmp ON dm.ID = tmp.CITY_ID
WHERE lower(dm.NAME) in ('paris', 'nantes', 'vincennes', 'toulouse');


-- Nb de vélos disponibles en moyenne dans chaque station
SELECT ds.name, ds.code, ds.address, tmp.avg_dock_available
FROM DIM_STATION ds JOIN (
    SELECT station_id, AVG(BICYCLE_AVAILABLE) AS avg_dock_available
    FROM FACT_STATION_STATEMENT
    GROUP BY station_id
) AS tmp ON ds.id = tmp.station_id;
```
