from cassandra.cluster import Cluster

# Connexion à Cassandra
cluster = Cluster(['127.0.0.1'])  # Remplacez par l'IP du conteneur ou du serveur
session = cluster.connect()

# Sélectionner la base de données (keyspace)
session.set_keyspace('Province_State')

# Créer la table
session.execute("""
CREATE TABLE IF NOT EXISTS covid_data (
    Province_State TEXT,
    Country_Region TEXT,
    Last_Update TEXT,
    Lat DOUBLE,
    Long_ DOUBLE,
    Confirmed INT,
    Deaths INT,
    Recovered DOUBLE,
    Active DOUBLE,
    FIPS DOUBLE,
    Incident_Rate DOUBLE,
    Total_Test_Results INT,
    People_Hospitalized INT,
    Case_Fatality_Ratio DOUBLE,
    UID INT,
    ISO3 TEXT,
    Testing_Rate DOUBLE,
    Hospitalization_Rate DOUBLE,
    Date TEXT,
    People_Tested INT,
    Mortality_Rate DOUBLE,
    PRIMARY KEY (Province_State, Country_Region)
)
""")

print("Table created successfully!")