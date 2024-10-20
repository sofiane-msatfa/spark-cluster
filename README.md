# Spark Cluster

## Getting started

### 1. Lancer le cluster
```
docker compose up -d
```

### 2. Accéder aux interfaces web

1. hadoop cluster: http://localhost:9870/
2. hadoop cluster - resource manager: http://localhost:8088/
3. spark cluster: https://localhost:8080/
4. jupyter notebook: https://localhost:8888/
5. spark history server: http://localhost:18080/
6. spark job monitoring: http://localhost:4040/

### 3. Aller plus loin

Les dossiers `/data`, `/notebooks` et `/scripts` sont directement montés dans le master.
Ils permettent de facilement y accéder depuis le container pour intéragir avec HDFS

Exemple:
```
docker exec -it hadoop-namenode /bin/bash -c "./scripts/traitement.sh"
```

### Traitements

Mettre les archives ZIP dans /data/input

Accéder à Jupyter : https://localhost:8888/

Lancer le script `/notebooks/traitement.ipynb`

Les données sont traitées et envoyées sur en base de données (les credentials se trouvent dans le compose.yml si besoin d'y accéder)

### Analyses

Explications des analyses proposées :

1. Analyse des retards par distance :
    - Ce graphique montre la relation entre la distance parcourue par les vols et les retards moyens pour chaque cause. Cela permet d'identifier si les vols plus longs ou plus courts sont plus susceptibles d'accumuler des retards.
    - Les lignes représentent les différents types de retards (compagnie, météo, etc.).

2. Distribution des retards par heure de départ :
    - Analyse les retards moyens selon l'heure de départ des vols. Cela permet de voir si certaines heures sont plus sujettes aux retards (par exemple, les vols du matin sont-ils plus ponctuels que ceux de l'après-midi ?).