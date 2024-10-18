# Spark Cluster

## Getting started

### 1. Lancer le cluster
```
docker compose up -d
```

### 2. Accéder aux interfaces web

1. hadoop cluster: http://localhost:50070/
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