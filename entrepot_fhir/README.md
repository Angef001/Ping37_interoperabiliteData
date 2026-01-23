# 🏥 Entrepôt de Données de Santé (EDS) - HAPI FHIR

Ce module contient l'infrastructure de l'entrepôt de données, basé sur le **HAPI FHIR JPA Server Starter**. Il sert de serveur central pour stocker et servir les ressources cliniques du projet PING au format standard FHIR.

## 📋 Architecture

* **Moteur** : HAPI FHIR (Java).
* **Framework** : Spring Boot.
* **Base de données** : H2 (embarquée par défaut pour le développement).
* **Interface Web** : Overlay de test intégré pour explorer les ressources.

---

## 🚀 Lancement Local (Sans Conteneur)

### Prérequis

* **Java JDK 17 ou 21** installé sur ton système.
* **Maven** installé (`sudo apt install maven` sur Linux).

### Étapes

1. **Entrer dans le dossier** :
```bash
cd entrepot_fhir/hapi-fhir-jpaserver-starter-master

```


2. **Lancer le serveur** :
```bash
mvn spring-boot:run

```


*(Note : La première exécution téléchargera toutes les dépendances Java, cela peut prendre quelques minutes)*.
3. **Accès** :
Ouvre ton navigateur sur [http://localhost:8080/fhir/].

---

## 🐳 Lancement avec Podman (Recommandé)

Le projet inclut un `Dockerfile` officiel optimisé pour la sécurité et la performance.

```bash
# Depuis la racine du projet PING
podman-compose up -d fhir-server

```

### Avantages de la version conteneurisée :

* **Isolation** : Pas besoin d'installer Java ou Maven sur ta machine.
* **Sécurité** : Exécution en mode "non-root" (UID 65532).
* **Persistance** : Les données sont sauvegardées dans un volume nommé `fhir-data`.

---

## ⚙️ Configuration

La configuration principale se trouve dans le fichier :
`src/main/resources/application.yaml`.

Tu peux y modifier :

* Le port d'écoute (par défaut 8080).
* Les paramètres de la base de données.
* Les options de validation FHIR.

---

## 📁 Structure du Projet

* `pom.xml` : Gestionnaire de dépendances Maven et plugins de build.
* `Dockerfile` : Instructions de build multi-stage (Build avec Maven, Run avec JRE).
* `src/main/resources/` : Fichiers de configuration et propriétés du serveur.