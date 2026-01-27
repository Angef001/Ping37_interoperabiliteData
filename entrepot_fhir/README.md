# 🏥 Entrepôt de Données de Santé (EDS) - HAPI FHIR

Ce module contient l'infrastructure de l'entrepôt de données, basé sur le **HAPI FHIR JPA Server Starter**. Il sert de serveur central pour stocker et servir les ressources cliniques du projet PING au format standard FHIR.

## 🛠️ Prérequis Système (Installation Linux)

Avant de commencer, vous devez installer les outils nécessaires selon votre mode de lancement préféré.

### 1. Pour le lancement avec Podman (Recommandé)

Podman est un moteur de conteneurisation sans démon, compatible avec Docker.

```bash
# Mise à jour des dépôts
sudo apt update

# Installation de Podman et Podman-compose
sudo apt install -y podman podman-compose

```

### 2. Pour le lancement natif (Maven)

Si vous préférez compiler et lancer le serveur directement sur votre hôte.

```bash
# Installation du JDK (Java Development Kit) 17 ou 21
sudo apt install -y openjdk-17-jdk

# Installation de Maven
sudo apt install -y maven

```

---

## 🚀 Lancement du Serveur

### Option A : Avec Podman (Recommandé)

Le projet utilise un `Dockerfile` optimisé pour la sécurité (mode "non-root").

```bash
# Depuis la racine du projet PING
podman-compose up -d fhir-server

```

* **Isolation** : Aucune installation de Java ou Maven n'est requise sur votre machine.
* **Persistance** : Les données sont conservées dans le volume `fhir-data`.

### Option B : Lancement Local (Développement)

```bash
# 1. Entrer dans le dossier du serveur
cd entrepot_fhir/hapi-fhir-jpaserver-starter-master

# 2. Lancer le serveur via Maven
mvn spring-boot:run

```

*Note : Le premier lancement peut être long en raison du téléchargement des dépendances Java.*

---

## ⚙️ Configuration et Accès

* **Accès Web** : Le serveur est accessible sur http://localhost:8080/fhir/.
* **Fichier de configuration** : La personnalisation (ports, base de données, validation) s'effectue dans `src/main/resources/application.yaml`.
* **Base de données** : Par défaut, le serveur utilise une base **H2** embarquée pour faciliter le développement.
