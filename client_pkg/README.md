
# 🏥 Client CLI - Interopérabilité FHIR CHU Rouen

Ce package Python fournit une interface en ligne de commande (CLI) pour interagir facilement avec l'entrepôt de données de santé FHIR du projet PING.

## 📋 Fonctionnalités

* **Vérification du statut** : Contrôler la connectivité avec le serveur FHIR.
* **Recherche de patients** : Rechercher des patients par nom et afficher les résultats sous forme de tableau.
* **Extraction de ressources** : Récupérer le contenu JSON brut de n'importe quelle ressource FHIR via son ID.

---

## 🚀 Installation (Mode Développement)

Si tu souhaites utiliser le client directement sur ta machine (hors conteneur) :

1. **Créer l'environnement virtuel** :
```bash
python3 -m venv .venv
source .venv/bin/activate

```


2. **Installer le package en mode éditable** :
```bash
pip install -e .

```


*Cette commande installe automatiquement les dépendances : `typer`, `requests` et `rich*`.

---

## 🛠️ Utilisation

Une fois installé, la commande `chu-fhir` est disponible partout dans ton terminal.

### 1. Vérifier la connexion

```bash
chu-fhir info

```

*Vérifie si le serveur est en ligne et affiche la version FHIR supportée*.

### 2. Rechercher un patient

```bash
chu-fhir search-patient --name "M Dupont"

```

*Affiche un tableau avec l'ID, le nom et la date de naissance des patients correspondants*.

### 3. Récupérer une ressource brute

```bash
chu-fhir get-resource Patient 123

```

*Affiche le JSON formaté de la ressource demandée*.

---

Markdown

## 🐳 Utilisation avec Podman (Mode Interactif)

Grâce à la conteneurisation, tu peux utiliser le client `chu-fhir` sans rien installer sur ton système hôte. La méthode la plus efficace consiste à entrer dans le conteneur pour utiliser l'outil en mode interactif :

```bash
# 1. Entrer dans le conteneur client
podman exec -it ping37_interoperabilitedata_cli-client_1 bash

# 2. Une fois à l'intérieur, utilise les commandes directement :
chu-fhir info
chu-fhir get-patient 1
chu-fhir get-patients 1 2 3
Astuce : Pour quitter le conteneur et revenir à ton terminal Windows/Linux, tape simplement exit

## ⚙️ Configuration

Le client utilise par défaut l'URL `http://localhost:8080/fhir`.


---

Voici la section de ton fichier README.md rédigée en Markdown, prête à être copiée-collée :

Markdown

## 🐳 Utilisation avec Podman (Mode Interactif)

Grâce à la conteneurisation, tu peux utiliser le client `chu-fhir` sans rien installer sur ton système hôte. La méthode la plus efficace consiste à entrer dans le conteneur pour utiliser l'outil en mode interactif :

```bash
# 1. Entrer dans le conteneur client
podman exec -it ping37_interoperabilitedata_cli-client_1 bash

# 2. Une fois à l'intérieur, utilise les commandes directement :
chu-fhir info
chu-fhir get-patient 1
chu-fhir get-patients 1 2 3
Astuce : Pour quitter le conteneur et revenir à ton terminal Windows/Linux, tape simplement exit.

⚙️ Configuration
Le client est conçu pour être flexible selon l'environnement d'exécution :

Variables d'environnement : En environnement conteneurisé, le client utilise les variables définies dans le fichier podman-compose.yml :

FHIR_URL : Configurée sur http://127.0.0.1:8080/fhir (adresse locale partagée en mode host).

PYTHONPATH : Définie sur . pour permettre la résolution correcte du module src.

Mode Réseau : L'utilisation du network_mode: host permet au client de communiquer avec l'entrepôt FHIR via l'interface de boucle locale, contournant les limitations DNS des conteneurs sous WSL.

📁 Structure du code
Le projet suit une structure de package Python standard :

src/ : Répertoire source contenant la logique métier.

main.py : Point d'entrée principal. Contient la définition des commandes CLI (Typer) et la gestion des requêtes HTTP vers l'entrepôt.

pyproject.toml : Fichier de configuration du projet. Il définit les dépendances (typer, requests, rich) et crée l'alias de commande chu-fhir.

Dockerfile : Instructions de build pour l'image du client, incluant l'installation du package et la gestion du répertoire de travail /app.
