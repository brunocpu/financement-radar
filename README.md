# 🏛️ Financement Radar PACA

> **[Démo live →](https://brunocpu.github.io/financement-radar/)**

**POC d'aide à l'instruction** — Vue croisée des financements État par commune en région PACA.

## Le problème

Chaque dotation (DETR, DSIL, Fonds Vert, ADEME…) est instruite **en silo**. Quand un dossier arrive sur le bureau d'un instructeur en préfecture ou au SGAR, il n'a aucune visibilité sur les financements déjà obtenus par la commune — ni sur les autres dispositifs, ni sur les autres porteurs de projets localisés sur le même territoire.

## La solution

Un outil HTML standalone qui croise **6 sources de données ouvertes** sur un même territoire :

- **Recherche par commune** (code INSEE ou nom) → carte d'identité complète + historique des financements
- **Recherche par porteur** (EPCI, entreprise, asso…) → tous les projets d'un bénéficiaire, ventilés par commune et dispositif
- **Qualification du porteur** : chaque projet est badgé (commune / EPCI / département / autre) pour distinguer ce qui relève de la commune vs ce qui est localisé sur son territoire

## Sources de données

| Source | Contenu | Millésimes | Périmètre |
|--------|---------|------------|-----------|
| **DGCL** | DETR, DSIL, DPV, DSID | 2018-2024 | PACA |
| **Fonds Vert** | Transition écologique (14 mesures) | 2023-2024 | PACA (corrigé) |
| **ADEME** | Aides financières aux communes | 2021-2026 | PACA |
| **ANCT** | Zonages : PVD, ACV, VA, TI, FS, Cités édu. | 2025 | National |
| **OFGL** | Population, strate, finances communales | 2024 | National |
| **Filosofi** (INSEE) | Revenu médian, Gini, profil socio-économique | 2021 | National |

Toutes les données proviennent de [data.gouv.fr](https://www.data.gouv.fr) et [data.ofgl.fr](https://data.ofgl.fr) sous Licence Ouverte 2.0.

## Fonctionnalités

### Fiche commune
- **Carte d'identité** : population, strate, revenu médian, Gini, zonages ANCT, finances OFGL
- **KPIs** : projets, subventions (€/hab), coût total HT, revenu médian
- **Synthèse** : graphe stacked bar par dispositif × année + tableau croisé
- **Projets localisés** : tableau filtrable avec badge porteur + nom bénéficiaire
- **Benchmark** : positionnement vs médiane départementale

### Fiche porteur
- Agrégation par bénéficiaire : montants totaux, communes, dispositifs
- Liste des projets avec lien vers la fiche commune

### Recherche
- Toggle **Commune** / **Porteur**
- Filtre par département
- Résultats enrichis (badges zonage, population, revenu médian)

## Architecture

```
financement-radar/
├── rebuild_all.py          # Pipeline complet (tout-en-un)
├── build_html.py           # Génération data.js depuis DuckDB
├── corrections_2023.json   # Corrections Fonds Vert PACA 2023
├── index.html              # Application standalone (HTML + Chart.js)
├── data.js                 # Données embarquées (généré)
├── data/                   # CSV + DuckDB (généré, non versionné)
├── requirements.txt
└── README.md
```

**Stack** : Python + DuckDB → data.js → HTML standalone + Chart.js. Zéro backend, zéro serveur.

## Installation

```bash
pip install -r requirements.txt
python rebuild_all.py
```

Puis ouvrir `index.html` dans un navigateur. C'est tout.

`rebuild_all.py` fait tout en une seule commande :
1. Télécharge les CSV depuis data.gouv.fr (~200 MB)
2. Crée la base DuckDB filtrée PACA
3. Applique les corrections Fonds Vert (arrondissements, codes mal mappés)
4. Enrichit avec ANCT, OFGL, Filosofi
5. Génère `data.js`

Pour tout retélécharger : `python rebuild_all.py --force`

## Corrections Fonds Vert

Les CSV Fonds Vert bruts contiennent des erreurs de localisation (code_commune NULL, mal codé, ou hors PACA). Ce POC intègre :
- **~80 corrections 2023** (`corrections_2023.json`) — issues du [POC Fonds Vert PACA](https://brunocpu.github.io/fonds-vert-paca-carto/)
- **24 corrections 2024** (hardcodées dans `rebuild_all.py`)
- **Fix arrondissements** Marseille (13201-13216 → 13055)
- **Fix SIREN ADEME** mal décodés (ex: 13005 → 13055 pour Marseille)

## Voir aussi

- [POC Fonds Vert PACA](https://brunocpu.github.io/fonds-vert-paca-carto/) — Cartographie des projets Fonds Vert en PACA (même architecture)

## Licence

Données sous [Licence Ouverte 2.0](https://www.etalab.gouv.fr/licence-ouverte-open-licence/) (data.gouv.fr). Code : usage libre.
