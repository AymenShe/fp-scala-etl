# Projet Scala — ETL (Dataset 3 : Movies)

## Dataset choisi

Dataset 3 **Movies – Films et séries** :

- `data/data_clean.json` : 100 entrées propres
- `data/data_dirty.json` : 500 entrées avec erreurs (champs manquants, valeurs hors bornes, doublons…)
- `data/data_large.json` : 15 000 entrées (films + séries)

Spécifications détaillées : `fp-scala-etl/data/SPECIFICATIONS.md`.

## Compiler et exécuter (projet ETL)

Se placer dans le projet :

```bash
cd fp-scala-etl
```

Compiler :

```bash
sbt compile
```

Exécuter :

```bash
sbt run
```

Résultat :

- JSON de sortie : `fp-scala-etl/output/results.json`

### Choix du fichier d’entrée

Le fichier d’entrée est défini dans `fp-scala-etl/src/main/scala/Main.scala` :

```scala
val filename = "data/data_large.json"
```

On peut le changer en `data_clean.json` / `data_dirty.json` pour tester les autres datasets.

## Choix techniques (résumé)

### Modélisation : `case class`

Les structures métier (`Movie`, `AnalysisReport`, etc.) sont définies en `case class` pour :

- l’immutabilité
- le pattern matching
- la (dé)sérialisation JSON simple

Les champs parfois absents (ex. `budget`, `revenue`) sont modélisés en `Option[Double]` plutôt que par des valeurs sentinelles.

### JSON : Circe

Le parsing/encodage JSON utilise Circe

### Gestion d’erreurs

- `Either[String, A]` pour remonter une erreur ou un résultat
- `Try` pour encapsuler les I/O (lecture/écriture fichier) puis conversion en `Either`

Une entrée invalide est ignorée mais comptée (`decodingErrors`) afin de produire un rapport même avec un dataset “dirty”.

## Performance (data_large.json)

Temps obtenu sur `data_large.json` : **0,323 s**.


## Difficultés rencontrées & solutions

- **Données incohérentes / manquantes** : validation métier + champs optionnels en `Option`
- **Entrées JSON invalides** : entrée ignorée mais comptabilisée
- **Doublons** : suppression via déduplication par `id`
