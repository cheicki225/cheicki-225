"""
Résolution du dossier de données persistantes (fix stockage Railway)
==========================================================================
Sans volume Railway attaché, tout fichier écrit par le bot (CSV, blacklist,
.env) vit sur le disque éphémère du conteneur — perdu à CHAQUE redéploiement.

Si un volume est attaché au service (dashboard Railway : ⌘K ou clic droit
sur le canvas -> "Volume", puis choisir un chemin de montage comme /data),
Railway définit AUTOMATIQUEMENT la variable d'environnement
RAILWAY_VOLUME_MOUNT_PATH — pas besoin de la configurer à la main, ni de
toucher railway.json (les volumes ne se déclarent pas dans ce fichier).

Tous les fichiers qui doivent survivre à un redéploiement passent par
chemin_donnees() plutôt que d'utiliser un chemin relatif en dur.

En LOCAL (pas de volume Railway), la variable n'existe pas -> on retombe
sur le dossier courant, donc rien ne change pour les tests en local ou
pour l'entraînement manuel du modèle ML sur ton PC.
"""

import os

DOSSIER_DONNEES = os.getenv("RAILWAY_VOLUME_MOUNT_PATH", ".")


def chemin_donnees(nom_fichier: str) -> str:
    """Retourne le chemin complet d'un fichier dans le dossier de données persistantes."""
    return os.path.join(DOSSIER_DONNEES, nom_fichier)
