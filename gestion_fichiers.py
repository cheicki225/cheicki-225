"""
Rotation automatique des fichiers CSV persistants
====================================================
Railway a rempli tout son volume aujourd'hui (Errno 28 : "No space left on
device") — les CSV du projet grossissent indéfiniment, une ligne par
opportunité ou par seconde suivie, sans aucune limite. Ce module empêche
que ça se reproduise : chaque fichier enregistré ici est plafonné à un
nombre de lignes, vérifié périodiquement en tâche de fond.

⚠️ COMPROMIS ASSUMÉ, à connaître avant de s'en servir
Les lignes les plus anciennes au-delà du plafond sont DÉFINITIVEMENT
supprimées — pas archivées ailleurs sur le même disque (ça ne résoudrait
rien : même volume, même limite physique). Le plafond par défaut
(100 000 lignes, quelques dizaines de Mo par fichier selon le CSV) laisse
largement de quoi entraîner le modèle ML tout en garantissant un espace
disque borné pour toujours. Si tu veux un historique complet et illimité,
il faudrait un stockage externe (base de données, S3...) — hors de portée
d'un simple CSV sur un volume Railway à taille fixe.

SÉCURITÉ D'ÉCRITURE
La réécriture se fait dans un fichier temporaire puis remplace l'original
par un déplacement atomique (os.replace) — jamais d'écriture directe dans
le fichier final. Si le processus est interrompu en plein milieu (crash,
redéploiement), le fichier original reste intact, jamais à moitié écrit.

GARDE-FOU D'URGENCE
En plus de la vérification périodique par fichier, une vérification de
l'espace disque RÉEL du volume tourne à part : si l'espace libre devient
critique, une passe de nettoyage plus agressive s'applique immédiatement à
tous les fichiers enregistrés, sans attendre le prochain cycle normal.
"""

import csv
import logging
import os
import shutil
import time
from collections import deque

import stockage

log = logging.getLogger("gestion_fichiers")

MAX_LIGNES_DEFAUT = 100_000

# En dessous de ce seuil d'espace libre, on déclenche un nettoyage
# d'urgence immédiat (plafond réduit) plutôt que d'attendre le prochain
# cycle normal — mieux vaut des données tronquées plus court que de
# retomber sur "No space left on device".
SEUIL_URGENCE_MO = 100
PLAFOND_URGENCE_LIGNES = 5_000

# {chemin: max_lignes} — rempli par chaque module au démarrage via
# enregistrer_fichier(), pour rester générique sans dépendre d'une liste
# figée de CSV existants aujourd'hui (un futur module peut s'ajouter).
_fichiers_surveilles: dict[str, int] = {}


def enregistrer_fichier(chemin: str, max_lignes: int = MAX_LIGNES_DEFAUT):
    """
    À appeler une fois, au chargement de chaque module qui écrit un CSV
    grossissant sans limite (opportunity_logger, suivi_opportunite,
    paper_trading, positions_attente, arbitrage_perpetuel).
    """
    _fichiers_surveilles[chemin] = max_lignes
    log.debug(f"surveillance activée : {os.path.basename(chemin)} (max {max_lignes} lignes)")


def _tronquer_si_necessaire(chemin: str, max_lignes: int) -> tuple[str, int]:
    """
    Retourne (statut, lignes_supprimees), avec statut parmi :
      "rien_a_faire"    — fichier absent, vide, ou déjà sous le plafond
      "tronque"         — réécriture réussie, lignes_supprimees > 0
      "echec_ecriture"  — le disque était trop plein pour même écrire le
                           fichier temporaire (voir _supprimer_en_urgence)

    Cette distinction compte : seul "echec_ecriture" doit déclencher le
    recours à la suppression totale — un petit fichier légitimement sous
    le plafond ne doit JAMAIS être supprimé juste parce que le disque
    reste critique à cause d'AUTRES fichiers plus gros.

    Ne charge JAMAIS le fichier entier en mémoire — une deque bornée à
    max_lignes suffit, même sur un CSV de plusieurs centaines de Mo.
    """
    if not os.path.exists(chemin):
        return "rien_a_faire", 0

    try:
        with open(chemin, encoding="utf-8", newline="") as f:
            entete = f.readline()
            if not entete:
                return "rien_a_faire", 0  # fichier vide

            total = 0
            dernieres_lignes: deque = deque(maxlen=max_lignes)
            for ligne in f:
                dernieres_lignes.append(ligne)
                total += 1

        if total <= max_lignes:
            return "rien_a_faire", 0  # sous le plafond

        chemin_temp = chemin + ".tmp_rotation"
        with open(chemin_temp, "w", encoding="utf-8", newline="") as f:
            f.write(entete)
            f.writelines(dernieres_lignes)
        os.replace(chemin_temp, chemin)  # remplacement atomique

        return "tronque", total - len(dernieres_lignes)

    except OSError as e:
        chemin_temp = chemin + ".tmp_rotation"
        if os.path.exists(chemin_temp):
            try:
                os.remove(chemin_temp)
            except OSError:
                pass
        if e.errno == 28:  # No space left on device
            log.error(
                f"rotation de {chemin} impossible : disque plein AU MOMENT "
                f"MÊME d'écrire le fichier temporaire"
            )
            return "echec_ecriture", 0
        log.error(f"échec de la rotation de {chemin} : {e}")
        return "rien_a_faire", 0
    except Exception as e:
        log.error(f"échec de la rotation de {chemin} : {e}")
        return "rien_a_faire", 0


def _supprimer_en_urgence(chemin: str) -> bool:
    """
    Dernier recours quand le disque est si plein que même écrire un
    fichier temporaire plus petit est impossible : supprime le fichier
    ENTIÈREMENT plutôt que de le tronquer — une suppression ne nécessite
    AUCUNE écriture, contrairement à une réécriture tronquée, donc elle
    fonctionne même à 0 octet libre.

    ⚠️ Perte totale des données de ce fichier, pas juste les plus
    anciennes. C'est le prix d'un disque arrivé à saturation complète —
    la priorité absolue devient de faire tourner le bot à nouveau, pas de
    préserver l'historique. Le fichier est recréé vide au prochain
    _init_csv() du module concerné.
    """
    if not os.path.exists(chemin):
        return False
    try:
        taille_mo = os.path.getsize(chemin) / (1024 * 1024)
        os.remove(chemin)
        log.critical(
            f"🚨 SUPPRESSION D'URGENCE : {os.path.basename(chemin)} "
            f"({taille_mo:.1f} Mo libérés) — disque trop plein pour même "
            f"une rotation tronquée, fichier supprimé entièrement"
        )
        return True
    except OSError as e:
        log.critical(f"🚨 impossible de supprimer {chemin} même en urgence : {e}")
        return False


def verifier_toutes_les_rotations():
    """Passe normale : chaque fichier enregistré, à son plafond habituel."""
    for chemin, max_lignes in _fichiers_surveilles.items():
        statut, supprimees = _tronquer_si_necessaire(chemin, max_lignes)
        if statut == "tronque":
            log.info(
                f"📉 Rotation : {os.path.basename(chemin)} — "
                f"{supprimees} ancienne(s) ligne(s) supprimée(s), "
                f"{max_lignes} conservée(s)"
            )


def espace_disque_libre_mo() -> float:
    """Espace libre RÉEL sur le volume, en Mo — pas une estimation."""
    try:
        _, _, libre = shutil.disk_usage(stockage.DOSSIER_DONNEES)
        return libre / (1024 * 1024)
    except Exception as e:
        log.warning(f"impossible de lire l'espace disque : {e}")
        return float("inf")  # ne bloque jamais le bot si la mesure échoue


def verifier_urgence_disque() -> bool:
    """
    Vérification indépendante du cycle normal. Retourne True si un
    nettoyage d'urgence a été déclenché (espace critique).
    """
    libre_mo = espace_disque_libre_mo()
    if libre_mo >= SEUIL_URGENCE_MO:
        return False

    log.critical(
        f"🚨 ESPACE DISQUE CRITIQUE : {libre_mo:.0f} Mo libres (< {SEUIL_URGENCE_MO}) "
        f"— nettoyage d'urgence immédiat de tous les fichiers surveillés"
    )
    for chemin in list(_fichiers_surveilles):
        statut, supprimees = _tronquer_si_necessaire(chemin, PLAFOND_URGENCE_LIGNES)

        if statut == "tronque":
            log.warning(
                f"🚨 Nettoyage d'urgence : {os.path.basename(chemin)} — "
                f"{supprimees} ligne(s) supprimée(s), {PLAFOND_URGENCE_LIGNES} conservée(s)"
            )
        elif statut == "echec_ecriture":
            # Seul ce cas précis justifie la suppression totale : le
            # disque était trop plein pour même écrire une version
            # tronquée plus petite. Un fichier "rien_a_faire" (déjà petit,
            # absent, ou vide) n'est JAMAIS supprimé pour cette raison.
            _supprimer_en_urgence(chemin)
        # statut == "rien_a_faire" : fichier déjà petit, absent ou vide —
        # aucune action, ce n'est pas lui qui pose problème.
    return True


async def boucle_surveillance(intervalle_normal_sec: float = 3600, intervalle_urgence_sec: float = 60):
    """
    À lancer au démarrage du bot :
        asyncio.create_task(gestion_fichiers.boucle_surveillance())

    Deux rythmes dans la même boucle : la vérification d'urgence (espace
    disque réel) tourne toutes les intervalle_urgence_sec (1 min par
    défaut) — rapide et bon marché, un simple appel système. La rotation
    normale par fichier ne tourne qu'une fois par heure, plus coûteuse
    (relit chaque fichier surveillé).
    """
    import asyncio

    derniere_verif_normale = 0.0
    while True:
        try:
            urgence_declenchee = verifier_urgence_disque()
            maintenant = time.time()
            if not urgence_declenchee and (maintenant - derniere_verif_normale) >= intervalle_normal_sec:
                verifier_toutes_les_rotations()
                derniere_verif_normale = maintenant
        except Exception:
            log.exception("erreur dans la boucle de surveillance des fichiers")
        await asyncio.sleep(intervalle_urgence_sec)
