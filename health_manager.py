"""
Gestionnaire de santé des paires
=====================================
Surveille en continu si chaque paire reçoit encore des prix frais sur
chaque exchange. Si une paire ne reçoit plus rien pendant trop longtemps
(panne probable : paire retirée, bug de souscription, etc.), elle est
ajoutée à une blacklist persistante (fichier JSON).

Au prochain redémarrage du bot, symbol_discovery.py exclut automatiquement
ces paires blacklistées et les remplace par d'autres candidates de
l'intersection — remplacement automatique, mais au redémarrage (pas en
live, voir discussion sur la complexité du hot-swap).
"""

import json
import logging
import os
import time

import stockage
from config import (
    SEUIL_PANNE_SEC, TTL_BLACKLIST_SEC, SEUIL_PERSISTANCE_SUSPECTE_SEC, GRACE_PERIODE_SEC,
)

log = logging.getLogger("health_manager")

BLACKLIST_PATH = stockage.chemin_donnees("paires_blacklist.json")

# Interrupteur général du blacklistage AUTOMATIQUE (persistance suspecte + pannes
# de connexion). DÉSACTIVÉ par défaut le 31/07 sur demande. Le désactiver n'efface
# PAS la blacklist existante et ne bloque PAS le blacklistage/déblacklistage
# MANUEL (actions explicites) — ça bloque uniquement les ajouts automatiques du
# bot lui-même. Réactivable via le bouton dans Settings du dashboard.
_blacklist_auto_active = False


def definir_blacklist_active(actif: bool):
    global _blacklist_auto_active
    _blacklist_auto_active = bool(actif)
    log.info(f"Blacklistage automatique : {'activé' if _blacklist_auto_active else 'désactivé'}")


def blacklist_active() -> bool:
    return _blacklist_auto_active

# Chaque entrée : {"debut": timestamp première détection, "dernier_vu": timestamp dernière détection}
# Le "dernier_vu" permet de tolérer de courtes absences (1-2 cycles) sans
# réinitialiser tout le compteur — sinon une paire un peu moins liquide
# (prix qui arrive de façon espacée) peut échapper indéfiniment à la détection.
_opportunites_persistantes: dict[str, dict] = {}


def charger_blacklist(ignorer_expiration: bool = False) -> dict:
    """Charge la blacklist, en retirant automatiquement les entrées expirées (> TTL_BLACKLIST_SEC)."""
    if not os.path.exists(BLACKLIST_PATH):
        return {}
    try:
        with open(BLACKLIST_PATH, "r", encoding="utf-8") as f:
            brute = json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}

    if ignorer_expiration:
        return brute

    maintenant = time.time()
    valides = {
        symbole: info for symbole, info in brute.items()
        if maintenant - info.get("detecte_le", 0) < TTL_BLACKLIST_SEC
    }

    # Si des entrées ont expiré, on réécrit le fichier nettoyé
    if len(valides) != len(brute):
        sauvegarder_blacklist(valides)
        log.info(f"Blacklist : {len(brute) - len(valides)} entrée(s) expirée(s) retirée(s) automatiquement")

    return valides


def sauvegarder_blacklist(blacklist: dict):
    with open(BLACKLIST_PATH, "w", encoding="utf-8") as f:
        json.dump(blacklist, f, indent=2)


def symboles_blacklistes() -> set[str]:
    """Retourne l'ensemble des symboles à exclure (peu importe l'exchange en panne), hors expirés."""
    return set(charger_blacklist().keys())


def vider_blacklist():
    """Vide complètement la blacklist — utile si un bug a exclu trop de paires par erreur."""
    sauvegarder_blacklist({})
    log.info("Blacklist entièrement réinitialisée")


async def surveiller_sante(prix_live: dict, intervalle_sec: float = 30.0):
    """
    Boucle continue : vérifie que chaque paire de chaque exchange reçoit
    encore des prix récents (panne = silence total). La détection des
    "faux" écarts qui ne se referment jamais se fait séparément via
    signaler_opportunite_active(), appelée depuis le scanner d'arbitrage
    à chaque cycle (toutes les 1s, plus réactif que ce check-ci).
    """
    import asyncio

    while True:
        await asyncio.sleep(intervalle_sec)

        if not _blacklist_auto_active:
            continue  # blacklistage auto désactivé — on ne surveille pas les pannes pour autant, juste on n'agit pas

        blacklist = charger_blacklist()
        modifie = False

        for exchange, symbols_data in prix_live.items():
            for symbol, data in symbols_data.items():
                age = time.time() - data.get("timestamp", 0)
                if age > SEUIL_PANNE_SEC and symbol not in blacklist:
                    blacklist[symbol] = {
                        "raison": f"Aucun prix reçu depuis {age:.0f}s sur {exchange}",
                        "detecte_le": time.time(),
                    }
                    modifie = True
                    log.warning(f"⚠️ Panne détectée : {symbol} sur {exchange} (silence {age:.0f}s)")

        if modifie:
            sauvegarder_blacklist(blacklist)


def signaler_opportunite_active(cle_opportunite: str, symbol: str) -> bool:
    """
    À appeler à CHAQUE cycle du scanner (1x/s) pour chaque opportunité détectée.
    Si la MÊME opportunité (même symbole + mêmes exchanges) reste active en
    continu (avec une tolérance de quelques secondes d'absence — voir
    nettoyer_opportunites_expirees) plus de SEUIL_PERSISTANCE_SUSPECTE_SEC,
    elle est blacklistée — signal fiable d'un bug de flux ou d'une collision
    de ticker (même symbole = crypto différente selon l'exchange), pas d'un
    vrai écart d'arbitrage.

    Retourne True si la paire vient d'être blacklistée à cet appel.
    """
    maintenant = time.time()

    if cle_opportunite not in _opportunites_persistantes:
        _opportunites_persistantes[cle_opportunite] = {"debut": maintenant, "dernier_vu": maintenant}
        return False

    info = _opportunites_persistantes[cle_opportunite]
    info["dernier_vu"] = maintenant
    duree = maintenant - info["debut"]

    if duree > SEUIL_PERSISTANCE_SUSPECTE_SEC:
        if not _blacklist_auto_active:
            return False  # blacklistage auto désactivé — on suit toujours la persistance, mais on n'agit pas dessus
        blacklist = charger_blacklist()
        if symbol not in blacklist:
            blacklist[symbol] = {
                "raison": f"Écart persistant depuis {duree:.0f}s sans jamais se refermer — probable bug de flux ou collision de ticker, pas une vraie opportunité",
                "detecte_le": maintenant,
            }
            sauvegarder_blacklist(blacklist)
            log.warning(
                f"⚠️ Opportunité SUSPECTE (persiste {duree:.0f}s sans se refermer) : "
                f"{symbol} — blacklisté"
            )
            return True
    return False


def nettoyer_opportunites_expirees(grace_sec: float = GRACE_PERIODE_SEC):
    """
    À appeler périodiquement depuis le scanner (pas besoin de tracker les
    clés actives/inactives manuellement). Retire du suivi les opportunités
    qui n'ont VRAIMENT plus été vues depuis grace_sec (pas juste absentes
    d'un seul cycle à cause d'un prix pas encore rafraîchi).
    """
    maintenant = time.time()
    expirees = [
        cle for cle, info in _opportunites_persistantes.items()
        if maintenant - info["dernier_vu"] > grace_sec
    ]
    for cle in expirees:
        del _opportunites_persistantes[cle]


def blacklister_manuellement(symbol: str, raison: str = "Blacklisté manuellement"):
    """Pour exclure immédiatement une paire suspecte sans attendre la détection automatique."""
    blacklist = charger_blacklist()
    blacklist[symbol] = {"raison": raison, "detecte_le": time.time()}
    sauvegarder_blacklist(blacklist)
    log.warning(f"⚠️ {symbol} blacklisté manuellement : {raison}")


def retirer_de_la_blacklist(symbol: str):
    """Utile si tu veux redonner sa chance à une paire manuellement."""
    blacklist = charger_blacklist()
    if symbol in blacklist:
        del blacklist[symbol]
        sauvegarder_blacklist(blacklist)
        log.info(f"{symbol} retiré de la blacklist")


if __name__ == "__main__":
    bl = charger_blacklist()
    if not bl:
        print("Aucune paire blacklistée pour l'instant.")
    else:
        print(f"{len(bl)} paire(s) blacklistée(s) :")
        for symbol, info in bl.items():
            print(f"  • {symbol} : {info['raison']}")
