"""
Sélection des plateformes actives pour l'arbitrage
=====================================================
Sépare deux choses qui étaient confondues jusqu'ici :
  - la COLLECTE de prix (WebSocket) — continue TOUJOURS sur les 10
    plateformes connectées, ça ne coûte rien de laisser tourner
  - la PARTICIPATION à la détection d'opportunités — filtrée ici

Une plateforme "désactivée" continue donc de recevoir ses prix en arrière-
plan (invisible, sans conséquence), mais aucune opportunité ne sera jamais
détectée ni alertée si elle implique cette plateforme, dans un sens ou
dans l'autre. C'est un choix de conception assumé : redémarrer une
connexion WebSocket à la volée aurait demandé une réécriture bien plus
lourde de main(), pour un bénéfice quasi nul (le prix d'une connexion WS
qui tourne pour rien est négligeable).

DEUX MODES
  "manuel" — toi seul décides, via le site ou l'API, quelles plateformes
             participent. Reste actif tant que tu ne repasses pas en auto.
  "auto"   — recalculé automatiquement toutes les 6h à partir du dernier
             classement (classement_exchanges.py) : seules les plateformes
             CONNECTÉES et dont le score dépasse SELECTION_SEUIL_SCORE
             participent. Si aucune ne dépasse le seuil à un instant donné
             (classement pas encore calculé, ou toutes sous le seuil), la
             sélection précédente est conservée plutôt que de tout couper —
             un bot qui n'alerte plus jamais serait pire que gardé
             temporairement sur une sélection légèrement obsolète.

Toute activation/désactivation manuelle bascule automatiquement le mode
sur "manuel" — pour qu'un réglage fait à la main ne soit jamais écrasé
silencieusement par le recalcul automatique 6h plus tard sans que tu t'en
rendes compte.
"""

import json
import logging
import os
import time

import stockage

log = logging.getLogger("selection_exchanges")

ETAT_PATH = stockage.chemin_donnees("selection_exchanges.json")

# Toutes les plateformes que le bot sait connecter — sert de référence pour
# valider les noms et pour le mode auto (on ne sélectionne jamais une
# plateforme non connectée, même si elle scorait bien dans le classement).
EXCHANGES_CONNECTES = (
    "binance", "bybit", "okx", "kucoin", "bitget", "gateio",
    "coinex", "bitvavo", "whitebit", "kraken",
)

_etat = {
    "mode": "manuel",  # "manuel" | "auto"
    "seuil_score": 50.0,
    "max_actives_auto": 12,  # plafond en mode auto — au-delà, seules les mieux notées sont gardées
    "actifs": list(EXCHANGES_CONNECTES),  # par défaut : tout actif, rien de cassé au premier démarrage
    "derniere_maj": 0.0,
}


def _sauvegarder():
    try:
        with open(ETAT_PATH, "w", encoding="utf-8") as f:
            json.dump(_etat, f, indent=2)
    except Exception as e:
        log.error(f"Échec sauvegarde de la sélection : {e}")


def charger():
    """À appeler une fois au démarrage du bot."""
    global _etat
    if not os.path.exists(ETAT_PATH):
        _sauvegarder()  # crée le fichier avec l'état par défaut, pour qu'il existe dès le premier lancement
        return
    try:
        with open(ETAT_PATH, encoding="utf-8") as f:
            charge = json.load(f)
        if isinstance(charge, dict) and "actifs" in charge:
            _etat.update(charge)
            _etat.setdefault("max_actives_auto", 12)  # ancien fichier sauvegardé avant l'ajout du plafond
            # Filtre défensif : si un vieux fichier contient une plateforme
            # retirée depuis, ou une entrée corrompue, on ne la garde pas.
            _etat["actifs"] = [e for e in _etat["actifs"] if e in EXCHANGES_CONNECTES]
            log.info(
                f"Sélection rechargée : mode={_etat['mode']}, "
                f"{len(_etat['actifs'])}/{len(EXCHANGES_CONNECTES)} actives"
            )
    except Exception as e:
        log.error(f"Échec chargement de la sélection : {e}")


def etat() -> dict:
    return dict(_etat)


def est_actif(exchange: str) -> bool:
    return exchange in _etat["actifs"]


def activer(exchange: str) -> tuple[bool, str]:
    if exchange not in EXCHANGES_CONNECTES:
        return False, f"'{exchange}' n'est pas une plateforme connectée"
    if exchange not in _etat["actifs"]:
        _etat["actifs"].append(exchange)
    _etat["mode"] = "manuel"  # un réglage manuel doit rester manuel
    _etat["derniere_maj"] = time.time()
    _sauvegarder()
    log.info(f"{exchange} activé manuellement ({len(_etat['actifs'])}/{len(EXCHANGES_CONNECTES)} actives)")
    return True, ""


def desactiver(exchange: str) -> tuple[bool, str]:
    if exchange not in EXCHANGES_CONNECTES:
        return False, f"'{exchange}' n'est pas une plateforme connectée"
    if len(_etat["actifs"]) <= 1 and exchange in _etat["actifs"]:
        # Un bot avec 0 plateforme active ne détecterait plus jamais rien
        # sans avertissement clair — mieux vaut refuser explicitement.
        return False, "impossible de désactiver la dernière plateforme active"
    if exchange in _etat["actifs"]:
        _etat["actifs"].remove(exchange)
    _etat["mode"] = "manuel"
    _etat["derniere_maj"] = time.time()
    _sauvegarder()
    log.info(f"{exchange} désactivé manuellement ({len(_etat['actifs'])}/{len(EXCHANGES_CONNECTES)} actives)")
    return True, ""


def definir_mode(mode: str, seuil_score: float | None = None, max_actives_auto: int | None = None) -> tuple[bool, str]:
    if mode not in ("manuel", "auto"):
        return False, "mode doit être 'manuel' ou 'auto'"
    _etat["mode"] = mode
    if seuil_score is not None:
        _etat["seuil_score"] = float(seuil_score)
    if max_actives_auto is not None:
        if max_actives_auto < 1:
            return False, "max_actives_auto doit être au moins 1"
        _etat["max_actives_auto"] = int(max_actives_auto)
    _etat["derniere_maj"] = time.time()
    _sauvegarder()
    log.info(
        f"Mode de sélection changé : {mode} "
        f"(seuil={_etat['seuil_score']}, max={_etat['max_actives_auto']})"
    )
    if mode == "auto":
        recalculer_auto()
    return True, ""


def recalculer_auto():
    """
    Recalcule la sélection à partir du dernier classement disponible.
    Ne fait RIEN si le mode est "manuel" — appelé sans risque depuis la
    boucle périodique de classement_exchanges, même en mode manuel.
    """
    if _etat["mode"] != "auto":
        return

    try:
        import classement_exchanges
        resultat = classement_exchanges.dernier_resultat()
    except Exception as e:
        log.warning(f"recalculer_auto : classement indisponible ({e})")
        return

    if resultat is None:
        log.info("recalculer_auto : premier classement pas encore calculé, sélection inchangée")
        return

    qualifiees_avec_score = sorted(
        (
            (ligne["exchange"], ligne["score"]) for ligne in resultat["classement"]
            if ligne["exchange"] in EXCHANGES_CONNECTES and ligne["score"] >= _etat["seuil_score"]
        ),
        key=lambda paire: paire[1], reverse=True,
    )

    if not qualifiees_avec_score:
        log.warning(
            f"recalculer_auto : aucune plateforme connectée ne dépasse "
            f"{_etat['seuil_score']} — sélection précédente conservée pour "
            f"éviter un bot qui n'alerte plus jamais"
        )
        return

    # Plafond : au-delà de max_actives_auto, on garde les mieux notées —
    # qualifiees_avec_score est déjà trié par score décroissant.
    plafond = _etat.get("max_actives_auto", 12)
    qualifiees = [nom for nom, _score in qualifiees_avec_score[:plafond]]
    if len(qualifiees_avec_score) > plafond:
        log.info(
            f"recalculer_auto : {len(qualifiees_avec_score)} plateformes qualifiées, "
            f"plafonné à {plafond} — écartées : "
            f"{', '.join(nom for nom, _ in qualifiees_avec_score[plafond:])}"
        )

    ancien = set(_etat["actifs"])
    _etat["actifs"] = qualifiees
    _etat["derniere_maj"] = time.time()
    _sauvegarder()

    nouveau = set(qualifiees)
    if ancien != nouveau:
        ajoutees = nouveau - ancien
        retirees = ancien - nouveau
        log.info(
            f"Sélection auto recalculée : {len(qualifiees)} actives"
            + (f" | ajoutées : {', '.join(sorted(ajoutees))}" if ajoutees else "")
            + (f" | retirées : {', '.join(sorted(retirees))}" if retirees else "")
        )
