"""
Durée de vie des spreads
==========================
Mesure depuis combien de temps un écart donné existe SANS INTERRUPTION.

POURQUOI C'EST UTILE
Un écart d'arbitrage normal se referme en quelques secondes : quelqu'un
l'exploite, les prix se rejoignent. Un écart qui dure des minutes ou des
heures est anormal — et l'anomalie est presque toujours la même : personne
ne PEUT l'exploiter (retraits fermés, token en délisting, liquidité
fantôme). C'est exactement ce que le filtre de retraits a confirmé sur COTI.

L'âge du spread est donc un signal INVERSÉ par rapport à l'intuition :
  - spread jeune (quelques secondes)  -> crédible, mais dur à attraper
  - spread ancien (plusieurs minutes) -> suspect, probablement bloqué

Ça complète le suivi 10s de suivi_opportunite.py, qui regarde ce qui se
passe APRÈS l'alerte. Ici on regarde ce qui s'est passé AVANT.

Aucun appel réseau : tout est calculé à partir des détections que le bot
fait déjà passer.
"""

import logging
import time

log = logging.getLogger("duree_spread")

# {(symbole, ex_achat, ex_vente): {"premiere_vue": ts, "derniere_vue": ts}}
_suivi: dict[tuple, dict] = {}

# Si un spread n'a pas été revu depuis ce délai, on considère qu'il a
# DISPARU puis réapparu : le chronomètre repart de zéro. Sans ça, un écart
# qui clignote toute la journée afficherait un âge de plusieurs heures alors
# qu'il n'a jamais tenu plus de deux secondes d'affilée.
TOLERANCE_INTERRUPTION_SEC = 30.0

# Au-delà, l'écart est signalé comme anormalement persistant
SEUIL_SUSPECT_SEC = 300.0  # 5 minutes

_dernier_purge = 0.0
_INTERVALLE_PURGE_SEC = 600
_AGE_MAX_ENTREE_SEC = 3600


def enregistrer(symbole: str, ex_achat: str, ex_vente: str) -> float:
    """
    Signale que ce spread est visible maintenant, et retourne son âge en
    secondes (0 s'il vient d'apparaître).

    Appelé à chaque détection : doit rester très rapide, sans await.
    """
    cle = (symbole, ex_achat, ex_vente)
    maintenant = time.time()
    entree = _suivi.get(cle)

    if entree is None or (maintenant - entree["derniere_vue"]) > TOLERANCE_INTERRUPTION_SEC:
        # Nouveau spread, ou réapparition après une interruption franche
        _suivi[cle] = {"premiere_vue": maintenant, "derniere_vue": maintenant}
        _purger(maintenant)
        return 0.0

    entree["derniere_vue"] = maintenant
    _purger(maintenant)
    return maintenant - entree["premiere_vue"]


def age(symbole: str, ex_achat: str, ex_vente: str) -> float | None:
    """Âge actuel sans rien modifier. None si jamais vu."""
    entree = _suivi.get((symbole, ex_achat, ex_vente))
    if entree is None:
        return None
    return time.time() - entree["premiere_vue"]


def est_suspect(age_sec: float | None) -> bool:
    return age_sec is not None and age_sec >= SEUIL_SUSPECT_SEC


def formater(age_sec: float | None) -> str:
    """Texte court pour les alertes Telegram."""
    if age_sec is None:
        return ""
    if age_sec < 60:
        duree = f"{age_sec:.0f}s"
    elif age_sec < 3600:
        duree = f"{age_sec / 60:.1f} min"
    else:
        duree = f"{age_sec / 3600:.1f} h"

    if est_suspect(age_sec):
        return f"⏳ Écart présent depuis {duree} — anormalement long, retrait probablement bloqué"
    return f"⏳ Écart présent depuis {duree}"


def _purger(maintenant: float):
    """Évite que le dictionnaire grossisse indéfiniment (service 24h/24)."""
    global _dernier_purge
    if maintenant - _dernier_purge < _INTERVALLE_PURGE_SEC:
        return
    _dernier_purge = maintenant
    perimees = [
        cle for cle, e in _suivi.items()
        if maintenant - e["derniere_vue"] > _AGE_MAX_ENTREE_SEC
    ]
    for cle in perimees:
        del _suivi[cle]
    if perimees:
        log.debug(f"Purge durée de spread : {len(perimees)} retirée(s), {len(_suivi)} suivie(s)")


def statistiques() -> dict:
    maintenant = time.time()
    ages = [maintenant - e["premiere_vue"] for e in _suivi.values()]
    return {
        "spreads_suivis": len(_suivi),
        "suspects": sum(1 for a in ages if a >= SEUIL_SUSPECT_SEC),
        "age_median_sec": round(sorted(ages)[len(ages) // 2], 1) if ages else 0,
    }
