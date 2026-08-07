"""
Diffusion en direct des écarts d'arbitrage (WebSocket)
============================================================
Contrairement au reste du dashboard (rafraîchi toutes les 8s par polling),
le panneau "Cryptos suivies" a besoin d'un vrai flux instantané — comme un
exchange. Ce module fait le lien entre bot_fusionne_v1.py (qui calcule les
écarts) et api_server.py (qui gère les connexions WebSocket des
navigateurs), sans dépendance circulaire entre les deux.

Comme tout tourne dans le MÊME process/event loop asyncio (api_server.py
est lancé via asyncio.create_task() depuis bot_fusionne_v1.py, pas un
process séparé), la diffusion est un simple appel de fonction async —
pas besoin de file de messages ni d'IPC.

Ne bloque JAMAIS le bot : une connexion cassée est juste retirée en
silence, jamais une exception qui remonterait et interromprait le calcul
d'arbitrage en cours.

⚠️ FILTRE DE CIRCULATION (ajouté le 07/08)
Ce module lui-même ne filtre rien — c'est bot_fusionne_v1.py qui décide,
AVANT d'appeler diffuser_spread(), si la crypto doit apparaître : seules
celles dont le retrait/dépôt entre les deux plateformes est VÉRIFIÉ ouvert
(verif_retraits.py, même critère que pour les alertes) sont diffusées.
Si une crypto affichée devient non circulable, retirer_spread() la retire
ACTIVEMENT plutôt que de simplement arrêter ses mises à jour — sinon elle
resterait visible avec une donnée périmée, contrairement à l'objectif du
filtre (« uniquement les cryptos disponibles sur le site »).
"""

import json
import logging
import time

log = logging.getLogger("spreads_live")

_connexions_ws: set = set()
_derniers_spreads: dict = {}  # {symbole: {...}} — snapshot pour un client qui vient de se connecter


def enregistrer_connexion(ws):
    _connexions_ws.add(ws)


def retirer_connexion(ws):
    _connexions_ws.discard(ws)


def nb_connexions() -> int:
    return len(_connexions_ws)


def obtenir_etat_actuel() -> list:
    """Snapshot de tous les derniers écarts connus — envoyé à un client qui vient de se connecter."""
    return list(_derniers_spreads.values())


async def diffuser_spread(symbole: str, spread_net_pct: float, exchanges: list, seuil_actif: float):
    """
    Met à jour le cache ET pousse aux clients connectés — mais UNIQUEMENT si
    la valeur a vraiment changé depuis la dernière diffusion (évite de
    spammer les clients à chaque cycle si rien n'a bougé pour cette crypto).
    """
    arrondi = round(spread_net_pct, 4)
    precedent = _derniers_spreads.get(symbole)
    if precedent and precedent["spread_net_pct"] == arrondi:
        return

    donnee = {
        "symbole": symbole,
        "spread_net_pct": arrondi,
        "exchanges": exchanges,
        "au_dessus_seuil": spread_net_pct >= seuil_actif,
        "timestamp": time.time(),
    }
    _derniers_spreads[symbole] = donnee

    if not _connexions_ws:
        return

    message = json.dumps({"type": "spread", "data": donnee})
    morts = set()
    for ws in list(_connexions_ws):
        try:
            await ws.send_str(message)
        except Exception:
            morts.add(ws)
    for ws in morts:
        _connexions_ws.discard(ws)


async def retirer_spread(symbole: str):
    """
    Retire ACTIVEMENT une crypto du panneau — pas juste « on arrête de la
    mettre à jour ». Nécessaire quand une crypto autrefois affichée devient
    non circulable (retrait/dépôt fermé entre-temps) : sans ce retrait
    explicite, elle resterait visible indéfiniment avec une donnée périmée,
    et un client qui se connecte APRÈS le changement la verrait encore dans
    obtenir_etat_actuel() alors qu'elle n'est plus « disponible ».
    """
    if symbole not in _derniers_spreads:
        return  # jamais affichée, ou déjà retirée — rien à faire
    del _derniers_spreads[symbole]

    if not _connexions_ws:
        return

    message = json.dumps({"type": "spread_retire", "data": {"symbole": symbole}})
    morts = set()
    for ws in list(_connexions_ws):
        try:
            await ws.send_str(message)
        except Exception:
            morts.add(ws)
    for ws in morts:
        _connexions_ws.discard(ws)
