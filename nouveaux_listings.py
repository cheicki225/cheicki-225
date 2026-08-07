"""
Détection des nouveaux listings
=================================
Repère les paires qui APPARAISSENT sur une plateforme où elles n'existaient
pas au relevé précédent.

POURQUOI CETTE APPROCHE PLUTÔT QUE LES PAGES D'ANNONCES
On pourrait scraper les pages « Nouvelles cotations » de chaque plateforme,
mais c'est fragile : format HTML changeant, protections anti-bot, langues
différentes, et surtout l'annonce précède parfois la cotation de plusieurs
jours. Ici on compare simplement la liste des paires réellement disponibles
à celle du relevé précédent. Une paire qui apparaît est une paire
effectivement cotée, maintenant, sur cette plateforme — c'est factuel et ça
réutilise les fonctions que le bot appelle déjà (symbol_discovery).

POURQUOI ÇA COMPTE POUR L'ARBITRAGE
Les minutes et heures qui suivent une nouvelle cotation sont le moment où
les écarts entre plateformes sont les plus larges : le carnet est encore
mince, le prix n'a pas convergé avec celui des autres plateformes, et peu
de monde surveille. C'est une des rares situations où un écart important
est réel plutôt que le symptôme d'un blocage.

⚠️ MAIS C'EST AUSSI LE MOMENT LE PLUS RISQUÉ
  - la volatilité est extrême dans les premières heures
  - la liquidité est très faible : le carnet se vide en un ordre
  - les RETRAITS sont fréquemment fermés au début d'une cotation, ce qui
    rend l'arbitrage inter-plateformes impossible malgré l'écart affiché
Le module signale donc l'événement, il ne dit pas que c'est exploitable.
Croise toujours avec verif_retraits avant d'en tirer une conclusion.
"""

import asyncio
import json
import logging
import os
import time

import stockage
import symbol_discovery

log = logging.getLogger("nouveaux_listings")

ETAT_PATH = stockage.chemin_donnees("listings_connus.json")

# {exchange: set(symboles)} — chargé au démarrage, sauvegardé à chaque relevé
_connus: dict[str, set] = {}
_premier_releve_fait = False

RECUPERATEURS = {
    "binance": symbol_discovery.paires_binance,
    "bybit": symbol_discovery.paires_bybit,
    "okx": symbol_discovery.paires_okx,
    "kucoin": symbol_discovery.paires_kucoin,
    "bitget": symbol_discovery.paires_bitget,
    "gateio": symbol_discovery.paires_gateio,
    "coinex": symbol_discovery.paires_coinex,
}


def _charger():
    global _connus, _premier_releve_fait
    if not os.path.exists(ETAT_PATH):
        return
    try:
        with open(ETAT_PATH, encoding="utf-8") as f:
            donnees = json.load(f)
        _connus = {ex: set(symboles) for ex, symboles in donnees.items()}
        _premier_releve_fait = bool(_connus)
        total = sum(len(s) for s in _connus.values())
        log.info(f"Listings connus rechargés : {total} paires sur {len(_connus)} plateformes")
    except Exception as e:
        log.error(f"Échec chargement des listings connus : {e}")
        _connus = {}


def _sauvegarder():
    try:
        with open(ETAT_PATH, "w", encoding="utf-8") as f:
            json.dump({ex: sorted(s) for ex, s in _connus.items()}, f)
    except Exception as e:
        log.error(f"Échec sauvegarde des listings connus : {e}")


async def _relever() -> dict[str, set]:
    """Liste des paires actuellement cotées, par plateforme."""
    resultats = await asyncio.gather(
        *(f() for f in RECUPERATEURS.values()), return_exceptions=True
    )
    actuel = {}
    for exchange, r in zip(RECUPERATEURS, resultats):
        if isinstance(r, Exception):
            log.warning(f"listings {exchange} : échec ({r})")
            continue
        actuel[exchange] = set(r.keys())
    return actuel


async def verifier() -> list[dict]:
    """
    Compare le relevé actuel au précédent et retourne les nouveautés.

    Au TOUT PREMIER relevé, on enregistre sans rien signaler : sinon les
    ~3000 paires existantes seraient toutes annoncées comme nouvelles.
    """
    global _premier_releve_fait

    actuel = await _relever()
    if not actuel:
        return []

    nouveautes = []

    if not _premier_releve_fait:
        _connus.update(actuel)
        _premier_releve_fait = True
        _sauvegarder()
        total = sum(len(s) for s in actuel.values())
        log.info(f"Premier relevé : {total} paires enregistrées (aucune alerte, c'est la référence)")
        return []

    for exchange, symboles in actuel.items():
        anciens = _connus.get(exchange)
        if anciens is None:
            # Plateforme jamais relevée (indisponible jusqu'ici) : on
            # enregistre sans alerter, même raison que le premier relevé.
            _connus[exchange] = symboles
            log.info(f"listings {exchange} : première lecture, {len(symboles)} paires enregistrées")
            continue

        apparues = symboles - anciens
        # Garde-fou : si des centaines de paires "apparaissent" d'un coup,
        # c'est presque sûrement une réponse d'API partielle au relevé
        # précédent, pas une vague de cotations. On met à jour sans alerter.
        if len(apparues) > 50:
            log.warning(
                f"listings {exchange} : {len(apparues)} paires apparues d'un coup — "
                f"relevé précédent probablement incomplet, aucune alerte envoyée"
            )
        else:
            for symbole in sorted(apparues):
                nouveautes.append({
                    "exchange": exchange, "symbole": symbole, "timestamp": time.time(),
                    "aussi_sur": sorted(
                        ex for ex, s in actuel.items() if ex != exchange and symbole in s
                    ),
                })

        _connus[exchange] = symboles

    if nouveautes:
        _sauvegarder()
    return nouveautes


def _formater(nouveaute: dict) -> str:
    autres = nouveaute["aussi_sur"]
    lignes = [
        f"🆕 <b>Nouvelle cotation</b> — {nouveaute['symbole']}",
        f"Plateforme : {nouveaute['exchange']}",
    ]
    if autres:
        lignes.append(f"Déjà coté sur : {', '.join(autres)}")
        lignes.append("\n<i>Les écarts sont souvent larges dans les premières heures, "
                      "mais la liquidité est très faible et les retraits sont "
                      "fréquemment fermés au début d'une cotation.</i>")
    else:
        lignes.append("Coté nulle part ailleurs pour l'instant")
        lignes.append("\n<i>Aucun arbitrage inter-plateformes possible tant qu'il "
                      "n'est coté que sur une seule plateforme.</i>")
    return "\n".join(lignes)


async def boucle(intervalle_sec: float = 1800):
    """
    Relève toutes les 30 minutes par défaut. À lancer au démarrage :
        asyncio.create_task(nouveaux_listings.boucle())
    """
    _charger()
    log.info(f"🆕 Détection des nouvelles cotations démarrée (relevé toutes les {intervalle_sec / 60:.0f} min)")

    while True:
        try:
            for nouveaute in await verifier():
                log.info(
                    f"🆕 Nouvelle cotation : {nouveaute['symbole']} sur {nouveaute['exchange']} "
                    f"(aussi sur : {', '.join(nouveaute['aussi_sur']) or 'nulle part'})"
                )
                try:
                    import telegram_notifier
                    await telegram_notifier.envoyer_message_simple(_formater(nouveaute))
                except Exception as e:
                    log.error(f"Échec notification de cotation : {e}")
        except Exception as e:
            log.error(f"Erreur boucle listings : {e}")
        await asyncio.sleep(intervalle_sec)


def statistiques() -> dict:
    return {
        "plateformes_suivies": len(_connus),
        "paires_connues": sum(len(s) for s in _connus.values()),
        "reference_etablie": _premier_releve_fait,
    }
