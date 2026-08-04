"""
Arbitrage spot-futures (perpétuel) et récolte de funding
==========================================================
POURQUOI CE MODULE EXISTE
Tout le reste du bot bute sur le même mur : pour boucler un arbitrage entre
deux plateformes, il faut TRANSFÉRER le token. D'où les retraits fermés
(COTI est prisonnier de kucoin), les 5 à 30 minutes de transfert pendant
lesquelles l'écart s'évapore, et les frais fixes qui imposent un point mort
autour de 2%.

L'arbitrage spot-futures supprime ce mur : les deux jambes sont ouvertes sur
LA MÊME plateforme. Aucun transfert, aucun retrait, aucun délai blockchain.

DEUX STRATÉGIES SUIVIES ICI

1. BASE (basis) — le perpétuel cote au-dessus du spot
   On achète au comptant et on vend à découvert le perpétuel au même moment.
   La position est neutre en direction : peu importe que le prix monte ou
   descende, les deux jambes se compensent. Le gain vient du retour du
   perpétuel vers le spot (les deux convergent mécaniquement).

2. FUNDING — on est payé pour tenir la position
   Sur un perpétuel, un taux de financement s'échange entre longs et shorts
   toutes les 8 heures (4h ou 1h selon la plateforme). Quand il est positif,
   les longs paient les shorts. Étant short sur le perpétuel et long sur le
   spot, on encaisse ce financement tant que la position reste ouverte.

Les deux se cumulent : on entre sur une base positive, on encaisse le
funding en attendant la convergence.

CHOIX TECHNIQUE : REST PLUTÔT QUE WEBSOCKET
Le reste du bot utilise des WebSockets parce que l'arbitrage inter-plateformes
se joue à la seconde. Ici, non : une position de funding se tient des heures
ou des jours, et le taux ne change qu'à chaque période de financement. Un
sondage REST toutes les 60 secondes est amplement suffisant et évite
d'ajouter 6 connexions permanentes à un bot qui en gère déjà 6.

⚠️ RISQUES PROPRES À CETTE STRATÉGIE — À LIRE
Ce n'est PAS sans risque, contrairement à ce que laissent entendre certaines
publicités qui annoncent 10% par mois :
  - LIQUIDATION : la jambe short est à effet de levier. Si le prix monte
    fortement et que la marge est insuffisante, elle est liquidée — et il
    ne reste que la jambe spot, donc une position directionnelle non voulue.
    C'est le mode d'échec le plus courant et le plus coûteux.
  - INVERSION DU FUNDING : un taux positif peut devenir négatif ; on passe
    alors de payé à payeur.
  - CAPITAL IMMOBILISÉ des deux côtés, pendant des jours parfois.
  - La base peut s'ÉCARTER avant de converger, avec des appels de marge.
Ce module ne fait que DÉTECTER et SIMULER. Aucun ordre n'est passé.
"""

import asyncio
import csv
import logging
import os
import time

import aiohttp

import stockage
from config import (
    FRAIS_TRADING_PCT, PERP_SEUIL_BASE_PCT, PERP_SEUIL_FUNDING_APR_PCT,
    PERP_INTERVALLE_SONDAGE_SEC, PERP_MONTANT_USDT, PERP_NOTIFIER,
)

log = logging.getLogger("arbitrage_perpetuel")

CSV_PATH = stockage.chemin_donnees("opportunites_perpetuel.csv")
COLONNES = [
    "timestamp", "exchange", "symbole", "prix_spot", "prix_perp",
    "base_pct", "funding_pct", "funding_apr_pct", "periodes_par_jour",
    "frais_aller_retour_pct", "gain_net_estime_pct", "type_signal",
]

TIMEOUT = aiohttp.ClientTimeout(total=20)

# Dernier instantané par plateforme : {symbole: {...}}
_perp_live: dict[str, dict] = {}
_derniere_maj: float = 0.0

# Anti-spam : dernière alerte par (exchange, symbole)
_dernieres_alertes: dict[tuple, float] = {}
COOLDOWN_ALERTE_SEC = 1800  # 30 min — un funding ne change qu'aux périodes


def _init_csv():
    if not os.path.exists(CSV_PATH):
        with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(COLONNES)


# ============================================================
# PARSERS — endpoints PUBLICS, aucune clé API
# ============================================================
# Chaque parser renvoie {symbole: {"prix": float, "funding": float,
#                                  "periodes_par_jour": int}}
# `funding` est le taux de la PÉRIODE en pourcentage (ex: 0.01 = 0.01%).

def _parser_binance(data) -> dict:
    """/fapi/v1/premiumIndex — funding toutes les 8h (3x/jour)."""
    resultat = {}
    if not isinstance(data, list):
        return resultat
    for item in data:
        try:
            symbole = str(item["symbol"]).upper()
            if not symbole.endswith("USDT"):
                continue
            resultat[symbole] = {
                "prix": float(item["markPrice"]),
                "funding": float(item["lastFundingRate"]) * 100,
                "periodes_par_jour": 3,
            }
        except (KeyError, TypeError, ValueError):
            continue
    return resultat


def _parser_bybit(data) -> dict:
    """/v5/market/tickers?category=linear — funding 8h."""
    resultat = {}
    try:
        liste = data["result"]["list"]
    except (KeyError, TypeError):
        return resultat
    for item in liste:
        try:
            symbole = str(item["symbol"]).upper()
            if not symbole.endswith("USDT"):
                continue
            resultat[symbole] = {
                "prix": float(item["markPrice"]),
                "funding": float(item["fundingRate"]) * 100,
                "periodes_par_jour": 3,
            }
        except (KeyError, TypeError, ValueError):
            continue
    return resultat


def _parser_okx(data) -> dict:
    """
    /api/v5/public/mark-price?instType=SWAP
    OKX nomme ses instruments BTC-USDT-SWAP : on normalise en BTCUSDT.
    Le funding n'est PAS dans cette réponse — il faut un second appel par
    instrument, ce qui serait bien trop coûteux. On ne garde donc que le
    prix : les signaux de BASE fonctionnent, ceux de FUNDING sont ignorés
    pour OKX (funding laissé à None plutôt qu'inventé à 0).
    """
    resultat = {}
    try:
        liste = data["data"]
    except (KeyError, TypeError):
        return resultat
    for item in liste:
        try:
            inst = str(item["instId"]).upper()
            if not inst.endswith("-USDT-SWAP"):
                continue
            symbole = inst.replace("-USDT-SWAP", "") + "USDT"
            resultat[symbole] = {
                "prix": float(item["markPx"]),
                "funding": None,
                "periodes_par_jour": 3,
            }
        except (KeyError, TypeError, ValueError):
            continue
    return resultat


def _parser_bitget(data) -> dict:
    """/api/v2/mix/market/tickers?productType=USDT-FUTURES"""
    resultat = {}
    try:
        liste = data["data"]
    except (KeyError, TypeError):
        return resultat
    for item in liste:
        try:
            symbole = str(item["symbol"]).upper()
            if not symbole.endswith("USDT"):
                continue
            funding = item.get("fundingRate")
            resultat[symbole] = {
                "prix": float(item.get("markPrice") or item["lastPr"]),
                "funding": float(funding) * 100 if funding is not None else None,
                "periodes_par_jour": 3,
            }
        except (KeyError, TypeError, ValueError):
            continue
    return resultat


def _parser_gateio(data) -> dict:
    """/api/v4/futures/usdt/contracts — nomme BTC_USDT."""
    resultat = {}
    if not isinstance(data, list):
        return resultat
    for item in data:
        try:
            nom = str(item["name"]).upper()
            if not nom.endswith("_USDT"):
                continue
            symbole = nom.replace("_USDT", "") + "USDT"
            funding = item.get("funding_rate")
            # funding_interval est en SECONDES chez Gate.io
            intervalle = int(item.get("funding_interval") or 28800)
            resultat[symbole] = {
                "prix": float(item["mark_price"]),
                "funding": float(funding) * 100 if funding is not None else None,
                "periodes_par_jour": max(1, round(86400 / intervalle)),
            }
        except (KeyError, TypeError, ValueError, ZeroDivisionError):
            continue
    return resultat


SOURCES = {
    "binance": ("https://fapi.binance.com/fapi/v1/premiumIndex", _parser_binance),
    "bybit": ("https://api.bybit.com/v5/market/tickers?category=linear", _parser_bybit),
    "okx": ("https://www.okx.com/api/v5/public/mark-price?instType=SWAP", _parser_okx),
    "bitget": ("https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES", _parser_bitget),
    "gateio": ("https://api.gateio.ws/api/v4/futures/usdt/contracts", _parser_gateio),
}
# KuCoin Futures utilise des noms de contrats spécifiques (XBTUSDTM...) et
# une correspondance moins directe : volontairement écarté pour l'instant
# plutôt que mal supporté.


# ============================================================
# RÉCUPÉRATION
# ============================================================
async def _telecharger_un(session, exchange: str, url: str, parser):
    try:
        async with session.get(url, timeout=TIMEOUT) as resp:
            if resp.status != 200:
                log.warning(f"perp {exchange} : statut HTTP {resp.status}")
                return exchange, {}
            return exchange, parser(await resp.json(content_type=None))
    except Exception as e:
        log.warning(f"perp {exchange} : échec ({e})")
        return exchange, {}


async def rafraichir():
    """Recharge prix perpétuels et funding pour toutes les plateformes."""
    global _perp_live, _derniere_maj
    async with aiohttp.ClientSession() as session:
        resultats = await asyncio.gather(*(
            _telecharger_un(session, ex, url, parser)
            for ex, (url, parser) in SOURCES.items()
        ))

    nouveau = {ex: data for ex, data in resultats if data}
    if not nouveau:
        log.warning("perp : aucune plateforme n'a répondu")
        return

    _perp_live = nouveau
    _derniere_maj = time.time()
    total = sum(len(d) for d in nouveau.values())
    log.info(
        f"perp : {len(nouveau)}/{len(SOURCES)} plateformes, "
        f"{total} contrats ({', '.join(f'{ex}:{len(d)}' for ex, d in nouveau.items())})"
    )


def donnees_disponibles() -> bool:
    return bool(_perp_live)


# ============================================================
# DÉTECTION
# ============================================================
def _frais_aller_retour_pct(exchange: str) -> float:
    """
    Coût total d'un cycle complet, en % du montant engagé.

    4 opérations : achat spot, vente à découvert du perp, puis rachat du perp
    et revente du spot à la clôture. On applique le tarif spot de la
    plateforme aux quatre — les frais futures sont en général plus BAS
    (souvent ~0.02-0.06% en taker), donc cette estimation est prudente,
    ce qui est le bon sens de l'erreur.
    """
    return FRAIS_TRADING_PCT.get(exchange, 0.10) * 4


def detecter(prix_live: dict) -> list[dict]:
    """
    Croise le cache spot du bot (prix_live) avec les perpétuels.
    Retourne la liste des opportunités dépassant les seuils.
    """
    opportunites = []
    if not _perp_live:
        return opportunites

    for exchange, contrats in _perp_live.items():
        spots = prix_live.get(exchange)
        if not spots:
            continue

        frais = _frais_aller_retour_pct(exchange)

        for symbole, perp in contrats.items():
            spot = spots.get(symbole)
            if not spot:
                continue
            # Prix d'achat au comptant = le ask (ce qu'on paie réellement)
            prix_spot = spot.get("ask") or 0
            prix_perp = perp.get("prix") or 0
            if prix_spot <= 0 or prix_perp <= 0:
                continue
            # Un prix spot périmé fausserait la base : on l'ignore
            if time.time() - spot.get("timestamp", 0) > 30:
                continue

            base_pct = (prix_perp - prix_spot) / prix_spot * 100

            funding = perp.get("funding")
            periodes = perp.get("periodes_par_jour") or 3
            funding_apr = funding * periodes * 365 if funding is not None else None

            # Signal BASE : le perp est nettement au-dessus du spot.
            # Gain = convergence de la base, moins les frais du cycle.
            if base_pct >= PERP_SEUIL_BASE_PCT:
                gain_net = base_pct - frais
                if gain_net > 0:
                    opportunites.append({
                        "type_signal": "base", "exchange": exchange, "symbole": symbole,
                        "prix_spot": prix_spot, "prix_perp": prix_perp,
                        "base_pct": base_pct, "funding_pct": funding,
                        "funding_apr_pct": funding_apr, "periodes_par_jour": periodes,
                        "frais_aller_retour_pct": frais, "gain_net_estime_pct": gain_net,
                    })

            # Signal FUNDING : on est payé pour tenir la position.
            # Comparé en taux ANNUALISÉ, sinon 0.05% par période ne parle pas.
            if funding_apr is not None and funding_apr >= PERP_SEUIL_FUNDING_APR_PCT:
                # Jours nécessaires pour que le funding couvre les frais —
                # c'est LE chiffre décisif : en dessous, on perd.
                gain_par_jour = funding * periodes
                jours_rentabilite = frais / gain_par_jour if gain_par_jour > 0 else 999
                opportunites.append({
                    "type_signal": "funding", "exchange": exchange, "symbole": symbole,
                    "prix_spot": prix_spot, "prix_perp": prix_perp,
                    "base_pct": base_pct, "funding_pct": funding,
                    "funding_apr_pct": funding_apr, "periodes_par_jour": periodes,
                    "frais_aller_retour_pct": frais,
                    "gain_net_estime_pct": funding_apr,
                    "jours_avant_rentabilite": jours_rentabilite,
                })

    opportunites.sort(key=lambda o: o["gain_net_estime_pct"], reverse=True)
    return opportunites


def _enregistrer(opp: dict):
    _init_csv()
    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=COLONNES, extrasaction="ignore").writerow({
            "timestamp": round(time.time(), 3),
            **{k: (round(v, 6) if isinstance(v, float) else v) for k, v in opp.items()},
        })


def _formater_alerte(opp: dict) -> str:
    if opp["type_signal"] == "base":
        return (
            f"⚖️ <b>Arbitrage spot-perp</b> — {opp['symbole']} sur {opp['exchange']}\n"
            f"<code>Acheter au comptant @ {opp['prix_spot']:.8g}\n"
            f"Vendre à découvert le perp @ {opp['prix_perp']:.8g}</code>\n"
            f"Base : {opp['base_pct']:+.3f}%\n"
            f"Frais aller-retour (4 ordres) : {opp['frais_aller_retour_pct']:.3f}%\n"
            f"🟢 <b>Gain net estimé : {opp['gain_net_estime_pct']:+.3f}%</b>\n"
            + (f"Funding : {opp['funding_pct']:+.4f}%/période "
               f"({opp['funding_apr_pct']:+.1f}% annualisé)\n"
               if opp.get("funding_apr_pct") is not None else "")
            + "\n<i>Aucun transfert entre plateformes — position neutre en direction. "
              "Attention au risque de liquidation sur la jambe short.</i>"
        )

    jours = opp.get("jours_avant_rentabilite", 0)
    return (
        f"💰 <b>Funding élevé</b> — {opp['symbole']} sur {opp['exchange']}\n"
        f"<code>Long au comptant + short perp (neutre)</code>\n"
        f"Funding : {opp['funding_pct']:+.4f}% toutes les "
        f"{24 // opp['periodes_par_jour']}h ({opp['periodes_par_jour']}x/jour)\n"
        f"🟢 <b>Annualisé : {opp['funding_apr_pct']:+.1f}%</b>\n"
        f"Base actuelle : {opp['base_pct']:+.3f}%\n"
        f"Frais aller-retour : {opp['frais_aller_retour_pct']:.3f}% "
        f"→ rentable après <b>{jours:.1f} jour(s)</b> de détention\n"
        f"\n<i>Le taux peut s'inverser à tout moment. Risque de liquidation "
        f"sur la jambe short si le prix monte fortement.</i>"
    )


async def _alerter(opp: dict):
    cle = (opp["exchange"], opp["symbole"], opp["type_signal"])
    maintenant = time.time()
    if maintenant - _dernieres_alertes.get(cle, 0) < COOLDOWN_ALERTE_SEC:
        return
    _dernieres_alertes[cle] = maintenant

    log.info(
        f"⚖️ Perp {opp['type_signal']} : {opp['symbole']} sur {opp['exchange']} "
        f"| base={opp['base_pct']:+.3f}% | gain net={opp['gain_net_estime_pct']:+.3f}%"
    )

    if not PERP_NOTIFIER:
        return
    try:
        import telegram_notifier
        await telegram_notifier.envoyer_message_simple(_formater_alerte(opp))
    except Exception as e:
        log.error(f"Échec notification perp : {e}")


async def boucle(prix_live: dict):
    """
    Boucle principale : rafraîchit les perpétuels puis croise avec le spot.
    À lancer une fois au démarrage :
        asyncio.create_task(arbitrage_perpetuel.boucle(prix_live))
    """
    log.info(
        f"⚖️ Arbitrage perpétuel démarré (base ≥ {PERP_SEUIL_BASE_PCT}%, "
        f"funding ≥ {PERP_SEUIL_FUNDING_APR_PCT}% annualisé, "
        f"sondage {PERP_INTERVALLE_SONDAGE_SEC}s)"
    )
    while True:
        try:
            await rafraichir()
            for opp in detecter(prix_live):
                _enregistrer(opp)
                await _alerter(opp)
        except Exception as e:
            log.error(f"Erreur boucle perpétuel : {e}")
        await asyncio.sleep(PERP_INTERVALLE_SONDAGE_SEC)


def statistiques() -> dict:
    return {
        "plateformes": len(_perp_live),
        "contrats_suivis": sum(len(d) for d in _perp_live.values()),
        "derniere_maj": _derniere_maj,
        "age_sec": round(time.time() - _derniere_maj, 1) if _derniere_maj else None,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    async def _test():
        await rafraichir()
        print(f"\n{statistiques()}\n")
        # Sans cache spot du bot, on liste juste les meilleurs funding
        lignes = []
        for exchange, contrats in _perp_live.items():
            for symbole, perp in contrats.items():
                if perp.get("funding") is None:
                    continue
                apr = perp["funding"] * perp["periodes_par_jour"] * 365
                lignes.append((apr, exchange, symbole, perp["funding"]))
        lignes.sort(reverse=True)
        print(f"{'ANNUALISÉ':>10} {'PLATEFORME':<10} {'SYMBOLE':<14} {'PAR PÉRIODE':>12}")
        print("-" * 50)
        for apr, ex, sym, f in lignes[:25]:
            print(f"{apr:>9.1f}% {ex:<10} {sym:<14} {f:>11.4f}%")

    asyncio.run(_test())
