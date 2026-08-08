"""
Suivi de la persistance d'une opportunité dans le temps
=========================================================
Pour chaque opportunité alertée, lit le prix courant chaque seconde pendant
10 secondes — directement dans le cache WebSocket partagé `prix_live`
(aucun appel réseau supplémentaire) — et recalcule le spread NET réel
(frais de trading + frais de retrait réels/estimés, même logique que
paper_trading.simuler_trade) à chaque instant.

Objectif : savoir si le spread affiché au moment de la détection est
encore en gain 1, 2... 10 secondes plus tard, ou s'il s'est déjà refermé —
avec des prix réellement observés, pas avec le score ML (qui est une
probabilité apprise, pas une mesure directe).

Résultat :
    - CSV (une ligne par seconde par opportunité suivie) pour analyse a posteriori
    - Log en direct seconde par seconde
    - Résumé Telegram à la fin des 10 secondes (évolution + point de bascule)

⚠️ Comme le reste du projet : c'est un suivi PASSIF, aucun trade réel n'est
jamais exécuté ici.
"""

import asyncio
import csv
import logging
import os
import time

import stockage
import frais_retrait
from config import (
    FRAIS_TRADING_PCT, SUIVI_DUREE_SEC, SUIVI_INTERVALLE_SEC,
    SUIVI_AGE_MAX_PRIX_SEC, SUIVI_ENVOYER_SEULEMENT_SI_POSITIF,
)

log = logging.getLogger("suivi_opportunite")

CSV_PATH = stockage.chemin_donnees("suivi_opportunites.csv")
COLONNES = [
    "timestamp", "opportunite_id", "symbole", "exchange_achat", "exchange_vente",
    "seconde", "prix_achat", "prix_vente", "spread_brut_pct", "spread_net_pct",
    "profit_usdt", "donnee_manquante",
]

# ⚠️ Ajouté le 07/08 : CSV SÉPARÉ pour le triangulaire, plutôt que de forcer
# ses colonnes dans le schéma inter-exchange (exchange_achat/exchange_vente
# n'ont pas de sens pour un triangle — une seule plateforme, 3 paires).
# Mélanger les deux schémas dans un même fichier aurait rendu l'analyse a
# posteriori confuse (colonnes vides selon le type, sens différent).
CSV_PATH_TRIANGLE = stockage.chemin_donnees("suivi_triangles.csv")
COLONNES_TRIANGLE = [
    "timestamp", "opportunite_id", "exchange", "paire_1", "paire_2", "paire_3",
    "seconde", "spread_brut_pct", "spread_net_pct", "profit_usdt", "donnee_manquante",
]

# Réglages lus depuis config.py (tout est centralisé là-bas, comme le reste
# du projet) — les noms courts restent utilisables dans ce module.
DUREE_SUIVI_SEC = SUIVI_DUREE_SEC
INTERVALLE_SEC = SUIVI_INTERVALLE_SEC
AGE_MAX_PRIX_SEC = SUIVI_AGE_MAX_PRIX_SEC

# Montant de référence pour le calcul du spread net — identique au montant
# standard utilisé par paper_trading (MONTANT_PAR_TRADE_USDT = 50.0), pour
# que les deux mesures restent directement comparables. Pas d'import croisé
# vers paper_trading pour garder ce module indépendant (aucun risque de
# import circulaire si paper_trading importe ce module plus tard).
MONTANT_SUIVI_USDT = 50.0


def _init_csv():
    """
    Comme opportunity_logger._init_csv() : si un fichier existant a un
    ancien schéma de colonnes, il est archivé plutôt que réutilisé tel
    quel — sinon les nouvelles lignes se retrouveraient décalées sous les
    en-têtes de l'ancien fichier.
    """
    if os.path.exists(CSV_PATH):
        with open(CSV_PATH, encoding="utf-8") as f:
            entete_existant = f.readline().strip().split(",")
        if entete_existant != COLONNES:
            horodatage = time.strftime("%Y%m%d_%H%M%S")
            chemin_archive = CSV_PATH.replace(".csv", f"_ancien_schema_{horodatage}.csv")
            os.rename(CSV_PATH, chemin_archive)
            log.warning(f"suivi_opportunites.csv : schéma changé, ancien fichier archivé sous {chemin_archive}")

    if not os.path.exists(CSV_PATH):
        with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(COLONNES)


def _init_csv_triangle():
    if os.path.exists(CSV_PATH_TRIANGLE):
        with open(CSV_PATH_TRIANGLE, encoding="utf-8") as f:
            entete_existant = f.readline().strip().split(",")
        if entete_existant != COLONNES_TRIANGLE:
            horodatage = time.strftime("%Y%m%d_%H%M%S")
            chemin_archive = CSV_PATH_TRIANGLE.replace(".csv", f"_ancien_schema_{horodatage}.csv")
            os.rename(CSV_PATH_TRIANGLE, chemin_archive)
            log.warning(f"suivi_triangles.csv : schéma changé, ancien fichier archivé sous {chemin_archive}")

    if not os.path.exists(CSV_PATH_TRIANGLE):
        with open(CSV_PATH_TRIANGLE, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(COLONNES_TRIANGLE)


def _ecrire_ligne(ligne: dict):
    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=COLONNES).writerow(ligne)


def _ecrire_ligne_triangle(ligne: dict):
    with open(CSV_PATH_TRIANGLE, "a", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=COLONNES_TRIANGLE).writerow(ligne)


def _lire_prix_live(prix_live: dict, exchange: str, symbole: str):
    """
    Lit le dernier prix connu pour (exchange, symbole) dans le cache
    WebSocket partagé. Retourne None si absent ou trop vieux — un prix figé
    ne doit jamais être confondu avec un prix réellement observé maintenant.
    """
    data = prix_live.get(exchange, {}).get(symbole)
    if data is None:
        return None
    if time.time() - data["timestamp"] > AGE_MAX_PRIX_SEC:
        return None
    return data


def _calculer_point(ex_achat: str, ex_vente: str, prix_achat: float, prix_vente: float, montant_usdt: float):
    """
    Même logique de coût que paper_trading.simuler_trade() : frais de
    trading (achat + vente) + frais de retrait réels/estimés du réseau le
    moins cher utilisable des DEUX côtés. frais_retrait.frais_transfert()
    est purement local une fois _reseaux chargé au démarrage du bot — pas
    d'appel réseau ici, donc appelable chaque seconde sans souci.
    """
    if prix_achat <= 0:
        return 0.0, 0.0, 0.0

    spread_brut_pct = ((prix_vente - prix_achat) / prix_achat) * 100

    frais_trading_pct = FRAIS_TRADING_PCT.get(ex_achat, 0.10) + FRAIS_TRADING_PCT.get(ex_vente, 0.10)
    frais_trading_usdt = montant_usdt * (frais_trading_pct / 100)

    info_retrait = frais_retrait.frais_transfert(ex_vente, ex_achat, montant_usdt)
    frais_retrait_usdt = info_retrait["frais"]

    profit_usdt = montant_usdt * (spread_brut_pct / 100) - frais_trading_usdt - frais_retrait_usdt
    spread_net_pct = (profit_usdt / montant_usdt) * 100 if montant_usdt else 0.0

    return spread_brut_pct, spread_net_pct, profit_usdt


async def suivre_opportunite(
    opp, prix_live: dict, montant_usdt: float = MONTANT_SUIVI_USDT,
    message_id_alerte: int | None = None, notifier: bool = True,
):
    """
    Suit une opportunité pendant DUREE_SUIVI_SEC secondes, une lecture par
    seconde, directement depuis le cache prix_live déjà rempli par les
    WebSockets de bot_fusionne_v1.py.

    message_id_alerte : id du message d'alerte Telegram correspondant. Le
        résumé sera envoyé EN RÉPONSE à ce message, pour qu'on voie
        immédiatement quelle opportunité il analyse.
    notifier : False = on remplit le CSV mais on n'envoie rien sur Telegram
        (utilisé en mode nuit : la collecte de données continue, seules les
        notifications sont coupées).

    À lancer en tâche de fond juste après l'alerte Telegram initiale :
        asyncio.create_task(suivi_opportunite.suivre_opportunite(
            opp, prix_live, message_id_alerte=mid))
    """
    _init_csv()

    ex_achat, ex_vente = opp.exchanges
    symbole = opp.symboles[0]
    opportunite_id = f"{symbole}:{ex_achat}-{ex_vente}:{int(opp.timestamp)}"

    points = []  # liste de (seconde, spread_net_pct ou None si donnée manquante)

    for seconde in range(DUREE_SUIVI_SEC):
        data_achat = _lire_prix_live(prix_live, ex_achat, symbole)
        data_vente = _lire_prix_live(prix_live, ex_vente, symbole)

        if data_achat is None or data_vente is None:
            log.debug(f"⚠️ Suivi {symbole} {ex_achat}->{ex_vente} : prix manquant à t={seconde}s")
            _ecrire_ligne({
                "timestamp": time.time(), "opportunite_id": opportunite_id, "symbole": symbole,
                "exchange_achat": ex_achat, "exchange_vente": ex_vente, "seconde": seconde,
                "prix_achat": "", "prix_vente": "", "spread_brut_pct": "", "spread_net_pct": "",
                "profit_usdt": "", "donnee_manquante": True,
            })
            points.append((seconde, None))
        else:
            prix_achat = data_achat["ask"]
            prix_vente = data_vente["bid"]
            spread_brut_pct, spread_net_pct, profit_usdt = _calculer_point(
                ex_achat, ex_vente, prix_achat, prix_vente, montant_usdt
            )
            _ecrire_ligne({
                "timestamp": time.time(), "opportunite_id": opportunite_id, "symbole": symbole,
                "exchange_achat": ex_achat, "exchange_vente": ex_vente, "seconde": seconde,
                "prix_achat": prix_achat, "prix_vente": prix_vente,
                "spread_brut_pct": round(spread_brut_pct, 4), "spread_net_pct": round(spread_net_pct, 4),
                "profit_usdt": round(profit_usdt, 4), "donnee_manquante": False,
            })
            points.append((seconde, spread_net_pct))
            log.info(
                f"📈 Suivi {symbole} {ex_achat}->{ex_vente} t={seconde}s : "
                f"spread net={spread_net_pct:+.3f}% (profit sur {montant_usdt:.0f}$ = {profit_usdt:+.3f}$)"
            )

        if seconde < DUREE_SUIVI_SEC - 1:
            await asyncio.sleep(INTERVALLE_SEC)

    await _envoyer_resume_telegram(
        symbole, ex_achat, ex_vente, montant_usdt, points,
        message_id_alerte=message_id_alerte, notifier=notifier,
    )


async def _envoyer_resume_telegram(
    symbole: str, ex_achat: str, ex_vente: str, montant_usdt: float, points: list,
    message_id_alerte: int | None = None, notifier: bool = True,
):
    import telegram_notifier  # import tardif, même pattern que paper_trading.py

    if not notifier:
        log.debug(f"Suivi {symbole} : résumé Telegram non envoyé (notifications coupées) — CSV rempli")
        return

    valides = [(s, v) for s, v in points if v is not None]

    if not valides:
        # Aucun prix reçu : c'est un problème de flux, pas une opportunité.
        # Vaut la peine d'être signalé, mais jamais en réponse à rien.
        await telegram_notifier.envoyer_message_simple(
            f"📉 <b>Suivi 10s</b> — {symbole} {ex_achat}→{ex_vente}\n"
            f"Aucune donnée de prix reçue pendant le suivi (flux WebSocket muet sur cette fenêtre).",
            repondre_a=message_id_alerte,
        )
        return

    a_ete_positif = any(v > 0 for _, v in valides)

    # Filtre anti-spam : les suivis négatifs du début à la fin sont
    # largement majoritaires et disent tous exactement la même chose. Les
    # notifier sature le quota Telegram (voir le calcul de charge dans
    # config.py) sans rien apprendre de nouveau. Le CSV, lui, garde TOUT —
    # rien n'est perdu pour l'analyse.
    if SUIVI_ENVOYER_SEULEMENT_SI_POSITIF and not a_ete_positif:
        log.info(
            f"📉 Suivi {symbole} {ex_achat}→{ex_vente} : négatif sur toute la fenêtre "
            f"({valides[0][1]:+.2f}% → {valides[-1][1]:+.2f}%) — enregistré au CSV, "
            f"résumé Telegram omis (SUIVI_ENVOYER_SEULEMENT_SI_POSITIF=True)"
        )
        return

    ligne_evolution = " → ".join(
        f"{v:+.2f}%" if v is not None else "?" for _, v in points
    )

    depart_pct = valides[0][1]
    fin_pct = valides[-1][1]
    meilleur_pct = max(v for _, v in valides)

    # Première seconde où le spread net est passé (ou repassé) négatif —
    # c'est l'info la plus utile : le point de bascule gain -> perte
    bascule = next((s for s, v in valides if v is not None and v < 0), None)
    if bascule is not None:
        ligne_bascule = f"⚠️ Passé négatif à t={bascule}s"
    elif fin_pct > 0:
        ligne_bascule = f"✅ Resté positif sur toute la fenêtre de {DUREE_SUIVI_SEC}s"
    else:
        ligne_bascule = "❌ Négatif dès le départ"

    emoji = "🟢" if fin_pct > 0 else "🔴"

    await telegram_notifier.envoyer_message_simple(
        f"{emoji} <b>Suivi {DUREE_SUIVI_SEC}s</b> — {symbole} {ex_achat}→{ex_vente}\n"
        f"Spread net par seconde (sur {montant_usdt:.0f}$, frais trading + retrait inclus) :\n"
        f"<code>{ligne_evolution}</code>\n"
        f"Départ : {depart_pct:+.2f}% → Fin (t={DUREE_SUIVI_SEC}s) : {fin_pct:+.2f}%"
        f" | meilleur : {meilleur_pct:+.2f}%\n"
        f"{ligne_bascule}",
        repondre_a=message_id_alerte,
    )


# ============================================================
# SUIVI DU TRIANGULAIRE — miroir simplifié, un seul exchange
# ============================================================
# ⚠️ Ajouté le 07/08. Contrairement à suivre_opportunite() (deux
# plateformes, un transfert implicite entre elles), un triangle se joue
# entièrement sur UNE SEULE plateforme — pas de retrait, pas de transfert,
# donc pas de frais_retrait à ajouter : spread_net_pct (3x frais de trading
# déjà déduits) EST le profit réel, contrairement à l'inter-exchange où le
# "spread net" affiché ignore encore les frais de retrait tant qu'on n'a
# pas explicitement calculé le "bénéfice réel".
async def suivre_triangle(
    opp, prix_live: dict, exchange: str, triangle: tuple[str, str, str],
    message_id_alerte: int | None = None, notifier: bool = True,
):
    """
    Suit un triangle pendant DUREE_SUIVI_SEC secondes, une lecture par
    seconde, en relisant directement prix_live (même cache, aucun appel
    réseau) — recalcule le même chemin USDT -> base1 -> base2 -> USDT que
    detecter_arbitrage_triangulaire(), sans dépendre de cette fonction pour
    éviter un import circulaire avec bot_fusionne_v1.py.

    À lancer juste après l'alerte triangulaire :
        asyncio.create_task(suivi_opportunite.suivre_triangle(
            opp, prix_live, exchange, triangle, message_id_alerte=mid))
    """
    _init_csv_triangle()

    paire_1, paire_2, paire_3 = triangle
    opportunite_id = f"tri:{exchange}:{'-'.join(triangle)}:{int(opp.timestamp)}"
    frais_total_pct = FRAIS_TRADING_PCT.get(exchange, 0.10) * 3

    points = []  # (seconde, spread_net_pct ou None)

    for seconde in range(DUREE_SUIVI_SEC):
        d1 = _lire_prix_live(prix_live, exchange, paire_1)
        d2 = _lire_prix_live(prix_live, exchange, paire_2)
        d3 = _lire_prix_live(prix_live, exchange, paire_3)

        spread_brut_pct = spread_net_pct = None
        if d1 and d2 and d3:
            try:
                montant = 1.0 / d1["ask"] / d2["ask"] * d3["bid"]
                spread_brut_pct = (montant - 1.0) * 100
                spread_net_pct = spread_brut_pct - frais_total_pct
            except (ZeroDivisionError, KeyError):
                pass

        if spread_net_pct is None:
            log.debug(f"⚠️ Suivi triangle {exchange} {'-'.join(triangle)} : prix manquant à t={seconde}s")
            _ecrire_ligne_triangle({
                "timestamp": time.time(), "opportunite_id": opportunite_id, "exchange": exchange,
                "paire_1": paire_1, "paire_2": paire_2, "paire_3": paire_3, "seconde": seconde,
                "spread_brut_pct": "", "spread_net_pct": "", "profit_usdt": "", "donnee_manquante": True,
            })
        else:
            profit_usdt = MONTANT_SUIVI_USDT * (spread_net_pct / 100)
            _ecrire_ligne_triangle({
                "timestamp": time.time(), "opportunite_id": opportunite_id, "exchange": exchange,
                "paire_1": paire_1, "paire_2": paire_2, "paire_3": paire_3, "seconde": seconde,
                "spread_brut_pct": round(spread_brut_pct, 4), "spread_net_pct": round(spread_net_pct, 4),
                "profit_usdt": round(profit_usdt, 4), "donnee_manquante": False,
            })
            log.info(
                f"📈 Suivi triangle {exchange} {'-'.join(triangle)} t={seconde}s : "
                f"spread net={spread_net_pct:+.3f}% (profit sur {MONTANT_SUIVI_USDT:.0f}$ = {profit_usdt:+.3f}$)"
            )

        points.append((seconde, spread_net_pct))
        if seconde < DUREE_SUIVI_SEC - 1:
            await asyncio.sleep(INTERVALLE_SEC)

    await _envoyer_resume_telegram_triangle(
        exchange, triangle, points, message_id_alerte=message_id_alerte, notifier=notifier,
    )


async def _envoyer_resume_telegram_triangle(
    exchange: str, triangle: tuple[str, str, str], points: list,
    message_id_alerte: int | None = None, notifier: bool = True,
):
    import telegram_notifier

    chemin = "→".join(triangle)

    if not notifier:
        log.debug(f"Suivi triangle {exchange} {chemin} : résumé Telegram non envoyé — CSV rempli")
        return

    valides = [(s, v) for s, v in points if v is not None]

    if not valides:
        await telegram_notifier.envoyer_message_simple(
            f"📉 <b>Suivi triangle 10s</b> — {exchange} : {chemin}\n"
            f"Aucune donnée de prix reçue pendant le suivi (flux WebSocket muet sur cette fenêtre).",
            repondre_a=message_id_alerte,
        )
        return

    a_ete_positif = any(v > 0 for _, v in valides)
    if SUIVI_ENVOYER_SEULEMENT_SI_POSITIF and not a_ete_positif:
        log.info(
            f"📉 Suivi triangle {exchange} {chemin} : négatif sur toute la fenêtre "
            f"({valides[0][1]:+.2f}% → {valides[-1][1]:+.2f}%) — enregistré au CSV, "
            f"résumé Telegram omis (SUIVI_ENVOYER_SEULEMENT_SI_POSITIF=True)"
        )
        return

    ligne_evolution = " → ".join(f"{v:+.2f}%" if v is not None else "?" for _, v in points)
    depart_pct = valides[0][1]
    fin_pct = valides[-1][1]
    meilleur_pct = max(v for _, v in valides)

    bascule = next((s for s, v in valides if v is not None and v < 0), None)
    if bascule is not None:
        ligne_bascule = f"⚠️ Passé négatif à t={bascule}s"
    elif fin_pct > 0:
        ligne_bascule = f"✅ Resté positif sur toute la fenêtre de {DUREE_SUIVI_SEC}s"
    else:
        ligne_bascule = "❌ Négatif dès le départ"

    emoji = "🟢" if fin_pct > 0 else "🔴"

    await telegram_notifier.envoyer_message_simple(
        f"{emoji} <b>Suivi triangle {DUREE_SUIVI_SEC}s</b> — {exchange} : {chemin}\n"
        f"Spread net par seconde (3x frais de trading déjà déduits — aucun frais de "
        f"retrait, tout se joue sur une seule plateforme) :\n"
        f"<code>{ligne_evolution}</code>\n"
        f"Départ : {depart_pct:+.2f}% → Fin (t={DUREE_SUIVI_SEC}s) : {fin_pct:+.2f}%"
        f" | meilleur : {meilleur_pct:+.2f}%\n"
        f"{ligne_bascule}",
        repondre_a=message_id_alerte,
    )


# ============================================================
# TEST AUTONOME — données simulées, aucun accès réseau
# ============================================================
async def _test():
    """
    python3 suivi_opportunite.py

    Simule un spread de 9% qui se referme progressivement sur 10 secondes
    (comme le prédit une confiance ML basse) — vérifie que le point de
    bascule est correctement détecté, sans toucher à Telegram ni au réseau.
    """
    import dataclasses

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    @dataclasses.dataclass
    class FausseOpp:
        exchanges: list
        symboles: list
        timestamp: float = time.time()

    opp = FausseOpp(exchanges=["gateio", "binance"], symboles=["COTIUSDT"])

    # Prix simulés : l'écart se resserre chaque seconde (le marché se
    # rééquilibre), jusqu'à passer négatif vers la fin — cas typique d'un
    # spread qui ne survit pas au délai de transfert réel
    prix_live_simule = {"gateio": {"COTIUSDT": {"bid": 0, "ask": 0.012312, "timestamp": time.time()}},
                         "binance": {"COTIUSDT": {"bid": 0.01339, "ask": 0, "timestamp": time.time()}}}

    async def _faire_evoluer_prix():
        prix_achat_depart = 0.012312
        prix_vente_depart = 0.01339
        for t in range(DUREE_SUIVI_SEC):
            # Le prix d'achat monte et le prix de vente descend -> l'écart
            # se referme progressivement, comme un vrai rééquilibrage de marché
            facteur = t / (DUREE_SUIVI_SEC - 1)
            prix_live_simule["gateio"]["COTIUSDT"] = {
                "bid": 0, "ask": prix_achat_depart * (1 + 0.06 * facteur), "timestamp": time.time(),
            }
            prix_live_simule["binance"]["COTIUSDT"] = {
                "bid": prix_vente_depart * (1 - 0.03 * facteur), "ask": 0, "timestamp": time.time(),
            }
            await asyncio.sleep(INTERVALLE_SEC)

    # Test 1 : vérifie le calcul d'un point isolé (sanity check du calcul de frais)
    print("\n--- Test 1 : calcul d'un point isolé ---")
    spread_brut, spread_net, profit = _calculer_point("gateio", "binance", 0.012312, 0.01339, 50.0)
    print(f"Spread brut={spread_brut:.3f}% | Spread net={spread_net:.3f}% | Profit={profit:+.3f}$")
    assert spread_brut > 0, "le spread brut devrait être positif avec ces prix"

    # Test 2 : donnée manquante -> ne doit pas planter, doit juste logguer "?"
    print("\n--- Test 2 : exchange absent du cache (flux coupé) ---")
    resultat = _lire_prix_live({}, "gateio", "COTIUSDT")
    assert resultat is None, "un exchange absent doit retourner None, pas planter"
    print("OK : retourne None proprement")

    # Test 3 : prix trop vieux (> AGE_MAX_PRIX_SEC) -> doit être ignoré
    print("\n--- Test 3 : prix périmé ---")
    vieux_cache = {"gateio": {"COTIUSDT": {"bid": 0, "ask": 1.0, "timestamp": time.time() - 10}}}
    resultat = _lire_prix_live(vieux_cache, "gateio", "COTIUSDT")
    assert resultat is None, "un prix vieux de 10s doit être rejeté (max autorisé: 5s)"
    print("OK : prix périmé correctement rejeté")

    # Test 4 : suivi complet sur 10s avec des prix qui évoluent en tâche de fond
    # (n'envoie PAS sur Telegram — on appelle directement la boucle sans le résumé)
    print(f"\n--- Test 4 : suivi complet sur {DUREE_SUIVI_SEC}s (prix simulés en tâche de fond) ---")
    tache_prix = asyncio.create_task(_faire_evoluer_prix())

    points = []
    for seconde in range(DUREE_SUIVI_SEC):
        data_achat = _lire_prix_live(prix_live_simule, "gateio", "COTIUSDT")
        data_vente = _lire_prix_live(prix_live_simule, "binance", "COTIUSDT")
        if data_achat and data_vente:
            _, spread_net, _ = _calculer_point(
                "gateio", "binance", data_achat["ask"], data_vente["bid"], 50.0
            )
            points.append((seconde, spread_net))
            print(f"  t={seconde}s : spread net = {spread_net:+.3f}%")
        await asyncio.sleep(INTERVALLE_SEC)

    await tache_prix

    valides = [v for _, v in points if v is not None]
    assert len(valides) == DUREE_SUIVI_SEC, "toutes les secondes devraient avoir une donnée ici"
    assert valides[0] > valides[-1], "le spread doit décroître avec ces prix simulés"
    print(f"\nOK : spread passé de {valides[0]:+.3f}% à {valides[-1]:+.3f}% sur {DUREE_SUIVI_SEC}s")

    # Test 5 : la vraie fonction publique suivre_opportunite() de bout en
    # bout — CSV compris. On coupe juste l'envoi Telegram réel (réseau non
    # disponible dans ce bac à sable) en remplaçant temporairement la
    # fonction d'envoi par une version qui capture le message au lieu de
    # l'envoyer, pour vérifier que le résumé est bien généré sans erreur.
    print(f"\n--- Test 5 : suivre_opportunite() de bout en bout (CSV + résumé) ---")
    import telegram_notifier
    messages_captes = []

    async def _fausse_envoi(texte):
        messages_captes.append(texte)
        return True

    _original_envoi = telegram_notifier.envoyer_message_simple
    telegram_notifier.envoyer_message_simple = _fausse_envoi
    try:
        if os.path.exists(CSV_PATH):
            os.remove(CSV_PATH)  # départ propre pour compter les lignes précisément

        opp2 = FausseOpp(exchanges=["gateio", "binance"], symboles=["COTIUSDT"])
        await suivre_opportunite(opp2, prix_live_simule, montant_usdt=50.0)
    finally:
        telegram_notifier.envoyer_message_simple = _original_envoi

    assert os.path.exists(CSV_PATH), "le CSV devrait exister après suivre_opportunite()"
    with open(CSV_PATH, encoding="utf-8") as f:
        lignes = list(csv.reader(f))
    assert len(lignes) == DUREE_SUIVI_SEC + 1, f"attendu {DUREE_SUIVI_SEC + 1} lignes (en-tête + 10), trouvé {len(lignes)}"
    print(f"OK : {len(lignes) - 1} lignes écrites dans {CSV_PATH}")

    assert len(messages_captes) == 1, "un seul résumé Telegram doit être envoyé à la fin du suivi"
    print(f"OK : résumé Telegram généré (capté, pas envoyé) :\n{messages_captes[0]}")

    print(f"\n✅ Tous les tests passent. CSV écrit dans : {CSV_PATH}")


if __name__ == "__main__":
    asyncio.run(_test())
