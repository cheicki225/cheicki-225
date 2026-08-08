"""
Positions en attente (mode papier uniquement)
================================================
IDÉE TESTÉE ICI
Quand un trade d'arbitrage se retrouve négatif au moment de la vente
(l'écart s'est refermé pendant le transfert), au lieu d'encaisser la perte
immédiatement, on garde la position et on surveille : dès que le prix
repasse au-dessus du point mort, on vend automatiquement.

POURQUOI DES GARDE-FOUS SONT OBLIGATOIRES
Sans limite, cette stratégie a un défaut structurel : elle plafonne les
gains (on vend dès que c'est positif) tout en laissant les pertes courir
(on ne vend jamais tant que c'est négatif). Elle produit surtout des
statistiques trompeuses — une position jamais clôturée n'apparaît JAMAIS
comme une perte, ce qui affiche un taux de réussite proche de 100% pendant
que du capital est immobilisé dans des positions perdantes invisibles.

Trois garde-fous, tous réglables dans config.py :
  1. DUREE_MAX_ATTENTE_SEC   — au-delà, vente au prix du marché (timeout)
  2. STOP_LOSS_POSITION_PCT  — en dessous, vente immédiate sans attendre
  3. MAX_POSITIONS_EN_ATTENTE— plafond, pour que le bot continue de trader

TOUT EST ENREGISTRÉ dans positions_attente.csv : durée d'attente, issue
(gagnante / stop-loss / timeout), capital immobilisé. C'est ce fichier qui
permettra de trancher objectivement si l'idée fonctionne, plutôt que de
se fier à une impression.

⚠️ Simulation uniquement — aucun ordre réel n'est jamais passé.
"""

import asyncio
import csv
import json
import logging
import os
import time

import stockage
import frais_retrait
from config import (
    POSITIONS_ATTENTE_ACTIF, DUREE_MAX_ATTENTE_SEC, STOP_LOSS_POSITION_PCT,
    MAX_POSITIONS_EN_ATTENTE, INTERVALLE_VERIF_ATTENTE_SEC, FRAIS_TRADING_PCT,
)

log = logging.getLogger("positions_attente")

CSV_PATH = stockage.chemin_donnees("positions_attente.csv")

import gestion_fichiers
gestion_fichiers.enregistrer_fichier(CSV_PATH)
COLONNES = [
    "timestamp_ouverture", "timestamp_cloture", "duree_attente_sec",
    "symbole", "exchange_achat", "exchange_vente", "montant_usdt",
    "prix_achat", "prix_vente_cloture",
    "profit_initial_usdt", "profit_final_usdt", "gain_vs_vente_immediate_usdt",
    "issue",  # "gagnante" | "stop_loss" | "timeout"
]

# Positions actuellement ouvertes, SAUVEGARDÉES sur le volume persistant.
#
# ⚠️ Pourquoi la persistance est indispensable ICI (contrairement à
# _stocks_tokens qui vit en mémoire) : une position en attente représente un
# token réellement détenu, en cours de perte. Si un redéploiement l'effaçait,
# on ne garderait que les positions déjà clôturées — donc majoritairement les
# GAGNANTES (elles se ferment vite, dès que le prix repasse positif), tandis
# que les perdantes, qui restent ouvertes plus longtemps, disparaîtraient
# silencieusement à chaque redémarrage. Le taux de réussite afficherait alors
# un chiffre flatteur et faux : exactement le biais que ce module cherche à
# éviter. Avec la sauvegarde, chaque position finit forcément comptée.
_positions: list[dict] = []

ETAT_PATH = stockage.chemin_donnees("positions_attente_etat.json")


def _sauvegarder_positions():
    """Écrit les positions ouvertes sur disque. Silencieux en cas d'échec."""
    try:
        with open(ETAT_PATH, "w", encoding="utf-8") as f:
            json.dump(_positions, f)
    except Exception as e:
        log.error(f"Échec sauvegarde des positions en attente : {e}")


def charger_positions():
    """
    Recharge les positions ouvertes après un redémarrage. Appelé une fois au
    démarrage par bot_fusionne_v1, juste avant la boucle de surveillance.

    Les positions dont la durée maximale est déjà dépassée pendant l'arrêt du
    service ne sont pas jetées : elles sont conservées et seront clôturées en
    « timeout » au premier passage de la surveillance, donc bien enregistrées.
    """
    global _positions
    if not os.path.exists(ETAT_PATH):
        return
    try:
        with open(ETAT_PATH, encoding="utf-8") as f:
            chargees = json.load(f)
        if isinstance(chargees, list):
            _positions = chargees
            if _positions:
                log.info(
                    f"♻️ {len(_positions)} position(s) en attente rechargée(s) "
                    f"après redémarrage — elles restent comptabilisées"
                )
    except Exception as e:
        log.error(f"Échec chargement des positions en attente : {e}")
        _positions = []


# Référence vers le cache de prix WebSocket, injectée au démarrage par
# bot_fusionne_v1 — évite un import circulaire.
_prix_live_ref: dict | None = None

# Un prix plus vieux que ça n'est pas une lecture valide « maintenant »
AGE_MAX_PRIX_SEC = 5.0


def definir_source_prix(prix_live: dict):
    """Appelé une fois au démarrage par bot_fusionne_v1."""
    global _prix_live_ref
    _prix_live_ref = prix_live


def _init_csv():
    if not os.path.exists(CSV_PATH):
        with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(COLONNES)


def _ecrire_ligne(ligne: dict):
    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=COLONNES).writerow(ligne)


def nb_positions_ouvertes() -> int:
    return len(_positions)


def places_disponibles() -> int:
    return max(0, MAX_POSITIONS_EN_ATTENTE - len(_positions))


def positions_ouvertes() -> list[dict]:
    """Copie lisible des positions en cours — pour le dashboard et Telegram."""
    maintenant = time.time()
    resultat = []
    for p in _positions:
        resultat.append({
            "symbole": p["symbole"],
            "exchange_achat": p["exchange_achat"],
            "exchange_vente": p["exchange_vente"],
            "montant_usdt": round(p["montant_usdt"], 2),
            "attente_sec": round(maintenant - p["timestamp_ouverture"]),
            "profit_initial_usdt": round(p["profit_initial_usdt"], 3),
            "profit_actuel_usdt": round(p.get("profit_actuel_usdt", p["profit_initial_usdt"]), 3),
        })
    return resultat


def capital_immobilise_usdt() -> float:
    return round(sum(p["montant_usdt"] for p in _positions), 2)


def peut_ouvrir(symbole: str) -> bool:
    """
    True si une nouvelle position d'attente peut être ouverte.

    Une seule position par symbole à la fois : sinon un token très volatil
    remplirait à lui seul tous les emplacements disponibles.
    """
    if not POSITIONS_ATTENTE_ACTIF:
        return False
    if any(p["symbole"] == symbole for p in _positions):
        return False
    return len(_positions) < MAX_POSITIONS_EN_ATTENTE


def ouvrir(symbole, exchange_achat, exchange_vente, montant_usdt,
           prix_achat, profit_initial_usdt, frais_usdt) -> bool:
    """
    Met une position en attente au lieu de vendre à perte immédiatement.
    Retourne True si la position a bien été ouverte.
    """
    if not peut_ouvrir(symbole):
        return False

    _init_csv()
    _positions.append({
        "symbole": symbole,
        "exchange_achat": exchange_achat,
        "exchange_vente": exchange_vente,
        "montant_usdt": montant_usdt,
        "prix_achat": prix_achat,
        "profit_initial_usdt": profit_initial_usdt,
        "profit_actuel_usdt": profit_initial_usdt,
        "frais_usdt": frais_usdt,
        "timestamp_ouverture": time.time(),
    })
    _sauvegarder_positions()
    log.info(
        f"⏳ Position EN ATTENTE : {symbole} {exchange_achat}->{exchange_vente} | "
        f"{montant_usdt:.2f}$ | perte évitée pour l'instant : {profit_initial_usdt:+.3f}$ | "
        f"{len(_positions)}/{MAX_POSITIONS_EN_ATTENTE} emplacements"
    )
    return True


def _profit_actuel(position: dict) -> float | None:
    """
    Recalcule le profit de la position aux prix courants, frais compris.
    Retourne None si aucun prix frais n'est disponible (flux muet).
    """
    if _prix_live_ref is None:
        return None

    ex_vente = position["exchange_vente"]
    donnees = _prix_live_ref.get(ex_vente, {}).get(position["symbole"])
    if donnees is None or time.time() - donnees["timestamp"] > AGE_MAX_PRIX_SEC:
        return None

    prix_vente_actuel = donnees["bid"]
    prix_achat = position["prix_achat"]
    if prix_achat <= 0 or prix_vente_actuel <= 0:
        return None

    montant = position["montant_usdt"]
    quantite = montant / prix_achat
    produit_vente = quantite * prix_vente_actuel

    # Les frais d'achat sont déjà payés ; on recompte les frais de vente et
    # de retrait, qui n'ont pas encore eu lieu.
    frais_vente = produit_vente * (FRAIS_TRADING_PCT.get(ex_vente, 0.10) / 100)
    info = frais_retrait.frais_transfert(ex_vente, position["exchange_achat"], montant)
    frais_achat_deja_paye = montant * (FRAIS_TRADING_PCT.get(position["exchange_achat"], 0.10) / 100)

    return produit_vente - montant - frais_vente - info["frais"] - frais_achat_deja_paye


def _cloturer(position: dict, profit_final: float, issue: str):
    """Ferme une position, l'enregistre au CSV et met à jour les stats papier."""
    import paper_trading  # import tardif : évite l'import circulaire

    maintenant = time.time()
    duree = maintenant - position["timestamp_ouverture"]
    gain_vs_immediat = profit_final - position["profit_initial_usdt"]

    prix_vente_cloture = ""
    if _prix_live_ref is not None:
        donnees = _prix_live_ref.get(position["exchange_vente"], {}).get(position["symbole"])
        if donnees:
            prix_vente_cloture = donnees["bid"]

    _ecrire_ligne({
        "timestamp_ouverture": position["timestamp_ouverture"],
        "timestamp_cloture": maintenant,
        "duree_attente_sec": round(duree, 1),
        "symbole": position["symbole"],
        "exchange_achat": position["exchange_achat"],
        "exchange_vente": position["exchange_vente"],
        "montant_usdt": round(position["montant_usdt"], 4),
        "prix_achat": position["prix_achat"],
        "prix_vente_cloture": prix_vente_cloture,
        "profit_initial_usdt": round(position["profit_initial_usdt"], 4),
        "profit_final_usdt": round(profit_final, 4),
        "gain_vs_vente_immediate_usdt": round(gain_vs_immediat, 4),
        "issue": issue,
    })

    if position in _positions:
        _positions.remove(position)
        _sauvegarder_positions()

    # Le résultat FINAL est celui qui compte dans les statistiques papier —
    # une position en attente ne doit jamais rester invisible dans les stats.
    paper_trading.enregistrer_resultat_position_attente(
        symbole=position["symbole"],
        exchange_achat=position["exchange_achat"],
        exchange_vente=position["exchange_vente"],
        montant_usdt=position["montant_usdt"],
        prix_achat=position["prix_achat"],
        prix_vente=prix_vente_cloture,
        profit_usdt=profit_final,
        frais_usdt=position["frais_usdt"],
    )

    emoji = {"gagnante": "✅", "stop_loss": "🛑", "timeout": "⌛"}.get(issue, "•")
    log.info(
        f"{emoji} Position CLÔTURÉE ({issue}) : {position['symbole']} | "
        f"attente {duree:.0f}s | profit {profit_final:+.3f}$ "
        f"(vente immédiate aurait donné {position['profit_initial_usdt']:+.3f}$, "
        f"écart {gain_vs_immediat:+.3f}$)"
    )
    return {"issue": issue, "duree_sec": duree, "profit_final": profit_final,
            "gain_vs_immediat": gain_vs_immediat}


def verifier_positions() -> list[dict]:
    """
    Passe en revue toutes les positions ouvertes et clôture celles qui
    remplissent une condition de sortie. Sans await : appelable partout.
    Retourne la liste des clôtures effectuées.
    """
    if not _positions:
        return []

    maintenant = time.time()
    clotures = []

    for position in list(_positions):
        profit = _profit_actuel(position)
        duree = maintenant - position["timestamp_ouverture"]

        if profit is None:
            # Pas de prix frais. On ne clôture PAS sur une donnée absente,
            # sauf si la durée maximale est dépassée — sinon une position
            # pourrait rester ouverte indéfiniment sur un flux mort.
            if duree >= DUREE_MAX_ATTENTE_SEC:
                clotures.append(_cloturer(position, position["profit_initial_usdt"], "timeout"))
            continue

        position["profit_actuel_usdt"] = profit
        perte_pct = (profit / position["montant_usdt"]) * 100 if position["montant_usdt"] else 0

        if profit > 0:
            clotures.append(_cloturer(position, profit, "gagnante"))
        elif perte_pct <= STOP_LOSS_POSITION_PCT:
            clotures.append(_cloturer(position, profit, "stop_loss"))
        elif duree >= DUREE_MAX_ATTENTE_SEC:
            clotures.append(_cloturer(position, profit, "timeout"))

    return clotures


async def boucle_surveillance(intervalle_sec: float = None):
    """Tâche de fond lancée par bot_fusionne_v1 au démarrage."""
    intervalle = intervalle_sec or INTERVALLE_VERIF_ATTENTE_SEC
    if not POSITIONS_ATTENTE_ACTIF:
        log.info("Positions en attente DÉSACTIVÉES (POSITIONS_ATTENTE_ACTIF=False)")
        return

    log.info(
        f"⏳ Surveillance des positions en attente activée : max {MAX_POSITIONS_EN_ATTENTE} "
        f"positions, durée max {DUREE_MAX_ATTENTE_SEC}s, stop-loss {STOP_LOSS_POSITION_PCT}%"
    )
    while True:
        try:
            verifier_positions()
        except Exception as e:
            log.error(f"Erreur pendant la surveillance des positions : {e}")
        await asyncio.sleep(intervalle)


def statistiques() -> dict:
    """
    Bilan lu depuis le CSV — c'est LUI qui dit si l'idée fonctionne.
    Le chiffre décisif est `gain_total_vs_vente_immediate` : positif =
    attendre a rapporté plus que vendre tout de suite, négatif = l'inverse.
    """
    if not os.path.exists(CSV_PATH):
        return {"total": 0}

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        lignes = list(csv.DictReader(f))

    if not lignes:
        return {"total": 0}

    def nombre(ligne, cle):
        try:
            return float(ligne.get(cle) or 0)
        except (TypeError, ValueError):
            return 0.0

    issues = [l.get("issue") for l in lignes]
    durees = [nombre(l, "duree_attente_sec") for l in lignes]
    gains = [nombre(l, "gain_vs_vente_immediate_usdt") for l in lignes]
    gagnantes = [d for d, i in zip(durees, issues) if i == "gagnante"]

    return {
        "total": len(lignes),
        "gagnantes": issues.count("gagnante"),
        "stop_loss": issues.count("stop_loss"),
        "timeout": issues.count("timeout"),
        "taux_reussite_pct": round(issues.count("gagnante") / len(lignes) * 100, 1),
        "duree_moyenne_sec": round(sum(durees) / len(durees), 1),
        "duree_moyenne_gagnantes_sec": round(sum(gagnantes) / len(gagnantes), 1) if gagnantes else None,
        "gain_total_vs_vente_immediate": round(sum(gains), 3),
        "ouvertes_maintenant": len(_positions),
        "capital_immobilise_usdt": capital_immobilise_usdt(),
    }


def resume_telegram() -> str:
    s = statistiques()
    if not s.get("total"):
        ouvertes = len(_positions)
        return (
            f"⏳ <b>Positions en attente</b>\n\n"
            f"Ouvertes : {ouvertes}/{MAX_POSITIONS_EN_ATTENTE}\n"
            f"Capital immobilisé : {capital_immobilise_usdt():.2f}$\n"
            f"Aucune position clôturée pour l'instant."
        )

    gain = s["gain_total_vs_vente_immediate"]
    verdict = (
        "✅ Attendre a rapporté plus que vendre immédiatement"
        if gain > 0 else
        "❌ Attendre a coûté plus cher que vendre immédiatement"
    )
    duree_g = s["duree_moyenne_gagnantes_sec"]

    return (
        f"⏳ <b>Positions en attente</b>\n\n"
        f"Ouvertes : {s['ouvertes_maintenant']}/{MAX_POSITIONS_EN_ATTENTE} "
        f"({s['capital_immobilise_usdt']:.2f}$ immobilisés)\n\n"
        f"Clôturées : {s['total']}\n"
        f"✅ Revenues positives : {s['gagnantes']} ({s['taux_reussite_pct']:.0f}%)\n"
        f"🛑 Stop-loss : {s['stop_loss']}\n"
        f"⌛ Timeout : {s['timeout']}\n\n"
        f"Attente moyenne : {s['duree_moyenne_sec']:.0f}s"
        + (f" (gagnantes : {duree_g:.0f}s)\n" if duree_g else "\n")
        + f"\n<b>Écart vs vente immédiate : {gain:+.3f}$</b>\n{verdict}"
    )
