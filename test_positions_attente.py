"""
Tests des positions en attente — python3 test_positions_attente.py

Le point CRITIQUE vérifié ici : aucune position ne peut rester ouverte
indéfiniment, et chaque clôture est bien enregistrée dans les statistiques
papier (une position perdante ne doit jamais devenir invisible).

Aucun accès réseau : prix et frais simulés.
"""

import csv
import logging
import os
import sys
import time

logging.basicConfig(level=logging.INFO, format="%(message)s")

echecs = []


def verifier(condition, message):
    print(("  ✅ " if condition else "  ❌ ") + message)
    if not condition:
        echecs.append(message)


import frais_retrait as fr
import positions_attente as pa
import paper_trading as pt
import config

# Frais de retrait simulés : 0.10$ via SOL entre les deux plateformes
fr._reseaux.clear()
fr._cache_frais.clear()
for exchange in ("kucoin", "gateio"):
    fr._reseaux[exchange] = {
        "SOL": {"frais": 0.10, "retrait_ouvert": True, "depot_ouvert": True, "min_retrait": 1}
    }

for chemin in (pa.CSV_PATH, pt.CSV_PATH):
    if os.path.exists(chemin):
        os.remove(chemin)

# Cache de prix simulé
prix = {"kucoin": {}, "gateio": {}}
pa.definir_source_prix(prix)


def poser_prix(symbole, prix_vente):
    prix["kucoin"][symbole] = {"bid": prix_vente, "ask": prix_vente * 1.001, "timestamp": time.time()}


def ouvrir_position(symbole, prix_achat=1.0, montant=50.0, profit_initial=-0.5):
    return pa.ouvrir(
        symbole=symbole, exchange_achat="gateio", exchange_vente="kucoin",
        montant_usdt=montant, prix_achat=prix_achat,
        profit_initial_usdt=profit_initial, frais_usdt=0.25,
    )


# ============================================================
print("\n[1] Les garde-fous sont bien configurés")
# ============================================================
verifier(config.DUREE_MAX_ATTENTE_SEC > 0, f"durée max = {config.DUREE_MAX_ATTENTE_SEC}s")
verifier(config.STOP_LOSS_POSITION_PCT < 0, f"stop-loss = {config.STOP_LOSS_POSITION_PCT}%")
verifier(
    0 < config.MAX_POSITIONS_EN_ATTENTE < config.MAX_TOKENS_EN_STOCK,
    f"plafond = {config.MAX_POSITIONS_EN_ATTENTE} positions "
    f"(< {config.MAX_TOKENS_EN_STOCK} emplacements de stock, le bot peut continuer à trader)",
)


# ============================================================
print("\n[2] Sortie GAGNANTE — le prix repasse positif")
# ============================================================
pa._positions.clear()
ouvrir_position("AAAUSDT", prix_achat=1.0, montant=50.0, profit_initial=-0.5)
poser_prix("AAAUSDT", 1.05)  # +5% -> largement positif après frais
clotures = pa.verifier_positions()
verifier(len(clotures) == 1 and clotures[0]["issue"] == "gagnante", "clôturée comme gagnante")
verifier(pa.nb_positions_ouvertes() == 0, "emplacement libéré")
if clotures:
    verifier(
        clotures[0]["gain_vs_immediat"] > 0,
        f"attendre a rapporté {clotures[0]['gain_vs_immediat']:+.3f}$ de plus que vendre tout de suite",
    )


# ============================================================
print("\n[3] Sortie STOP-LOSS — le prix s'effondre")
# ============================================================
pa._positions.clear()
ouvrir_position("BBBUSDT", prix_achat=1.0, montant=50.0, profit_initial=-0.5)
poser_prix("BBBUSDT", 0.90)  # -10% -> bien en dessous du stop-loss de -3%
clotures = pa.verifier_positions()
verifier(len(clotures) == 1 and clotures[0]["issue"] == "stop_loss", "clôturée par stop-loss")
verifier(pa.nb_positions_ouvertes() == 0, "emplacement libéré")


# ============================================================
print("\n[4] Sortie TIMEOUT — le prix stagne")
# ============================================================
pa._positions.clear()
ouvrir_position("CCCUSDT", prix_achat=1.0, montant=50.0, profit_initial=-0.5)
poser_prix("CCCUSDT", 0.995)  # légèrement négatif, mais au-dessus du stop-loss
clotures = pa.verifier_positions()
verifier(len(clotures) == 0, "reste ouverte tant que le délai n'est pas écoulé")

# On force l'expiration du délai
pa._positions[0]["timestamp_ouverture"] = time.time() - config.DUREE_MAX_ATTENTE_SEC - 1
clotures = pa.verifier_positions()
verifier(len(clotures) == 1 and clotures[0]["issue"] == "timeout", "clôturée par timeout")
verifier(pa.nb_positions_ouvertes() == 0, "emplacement libéré")


# ============================================================
print("\n[5] Aucune position ne peut rester bloquée sur un flux mort")
# ============================================================
pa._positions.clear()
ouvrir_position("DDDUSDT")
# Aucun prix posé du tout -> _profit_actuel renvoie None
clotures = pa.verifier_positions()
verifier(len(clotures) == 0, "pas de clôture sur une donnée absente (on ne devine pas un prix)")

pa._positions[0]["timestamp_ouverture"] = time.time() - config.DUREE_MAX_ATTENTE_SEC - 1
clotures = pa.verifier_positions()
verifier(
    len(clotures) == 1 and clotures[0]["issue"] == "timeout",
    "MAIS le timeout ferme quand même la position (pas de blocage éternel)",
)


# ============================================================
print("\n[6] Le plafond protège la capacité de trading")
# ============================================================
pa._positions.clear()
ouvertes = sum(1 for i in range(10) if ouvrir_position(f"TOK{i}USDT"))
verifier(
    ouvertes == config.MAX_POSITIONS_EN_ATTENTE,
    f"{ouvertes} positions ouvertes sur 10 tentatives (plafond {config.MAX_POSITIONS_EN_ATTENTE})",
)
verifier(not pa.peut_ouvrir("AUTREUSDT"), "toute nouvelle position est refusée une fois le plafond atteint")
verifier(
    config.MAX_TOKENS_EN_STOCK - config.MAX_POSITIONS_EN_ATTENTE >= 7,
    f"il reste {config.MAX_TOKENS_EN_STOCK - config.MAX_POSITIONS_EN_ATTENTE} emplacements pour trader normalement",
)

# Un même symbole ne peut pas occuper deux emplacements
pa._positions.clear()
ouvrir_position("EEEUSDT")
verifier(not pa.peut_ouvrir("EEEUSDT"), "un même symbole ne peut pas ouvrir deux positions")


# ============================================================
print("\n[7] Les pertes restent VISIBLES dans les statistiques")
# ============================================================
pa._positions.clear()
trades_avant = len(pt._lire_trades_valides())
reussis_avant = pt._etat_papier["nb_trades_reussis"]

ouvrir_position("FFFUSDT", prix_achat=1.0, montant=50.0, profit_initial=-0.5)
poser_prix("FFFUSDT", 0.90)
pa.verifier_positions()  # -> stop_loss, donc perte

trades_apres = pt._lire_trades_valides()
verifier(
    len(trades_apres) == trades_avant + 1,
    "une position clôturée à perte APPARAÎT bien dans trades_papier.csv",
)
verifier(
    pt._etat_papier["nb_trades_reussis"] == reussis_avant,
    "…et n'est PAS comptée comme réussie (le taux de réussite reste honnête)",
)
if trades_apres:
    verifier(trades_apres[-1]["profit_usdt"] < 0, f"profit enregistré négatif : {trades_apres[-1]['profit_usdt']:+.3f}$")


# ============================================================
print("\n[8] Le CSV permet de trancher objectivement")
# ============================================================
stats = pa.statistiques()
verifier(stats["total"] >= 4, f"{stats['total']} positions clôturées enregistrées")
verifier("gain_total_vs_vente_immediate" in stats, "l'écart vs vente immédiate est calculé")
print(f"     → gagnantes {stats['gagnantes']} | stop-loss {stats['stop_loss']} | timeout {stats['timeout']}")
print(f"     → écart total vs vente immédiate : {stats['gain_total_vs_vente_immediate']:+.3f}$")

print("\n  --- Aperçu du résumé Telegram ---")
for ligne in pa.resume_telegram().split("\n"):
    print("   ", ligne.replace("<b>", "").replace("</b>", ""))


# ============================================================
for chemin in (pa.CSV_PATH, pt.CSV_PATH):
    if os.path.exists(chemin):
        os.remove(chemin)

print("\n" + "=" * 60)
if echecs:
    print(f"❌ {len(echecs)} TEST(S) EN ÉCHEC :")
    for e in echecs:
        print(f"   - {e}")
    sys.exit(1)
print("✅ TOUS LES TESTS PASSENT")
