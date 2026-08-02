"""
Tests des correctifs du 02/08 — à lancer avec : python3 test_correctifs.py

Couvre :
  1. Charge Telegram sous la limite (3 messages x MAX_ALERTES_PAR_MINUTE)
  2. Pas de constante dupliquée dans config.py
  3. envoyer_alerte retourne un message_id (et reste compatible avec `if x:`)
  4. Le suivi n'envoie rien si l'alerte n'est pas partie (orphelins)
  5. Le suivi omet le résumé si négatif du début à la fin (anti-spam)
  6. Le suivi envoie bien EN RÉPONSE à l'alerte (reply_to_message_id)
  7. Le mode nuit ne coupe QUE les notifications, pas la collecte
  8. La purge mémoire fonctionne
  9. opportunity_logger : vraies 5s en parallèle + label de rentabilité

Aucun accès réseau : tout est simulé.
"""

import asyncio
import ast
import collections
import csv
import os
import sys
import time

echecs = []


def verifier(condition, message):
    if condition:
        print(f"  ✅ {message}")
    else:
        print(f"  ❌ {message}")
        echecs.append(message)


# ============================================================
print("\n[1] Charge Telegram")
# ============================================================
import config

messages_par_opportunite = 3  # alerte + trade papier + suivi
charge = config.MAX_ALERTES_PAR_MINUTE * messages_par_opportunite
verifier(
    charge <= 20,
    f"{config.MAX_ALERTES_PAR_MINUTE} alertes/min x {messages_par_opportunite} msg = "
    f"{charge}/min (limite Telegram ~20)",
)


# ============================================================
print("\n[2] Doublons dans config.py")
# ============================================================
source = open("config.py", encoding="utf-8").read()
noms = [
    cible.id
    for noeud in ast.parse(source).body
    if isinstance(noeud, ast.Assign)
    for cible in noeud.targets
    if isinstance(cible, ast.Name)
]
doublons = [nom for nom, n in collections.Counter(noms).items() if n > 1]
verifier(not doublons, f"aucune constante définie deux fois (trouvé : {doublons or 'aucune'})")


# ============================================================
print("\n[3] envoyer_alerte retourne un message_id")
# ============================================================
import telegram_notifier as tn

tn.TELEGRAM_BOT_TOKEN = "faux"
tn.TELEGRAM_CHAT_ID = "123"

payloads_envoyes = []
_id_suivant = [1000]
_echouer = [False]


async def faux_envoyer_payload(payload):
    payloads_envoyes.append(payload)
    if _echouer[0]:
        return None
    _id_suivant[0] += 1
    return _id_suivant[0]


tn._envoyer_payload = faux_envoyer_payload


class FausseOpp:
    def __init__(self, symbole="COTIUSDT", exchanges=("gateio", "binance")):
        self.type_arbitrage = "inter_exchange"
        self.description = f"Acheter {symbole}"
        self.spread_brut_pct = 3.0
        self.frais_total_pct = 0.3
        self.spread_net_pct = 2.7
        self.exchanges = list(exchanges)
        self.symboles = [symbole]
        self.timestamp = time.time()
        self.score_ml = 0.06
        self.liquidite_info = None


async def test_message_id():
    tn._dernieres_notifications.clear()
    mid = await tn.envoyer_alerte(FausseOpp())
    verifier(isinstance(mid, int) and mid > 0, f"message_id retourné : {mid}")
    verifier(bool(mid), "un message_id reste 'vrai' → le code existant `if envoye:` marche encore")

    # Cooldown : deuxième envoi immédiat sur la même clé → None
    mid2 = await tn.envoyer_alerte(FausseOpp())
    verifier(mid2 is None, "cooldown anti-spam → retourne None (et non True comme avant)")


asyncio.run(test_message_id())


# ============================================================
print("\n[4+5+6] Comportement du suivi (orphelins / anti-spam / réponse)")
# ============================================================
import suivi_opportunite

CSV_SUIVI = suivi_opportunite.CSV_PATH


def cache_prix(spread_favorable: bool, frais_ok: bool = True):
    """Construit un faux cache prix_live. spread_favorable=True -> gros écart."""
    maintenant = time.time()
    prix_achat = 0.01
    prix_vente = 0.012 if spread_favorable else 0.0100001
    return {
        "gateio": {"COTIUSDT": {"bid": 0, "ask": prix_achat, "timestamp": maintenant}},
        "binance": {"COTIUSDT": {"bid": prix_vente, "ask": 0, "timestamp": maintenant}},
    }


resumes = []


async def faux_message_simple(texte, repondre_a=None):
    resumes.append({"texte": texte, "repondre_a": repondre_a})
    return 5555


tn.envoyer_message_simple = faux_message_simple

# Accélère les tests : 3 lectures espacées de 0.01s au lieu de 10 x 1s
suivi_opportunite.DUREE_SUIVI_SEC = 3
suivi_opportunite.INTERVALLE_SEC = 0.01


async def test_suivi():
    global resumes

    # --- 4. Alerte non partie (message_id None) -> aucun résumé ---
    resumes = []
    if os.path.exists(CSV_SUIVI):
        os.remove(CSV_SUIVI)
    await suivi_opportunite.suivre_opportunite(
        FausseOpp(), cache_prix(spread_favorable=True),
        message_id_alerte=None, notifier=False,
    )
    verifier(len(resumes) == 0, "alerte non envoyée → aucun résumé Telegram (plus d'orphelins)")
    verifier(os.path.exists(CSV_SUIVI), "…mais le CSV est quand même rempli (données conservées)")

    # --- 5. Spread négatif du début à la fin -> résumé omis ---
    resumes = []
    suivi_opportunite.SUIVI_ENVOYER_SEULEMENT_SI_POSITIF = True
    await suivi_opportunite.suivre_opportunite(
        FausseOpp(), cache_prix(spread_favorable=False),
        message_id_alerte=777, notifier=True,
    )
    verifier(len(resumes) == 0, "spread négatif sur toute la fenêtre → résumé omis (anti-spam)")

    # --- 5b. Même cas mais filtre désactivé -> résumé envoyé ---
    resumes = []
    suivi_opportunite.SUIVI_ENVOYER_SEULEMENT_SI_POSITIF = False
    await suivi_opportunite.suivre_opportunite(
        FausseOpp(), cache_prix(spread_favorable=False),
        message_id_alerte=777, notifier=True,
    )
    verifier(len(resumes) == 1, "filtre désactivé → le résumé négatif est bien envoyé")
    suivi_opportunite.SUIVI_ENVOYER_SEULEMENT_SI_POSITIF = True

    # --- 6. Spread positif -> résumé envoyé EN RÉPONSE à l'alerte ---
    resumes = []
    await suivi_opportunite.suivre_opportunite(
        FausseOpp(), cache_prix(spread_favorable=True),
        message_id_alerte=777, notifier=True,
    )
    verifier(len(resumes) == 1, "spread positif → résumé envoyé")
    if resumes:
        verifier(resumes[0]["repondre_a"] == 777, f"résumé rattaché à l'alerte (reply_to={resumes[0]['repondre_a']})")


asyncio.run(test_suivi())


# ============================================================
print("\n[7] Mode nuit : notifications coupées, collecte maintenue")
# ============================================================
import paper_trading
import inspect

signature = inspect.signature(paper_trading.simuler_trade)
verifier("notifier" in signature.parameters, "simuler_trade accepte un paramètre `notifier`")
verifier(
    signature.parameters["notifier"].default is True,
    "…qui vaut True par défaut (comportement inchangé pour les appels existants)",
)

signature_suivi = inspect.signature(suivi_opportunite.suivre_opportunite)
verifier("notifier" in signature_suivi.parameters, "suivre_opportunite accepte `notifier`")
verifier("message_id_alerte" in signature_suivi.parameters, "suivre_opportunite accepte `message_id_alerte`")

# Le mode nuit ne doit plus bloquer le traitement dans bot_fusionne_v1
source_bot = open("bot_fusionne_v1.py", encoding="utf-8").read()
verifier(
    "(not telegram_menu_bot.etat_bot.mode_nuit)\n                and _peut_alerter" not in source_bot,
    "mode_nuit n'est plus dans la condition qui gouverne le trade papier",
)
verifier(
    "notifier=not mode_nuit" in source_bot,
    "le mode nuit est passé au trade papier comme simple coupure de notification",
)


# ============================================================
print("\n[8] Purge mémoire")
# ============================================================
import bot_fusionne_v1 as bot

bot._dernier_alerte_par_symbole.clear()
maintenant = time.time()
bot._dernier_alerte_par_symbole["VIEUX"] = maintenant - 10_000
bot._dernier_alerte_par_symbole["RECENT"] = maintenant - 1
bot._dernier_purge_etats = 0
bot._purger_etats_periodiquement(maintenant)
verifier("VIEUX" not in bot._dernier_alerte_par_symbole, "entrée périmée supprimée")
verifier("RECENT" in bot._dernier_alerte_par_symbole, "entrée récente conservée")

# Purge côté telegram_notifier
tn._dernieres_notifications.clear()
tn._dernieres_notifications["vieille"] = maintenant - 500
tn._dernieres_notifications["recente"] = maintenant - 5
tn._dernier_purge_notifications = 0
tn._purger_notifications(maintenant)
verifier(
    "vieille" not in tn._dernieres_notifications and "recente" in tn._dernieres_notifications,
    "purge du dictionnaire anti-spam Telegram",
)


# ============================================================
print("\n[9] opportunity_logger : timing réel + label de rentabilité")
# ============================================================
import opportunity_logger

if os.path.exists(opportunity_logger.CSV_PATH):
    os.remove(opportunity_logger.CSV_PATH)


class OppTrouvee:
    def __init__(self, exchanges, spread_net_pct):
        self.exchanges = exchanges
        self.spread_net_pct = spread_net_pct


def detecteur_toujours_la(symbole):
    return [OppTrouvee(["gateio", "binance"], 0.30)]


async def test_logger():
    opp = FausseOpp()
    opp.spread_net_pct = 0.40
    debut = time.time()
    await opportunity_logger.logger_avec_suivi(opp, detecteur_toujours_la, None, None)
    duree = time.time() - debut

    verifier(
        4.5 < duree < 6.0,
        f"durée {duree:.2f}s → vraies 5s en parallèle (avant : 2.5s ; séquentiel aurait donné 7s)",
    )

    with open(opportunity_logger.CSV_PATH, newline="", encoding="utf-8") as f:
        lignes = list(csv.DictReader(f))
    ligne = lignes[0]
    verifier("rentable_apres_5s" in ligne, "colonne `rentable_apres_5s` présente")
    verifier(
        ligne["confirmee_5s"] == "1" and ligne["rentable_apres_5s"] == "0",
        f"spread 'confirmé' mais NON rentable une fois le retrait inclus "
        f"({ligne['spread_net_pct_apres_5s']}% → {ligne['spread_net_reel_pct_apres_5s']}%)",
    )


asyncio.run(test_logger())


# ============================================================
# Nettoyage + bilan
# ============================================================
for chemin in (CSV_SUIVI, opportunity_logger.CSV_PATH):
    if os.path.exists(chemin):
        os.remove(chemin)

print("\n" + "=" * 60)
if echecs:
    print(f"❌ {len(echecs)} TEST(S) EN ÉCHEC :")
    for e in echecs:
        print(f"   - {e}")
    sys.exit(1)
print("✅ TOUS LES TESTS PASSENT")
