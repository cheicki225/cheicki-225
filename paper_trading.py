"""
Mode papier (trading simulé) avec gestion du risque
========================================================
Simule l'exécution des opportunités détectées, SANS aucun argent réel ni
appel API authentifié — pour savoir objectivement combien le bot aurait
vraiment gagné/perdu, ET pour tester des règles de gestion du risque
avant de les appliquer un jour à de l'argent réel.

Fonctionnalités de gestion du risque :
1. Élimination d'une crypto après 5 échecs consécutifs (déjà existant)
2. Circuit breaker global : pause du bot après trop de pertes consécutives
   toutes cryptos confondues (signal d'un problème plus large)
3. Stop-loss journalier : coupe les trades papier si le profit du jour
   devient trop négatif, reset automatique le lendemain
4. Contrôle de rentabilité : un trade n'est compté "réussi" que si le profit
   net est positif APRÈS tous les frais (trading + retrait réel)
5. Score de confiance par crypto : classement du taux de réussite réel

⚠️ Aucune clé API n'est utilisée ici — carnets d'ordres publics uniquement.

⚠️ NOTE DE NOMMAGE (corrigé le 02/08) : le code parle de "double
vérification" un peu partout (`double_verif_ok`, colonne CSV
`double_verification_ok`, compteur `nb_trades_rejetes_double_verif`,
"Rejetés (double vérif)" dans le dashboard). C'est un vestige : il n'y a
plus qu'UN SEUL contrôle de profondeur, et `double_verif_ok` vaut
simplement `profit_net_usdt > 0`, c'est-à-dire "le trade est rentable".
Ces noms sont conservés VOLONTAIREMENT car les renommer casserait
index.html (qui lit `double_verification_ok`) et rendrait illisible tout
l'historique CSV déjà accumulé. Lis-les comme "trade rentable".
"""

import asyncio
import csv
import logging
import os
import time
from datetime import date

import orderbook_depth
import health_manager
import stockage
from config import (
    CIRCUIT_BREAKER_PERTES_CONSECUTIVES, CIRCUIT_BREAKER_ACTIVE, STOP_LOSS_JOURNALIER_USDT,
    CAPITAL_PAR_EXCHANGE_PAPIER,
    SEUIL_REEQUILIBRAGE_PCT, FRAIS_TRANSFERT_SIMULE_USDT, RESEAU_PREFERE, RESEAU_FALLBACK,
    MAX_TOKENS_EN_STOCK, VALEUR_STOCK_PAR_TOKEN_USDT, SUIVI_STOCKS_ACTIF,
    FRAIS_TRADING_PCT, MONTANT_PAR_TRADE_USDT,
)

log = logging.getLogger("paper_trading")

CSV_PATH = stockage.chemin_donnees("trades_papier.csv")

import gestion_fichiers
gestion_fichiers.enregistrer_fichier(CSV_PATH)
COLONNES = [
    "timestamp", "symbole", "exchange_achat", "exchange_vente",
    "montant_usdt", "spread_affiche_pct",
    "prix_achat_reel", "prix_vente_reel", "spread_reel_pct",
    "liquidite_suffisante", "double_verification_ok", "profit_usdt", "frais_usdt",
]

# MONTANT_PAR_TRADE_USDT est maintenant défini dans config.py (importé
# ci-dessus) — il sert aussi au calcul du seuil de bénéfice réel dans
# bot_fusionne_v1, donc les deux doivent impérativement être la même valeur.
# Reste accessible en `paper_trading.MONTANT_PAR_TRADE_USDT` : plusieurs
# fichiers y font référence sous ce nom (bot_fusionne_v1, api_server).

# Aucun minimum — n'importe quel montant réellement exécutable (même faible)
# déclenche un trade papier, à prix toujours honnête. Seul un carnet
# véritablement vide (zéro niveau de prix) bloque encore.
MONTANT_MIN_EXECUTABLE_USDT = 0.0

# --- 1. Élimination par crypto après échecs consécutifs (déjà existant) ---
MAX_ECHECS_CONSECUTIFS = 5
_echecs_consecutifs = {}

# --- 2. Circuit breaker global (toutes cryptos confondues) ---
_pertes_consecutives_globales = 0
_circuit_breaker_actif = False

# --- 3. Stop-loss journalier ---
_jour_actuel = {"date": None, "profit_du_jour": 0.0, "stop_loss_declenche": False}

_etat_papier = {
    "capital_initial": 1000.0,
    "profit_cumule_usdt": 0.0,
    "nb_trades_reussis": 0,
    "nb_trades_rejetes_liquidite": 0,
    "nb_trades_rejetes_double_verif": 0,
    "nb_trades_rejetes_stock": 0,
    "nb_trades_total": 0,
    "nb_cryptos_eliminees": 0,
}

# --- 5. Score de confiance par crypto ---
_stats_par_crypto = {}  # symbole -> {"reussis": N, "total": N}

# --- Soldes fictifs par exchange (pour le rééquilibrage simulé) ---
_soldes_virtuels = {}  # exchange -> solde USDT fictif
TRANSFERTS_CSV_PATH = stockage.chemin_donnees("transferts_papier.csv")
gestion_fichiers.enregistrer_fichier(TRANSFERTS_CSV_PATH)
TRANSFERTS_COLONNES = [
    "timestamp", "exchange_source", "exchange_destination",
    "montant_usdt", "frais_usdt", "reseau", "raison",
]


def _init_csv():
    if not os.path.exists(CSV_PATH):
        with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(COLONNES)


def _ecrire_ligne(ligne):
    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=COLONNES).writerow(ligne)


def _reset_jour_si_necessaire():
    """Remet à zéro le compteur de profit journalier si on a changé de jour."""
    aujourdhui = str(date.today())
    if _jour_actuel["date"] != aujourdhui:
        if _jour_actuel["date"] is not None:
            log.info(
                f"📅 Nouveau jour — reset stop-loss journalier "
                f"(hier : {_jour_actuel['profit_du_jour']:+.3f}$)"
            )
        _jour_actuel["date"] = aujourdhui
        _jour_actuel["profit_du_jour"] = 0.0
        _jour_actuel["stop_loss_declenche"] = False


def stop_loss_journalier_actif():
    """True si le stop-loss du jour est déclenché — les trades papier doivent être suspendus."""
    _reset_jour_si_necessaire()
    return _jour_actuel["stop_loss_declenche"]


def circuit_breaker_actif():
    """True si le circuit breaker global est déclenché (toujours False si désactivé via config.py)."""
    if not CIRCUIT_BREAKER_ACTIVE:
        return False
    return _circuit_breaker_actif


def reinitialiser_circuit_breaker():
    """Permet de relancer manuellement après une pause circuit breaker (ex: via Telegram)."""
    global _pertes_consecutives_globales, _circuit_breaker_actif
    _pertes_consecutives_globales = 0
    _circuit_breaker_actif = False
    log.info("♻️ Circuit breaker réinitialisé manuellement")


# Interrupteur pour l'élimination automatique par performance (5 échecs
# consécutifs -> blacklist). DÉSACTIVÉ par défaut le 31/07 sur demande —
# toutes les cryptos doivent pouvoir continuer à trader même après des
# échecs répétés. Le compteur d'échecs continue d'être suivi en interne
# même désactivé (pour les stats), juste aucune action n'est prise dessus.
_elimination_performance_active = False


def definir_elimination_active(actif: bool):
    global _elimination_performance_active
    _elimination_performance_active = bool(actif)
    log.info(f"Élimination par performance : {'activée' if _elimination_performance_active else 'désactivée'}")


def elimination_active() -> bool:
    return _elimination_performance_active


def _enregistrer_resultat_et_verifier_elimination(symbol, succes):
    """Élimination par crypto après MAX_ECHECS_CONSECUTIFS échecs d'affilée — désactivable, voir elimination_active()."""
    if succes:
        _echecs_consecutifs[symbol] = 0
        return

    _echecs_consecutifs[symbol] = _echecs_consecutifs.get(symbol, 0) + 1
    nb = _echecs_consecutifs[symbol]

    if nb >= MAX_ECHECS_CONSECUTIFS:
        _echecs_consecutifs[symbol] = 0  # reset le compteur dans tous les cas, actif ou pas

        if not _elimination_performance_active:
            return  # suivi désactivé -> aucune action, la crypto continue de trader normalement

        if symbol not in health_manager.symboles_blacklistes():
            health_manager.blacklister_manuellement(
                symbol,
                f"Échec du mode papier {nb} fois d'affilée (spread réel insuffisant "
                f"ou liquidité trop faible après vérification de profondeur)"
            )
            _etat_papier["nb_cryptos_eliminees"] += 1
            log.warning(f"🗑️ {symbol} ÉLIMINÉE après {nb} échecs papier consécutifs")


def _verifier_circuit_breaker_global(succes):
    """Incrémente/réinitialise le compteur global de pertes. Déclenche la pause si seuil atteint."""
    global _pertes_consecutives_globales, _circuit_breaker_actif

    if not CIRCUIT_BREAKER_ACTIVE:
        return  # désactivé via config.py — aucun suivi, aucune pause automatique

    if succes:
        _pertes_consecutives_globales = 0
        return

    _pertes_consecutives_globales += 1

    if _pertes_consecutives_globales >= CIRCUIT_BREAKER_PERTES_CONSECUTIVES and not _circuit_breaker_actif:
        _circuit_breaker_actif = True
        log.warning(
            f"🚨 CIRCUIT BREAKER DÉCLENCHÉ : {_pertes_consecutives_globales} pertes "
            f"consécutives (mode papier) — trades papier suspendus (détection et "
            f"alertes réelles continuent normalement)"
        )
        try:
            asyncio.create_task(_envoyer_alerte_circuit_breaker())
        except Exception as e:
            log.error(f"Impossible d'envoyer l'alerte circuit breaker : {e}")


async def _envoyer_alerte_circuit_breaker():
    try:
        import telegram_notifier
        await telegram_notifier.envoyer_message_simple(
            f"🚨 <b>CIRCUIT BREAKER DÉCLENCHÉ</b>\n\n"
            f"{CIRCUIT_BREAKER_PERTES_CONSECUTIVES} pertes consécutives détectées en mode papier.\n\n"
            f"⚠️ Seul le <b>mode papier</b> est suspendu — la détection et les "
            f"alertes réelles continuent normalement.\n\n"
            f"Utilise \"♻️ Réinitialiser\" dans le menu pour reprendre le mode papier."
        )
    except Exception as e:
        log.error(f"Échec envoi alerte circuit breaker : {e}")


def _verifier_stop_loss_journalier(profit_usdt):
    """Met à jour le profit du jour et déclenche le stop-loss si le seuil est franchi."""
    _reset_jour_si_necessaire()
    _jour_actuel["profit_du_jour"] += profit_usdt

    if _jour_actuel["profit_du_jour"] <= STOP_LOSS_JOURNALIER_USDT and not _jour_actuel["stop_loss_declenche"]:
        _jour_actuel["stop_loss_declenche"] = True
        log.warning(
            f"🛑 STOP-LOSS JOURNALIER DÉCLENCHÉ : {_jour_actuel['profit_du_jour']:+.3f}$ "
            f"aujourd'hui (seuil : {STOP_LOSS_JOURNALIER_USDT}$) — trades papier suspendus jusqu'à demain"
        )
        try:
            asyncio.create_task(_envoyer_alerte_stop_loss())
        except Exception:
            pass


async def _envoyer_alerte_stop_loss():
    try:
        import telegram_notifier
        await telegram_notifier.envoyer_message_simple(
            f"🛑 <b>STOP-LOSS JOURNALIER DÉCLENCHÉ</b>\n\n"
            f"Perte du jour : {_jour_actuel['profit_du_jour']:+.3f}$\n"
            f"Seuil configuré : {STOP_LOSS_JOURNALIER_USDT}$\n\n"
            f"Trades papier suspendus jusqu'à demain (reset automatique à minuit)."
        )
    except Exception as e:
        log.error(f"Échec envoi alerte stop-loss : {e}")


def _enregistrer_score_crypto(symbol, succes):
    """Alimente le classement de confiance par crypto (bouton Top performers)."""
    stats = _stats_par_crypto.setdefault(symbol, {"reussis": 0, "total": 0})
    stats["total"] += 1
    if succes:
        stats["reussis"] += 1


def _init_transferts_csv():
    if not os.path.exists(TRANSFERTS_CSV_PATH):
        with open(TRANSFERTS_CSV_PATH, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(TRANSFERTS_COLONNES)


def _obtenir_solde(exchange):
    """Initialise le solde fictif d'un exchange à sa première utilisation."""
    if exchange not in _soldes_virtuels:
        _soldes_virtuels[exchange] = CAPITAL_PAR_EXCHANGE_PAPIER
    return _soldes_virtuels[exchange]


def _appliquer_mouvement_trade(ex_achat, ex_vente, montant_usdt, profit_net_usdt):
    """
    Simule le mouvement de capital d'un trade réussi : on suppose un
    aller-retour complet sur l'exchange d'achat (stratégie "fonds
    pré-répartis" choisie plus tôt — pas de transfert physique à chaque
    trade), et le profit net est crédité sur l'exchange de vente.
    """
    _obtenir_solde(ex_achat)
    _obtenir_solde(ex_vente)
    _soldes_virtuels[ex_vente] += profit_net_usdt


# ============================================================
# STOCKS DE TOKENS PAR PLATEFORME
# ============================================================
# Vendre un token sur une plateforme suppose de l'y détenir. Un transfert
# d'USDT ne crée pas ce stock — il faut avoir acheté le token à l'avance.
# Avec un capital limité, seul un petit nombre de tokens peut être
# pré-positionné : c'est cette contrainte qui rend inexploitable la
# majorité des opportunités détectées.
#
# {exchange: {symbole: quantité}}
_stocks_tokens: dict[str, dict[str, float]] = {}


def _tokens_en_stock() -> set:
    """Ensemble des symboles pour lesquels un stock est ouvert quelque part."""
    return {s for par_ex in _stocks_tokens.values() for s, q in par_ex.items() if q > 0}


def _stock_disponible(exchange: str, symbole: str) -> float:
    return _stocks_tokens.get(exchange, {}).get(symbole, 0.0)


def _tenter_ouvrir_stock(exchange: str, symbole: str, prix_unitaire: float, frais_pct: float) -> bool:
    """
    Ouvre une position de stock sur un token, si le capital et le nombre de
    positions le permettent. Retourne True si le stock est désormais ouvert.

    Le coût est réel : on immobilise VALEUR_STOCK_PAR_TOKEN_USDT sur la
    plateforme, plus les frais d'achat. C'est du capital qui ne travaille
    plus ailleurs.
    """
    if prix_unitaire <= 0:
        return False

    deja = _tokens_en_stock()
    if symbole not in deja and len(deja) >= MAX_TOKENS_EN_STOCK:
        return False  # toutes les positions sont occupées

    _obtenir_solde(exchange)
    cout_achat = VALEUR_STOCK_PAR_TOKEN_USDT * (1 + frais_pct / 100)
    if _soldes_virtuels[exchange] < cout_achat:
        return False  # pas assez de capital sur cette plateforme

    _soldes_virtuels[exchange] -= cout_achat
    # Les frais d'acquisition sont un coût réel, pas une écriture neutre
    frais = VALEUR_STOCK_PAR_TOKEN_USDT * (frais_pct / 100)
    _etat_papier["profit_cumule_usdt"] -= frais
    _jour_actuel["profit_du_jour"] -= frais

    quantite = VALEUR_STOCK_PAR_TOKEN_USDT / prix_unitaire
    _stocks_tokens.setdefault(exchange, {})
    _stocks_tokens[exchange][symbole] = _stocks_tokens[exchange].get(symbole, 0.0) + quantite

    log.info(
        f"📦 Stock ouvert : {quantite:.6g} {symbole} sur {exchange} "
        f"({VALEUR_STOCK_PAR_TOKEN_USDT:.0f}$ immobilisés, frais {frais:.3f}$)"
    )
    return True


def _appliquer_mouvement_stock(ex_achat, ex_vente, symbole, quantite):
    """Après le trade : le token quitte la plateforme de vente, arrive sur celle d'achat."""
    _stocks_tokens.setdefault(ex_vente, {})
    _stocks_tokens.setdefault(ex_achat, {})
    _stocks_tokens[ex_vente][symbole] = _stocks_tokens[ex_vente].get(symbole, 0.0) - quantite
    _stocks_tokens[ex_achat][symbole] = _stocks_tokens[ex_achat].get(symbole, 0.0) + quantite


def obtenir_stocks() -> dict:
    """Copie des stocks de tokens actuels, pour le dashboard."""
    return {ex: dict(t) for ex, t in _stocks_tokens.items() if any(q > 0 for q in t.values())}


def obtenir_soldes():
    """Retourne une copie des soldes fictifs actuels par exchange."""
    return dict(_soldes_virtuels)


def _verifier_besoin_reequilibrage():
    """
    Si un exchange a un solde significativement plus bas que la moyenne des
    autres, simule un transfert automatique depuis l'exchange le plus riche
    — TOUJOURS en simulation, aucune clé API réelle n'est utilisée ici.
    """
    if len(_soldes_virtuels) < 2:
        return

    moyenne = sum(_soldes_virtuels.values()) / len(_soldes_virtuels)
    if moyenne <= 0:
        return

    exchange_pauvre = min(_soldes_virtuels, key=_soldes_virtuels.get)
    solde_pauvre = _soldes_virtuels[exchange_pauvre]

    if solde_pauvre < moyenne * (SEUIL_REEQUILIBRAGE_PCT / 100):
        exchange_riche = max(_soldes_virtuels, key=_soldes_virtuels.get)
        if exchange_riche == exchange_pauvre:
            return

        solde_riche = _soldes_virtuels[exchange_riche]
        montant_transfert = round((solde_riche - moyenne) / 2, 2)
        if montant_transfert > FRAIS_TRANSFERT_SIMULE_USDT:
            simuler_transfert(
                exchange_riche, exchange_pauvre, montant_transfert,
                raison=f"Rééquilibrage auto ({exchange_pauvre} à {solde_pauvre:.2f}$, "
                       f"sous {SEUIL_REEQUILIBRAGE_PCT}% de la moyenne {moyenne:.2f}$)"
            )


def simuler_transfert(exchange_source, exchange_dest, montant_usdt, raison="Manuel"):
    """
    Simule un transfert entre 2 exchanges — NE FAIT AUCUN appel API réel.

    Les frais sont ceux du réseau le moins cher réellement utilisable entre
    les deux plateformes (voir frais_retrait), avec repli sur la constante
    de configuration si les données publiques ne sont pas disponibles.

    ⚠️ CORRECTIF : ces frais sont désormais déduits de profit_cumule_usdt.
    Une version antérieure les retirait des soldes virtuels sans jamais
    toucher au profit affiché — celui-ci était donc surestimé du montant
    total des rééquilibrages, sans que rien ne le signale.
    """
    _init_transferts_csv()
    _obtenir_solde(exchange_source)
    _obtenir_solde(exchange_dest)

    import frais_retrait
    info = frais_retrait.frais_transfert(exchange_source, exchange_dest, montant_usdt)
    frais = info["frais"] if not info["est_estime"] else FRAIS_TRANSFERT_SIMULE_USDT
    reseau = info["reseau"] if not info["est_estime"] else RESEAU_PREFERE

    if _soldes_virtuels[exchange_source] < montant_usdt + frais:
        log.warning(f"🚫 Transfert simulé impossible : solde insuffisant sur {exchange_source}")
        return {"execute": False, "raison": "solde insuffisant"}

    _soldes_virtuels[exchange_source] -= (montant_usdt + frais)
    _soldes_virtuels[exchange_dest] += montant_usdt

    # Le rééquilibrage est un COÛT RÉEL : il doit peser sur le profit affiché
    _etat_papier["profit_cumule_usdt"] -= frais
    _jour_actuel["profit_du_jour"] -= frais

    with open(TRANSFERTS_CSV_PATH, "a", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=TRANSFERTS_COLONNES).writerow({
            "timestamp": time.time(),
            "exchange_source": exchange_source, "exchange_destination": exchange_dest,
            "montant_usdt": montant_usdt, "frais_usdt": round(frais, 4),
            "reseau": reseau, "raison": raison,
        })

    log.info(
        f"💸 Transfert SIMULÉ : {montant_usdt:.2f}$ {exchange_source} → {exchange_dest} "
        f"(frais {frais:.3f}$, réseau {reseau}) — {raison}"
    )
    return {"execute": True, "montant": montant_usdt, "frais": frais}


def stats_soldes():
    """Résumé des soldes fictifs par exchange, pour le menu Telegram."""
    if not _soldes_virtuels:
        return "💼 Aucun solde fictif enregistré pour l'instant (attends le premier trade papier)."

    total = sum(_soldes_virtuels.values())
    lignes = ["💼 <b>SOLDES FICTIFS PAR EXCHANGE</b> (mode papier)\n"]
    for exchange, solde in sorted(_soldes_virtuels.items(), key=lambda x: -x[1]):
        lignes.append(f"• {exchange} : {solde:.2f}$")
    lignes.append(f"\nTotal : {total:.2f}$")
    lignes.append("\n⚠️ Simulation uniquement — aucun vrai transfert n'est exécuté.")
    return "\n".join(lignes)


def historique_transferts(limite=10):
    """Derniers transferts simulés — pour le menu Telegram."""
    if not os.path.exists(TRANSFERTS_CSV_PATH):
        return "💸 Aucun transfert simulé pour l'instant."
    with open(TRANSFERTS_CSV_PATH, newline="", encoding="utf-8") as f:
        lignes = list(csv.DictReader(f))
    if not lignes:
        return "💸 Aucun transfert simulé pour l'instant."

    dernieres = lignes[-limite:]
    texte = ["💸 <b>DERNIERS TRANSFERTS SIMULÉS</b>\n"]
    for l in reversed(dernieres):
        texte.append(
            f"• {l['montant_usdt']}$ : {l['exchange_source']} → {l['exchange_destination']} "
            f"(frais {l['frais_usdt']}$)"
        )
    return "\n".join(texte)


async def simuler_trade(opp, frais_pct_total, montant_usdt=MONTANT_PAR_TRADE_USDT, notifier: bool = True):
    """
    Simule l'exécution d'une opportunité d'arbitrage inter-exchange, avec
    gestion complète du risque :
    1. Bloque si le circuit breaker global ou le stop-loss journalier est actif
    2. Contrôle de profondeur de carnet réel
    3. Calcule le profit réaliste après slippage ET frais (trading + retrait)
    4. Met à jour circuit breaker, stop-loss et score de confiance
    5. Élimine la crypto après 5 échecs consécutifs

    notifier : False = la simulation tourne normalement et le CSV est rempli,
        mais aucun message Telegram n'est envoyé (mode nuit). La collecte de
        données ne doit JAMAIS s'arrêter juste parce que les notifications
        sont coupées — c'est précisément la nuit qu'on veut continuer à
        mesurer sans être réveillé.
    """
    _init_csv()

    if circuit_breaker_actif():
        return {"execute": False, "raison": "circuit breaker actif"}
    if stop_loss_journalier_actif():
        return {"execute": False, "raison": "stop-loss journalier actif"}

    ex_achat, ex_vente = opp.exchanges
    symbol = opp.symboles[0]

    # Re-vérifie la blacklist ICI, pas juste au moment de la détection —
    # avec un token très volatil, des dizaines de tâches peuvent déjà être
    # en file d'attente au moment où la crypto se fait blacklister ; sans
    # ce deuxième contrôle, elles s'exécutent quand même jusqu'au bout
    if symbol in health_manager.symboles_blacklistes():
        return {"execute": False, "raison": "blacklistée entre-temps"}

    _etat_papier["nb_trades_total"] += 1

    # --- Contrôle instantané unique (pas de délai d'attente) ---
    _debut_verif = time.time()
    resultat = await orderbook_depth.estimer_execution_reelle(ex_achat, ex_vente, symbol, montant_usdt)
    duree_verif_ms = (time.time() - _debut_verif) * 1000

    montant_executable = resultat.get("montant_executable", 0.0) if resultat else 0.0
    prix_ok = bool(resultat and resultat.get("prix_achat_reel", 0) > 0 and resultat.get("prix_vente_reel", 0) > 0)

    ligne = {
        "timestamp": time.time(), "symbole": symbol,
        "exchange_achat": ex_achat, "exchange_vente": ex_vente,
        "montant_usdt": montant_usdt, "spread_affiche_pct": opp.spread_net_pct,
        "prix_achat_reel": "", "prix_vente_reel": "", "spread_reel_pct": "",
        "liquidite_suffisante": False, "double_verification_ok": False,
        "profit_usdt": "", "frais_usdt": "",
    }

    # Rejet uniquement si VRAIMENT rien d'exploitable (carnet quasi vide) —
    # sinon on trade avec le montant réellement disponible, à prix honnête
    if not prix_ok or montant_executable < MONTANT_MIN_EXECUTABLE_USDT:
        _etat_papier["nb_trades_rejetes_liquidite"] += 1
        _ecrire_ligne(ligne)
        log.info(f"🚫 Trade papier REJETÉ (liquidité quasi nulle, {montant_executable:.2f}$ dispo < {MONTANT_MIN_EXECUTABLE_USDT}$ min) : {symbol} {ex_achat}->{ex_vente} | vérif={duree_verif_ms:.0f}ms")
        _enregistrer_resultat_et_verifier_elimination(symbol, succes=False)
        _verifier_circuit_breaker_global(succes=False)
        _enregistrer_score_crypto(symbol, succes=False)
        return {"execute": False, "raison": "liquidité quasi nulle"}

    # Montant réellement tradé = le plus petit entre ce qu'on visait et ce
    # que le carnet peut vraiment absorber — jamais plus que ce qui est
    # réellement disponible, jamais un prix fictif
    montant_reel = min(montant_usdt, montant_executable)
    liquidite_partielle = montant_reel < montant_usdt - 0.01

    # --- CONTRÔLE DE STOCK ---
    # Vendre un token sur une plateforme suppose de l'y détenir. Sans stock,
    # le trade est simplement IMPOSSIBLE dans la réalité — même si l'écart
    # de prix est excellent. C'est la contrainte qui élimine la majorité des
    # opportunités détectées quand le capital est limité.
    if SUIVI_STOCKS_ACTIF:
        prix_vente_ref = resultat["prix_vente_reel"]
        quantite_requise = montant_reel / prix_vente_ref if prix_vente_ref > 0 else 0

        if _stock_disponible(ex_vente, symbol) < quantite_requise:
            # Pas de stock : on tente d'en ouvrir un (coût réel, capital immobilisé)
            ouvert = _tenter_ouvrir_stock(
                ex_vente, symbol, prix_vente_ref, FRAIS_TRADING_PCT.get(ex_vente, 0.10)
            )
            if not ouvert or _stock_disponible(ex_vente, symbol) < quantite_requise:
                _etat_papier["nb_trades_rejetes_stock"] = _etat_papier.get("nb_trades_rejetes_stock", 0) + 1
                _ecrire_ligne(ligne)
                log.info(
                    f"📦 Trade papier REJETÉ (pas de {symbol} en stock sur {ex_vente}) : "
                    f"{ex_achat}->{ex_vente} | positions ouvertes {len(_tokens_en_stock())}/{MAX_TOKENS_EN_STOCK}"
                )
                return {"execute": False, "raison": "stock de token insuffisant"}

    spread_reel_pct = resultat["spread_reel_pct"]
    frais_trading_usdt = montant_reel * (frais_pct_total / 100)

    # Frais de RETRAIT : boucler un arbitrage suppose de rapatrier les fonds
    # de la plateforme de vente vers celle d'achat. Le coût dépend du réseau
    # le moins cher utilisable DES DEUX CÔTÉS — un réseau bon marché que la
    # destination n'accepte pas ne compte pas.
    # C'est un coût FIXE en dollars, donc proportionnellement écrasant sur un
    # petit trade (1$ sur 50$ = 2%, soit plus que l'écart lui-même).
    import frais_retrait
    info_retrait = frais_retrait.frais_transfert(ex_vente, ex_achat, montant_reel)
    frais_retrait_usdt = info_retrait["frais"]

    frais_usdt = frais_trading_usdt + frais_retrait_usdt
    profit_net_usdt = montant_reel * (spread_reel_pct / 100) - frais_usdt

    # --- POSITION EN ATTENTE ---
    # Le trade serait perdant : au lieu d'encaisser la perte maintenant, on
    # peut garder la position et vendre automatiquement dès qu'elle repasse
    # positive (voir positions_attente.py). Trois garde-fous bornent
    # l'attente : durée max, stop-loss, et nombre d'emplacements.
    # Le résultat FINAL sera enregistré à la clôture via
    # enregistrer_resultat_position_attente() — rien n'est perdu pour les
    # statistiques, c'est juste comptabilisé plus tard.
    if profit_net_usdt <= 0:
        try:
            import positions_attente
            if positions_attente.ouvrir(
                symbole=symbol, exchange_achat=ex_achat, exchange_vente=ex_vente,
                montant_usdt=montant_reel, prix_achat=resultat["prix_achat_reel"],
                profit_initial_usdt=profit_net_usdt, frais_usdt=frais_usdt,
            ):
                if notifier:
                    asyncio.create_task(_notifier_mise_en_attente(
                        symbol, ex_achat, ex_vente, montant_reel, profit_net_usdt
                    ))
                return {
                    "execute": False, "raison": "mis en attente",
                    "en_attente": True, "profit_potentiel_evite": profit_net_usdt,
                }
        except Exception as e:
            # Ne doit jamais empêcher le trade normal de se dérouler
            log.error(f"Échec mise en attente ({symbol}) : {e} — trade traité normalement")

    # Rentable seulement si l'écart couvre TOUS les frais, retrait compris
    double_verif_ok = profit_net_usdt > 0

    ligne.update({
        "montant_usdt": round(montant_reel, 4),  # le montant RÉELLEMENT tradé, pas celui visé au départ
        "prix_achat_reel": resultat["prix_achat_reel"],
        "prix_vente_reel": resultat["prix_vente_reel"],
        "spread_reel_pct": spread_reel_pct,
        "liquidite_suffisante": not liquidite_partielle,
        "double_verification_ok": double_verif_ok,
        "profit_usdt": round(profit_net_usdt, 4),
        "frais_usdt": round(frais_usdt, 4),
    })
    _ecrire_ligne(ligne)

    if liquidite_partielle:
        log.info(f"⚠️ Trade papier PARTIEL : {symbol} {ex_achat}->{ex_vente} | {montant_reel:.2f}$ sur {montant_usdt:.2f}$ visés (carnet limité)")

    # Un trade n'est compté "réussi" que si la double vérif ET le profit sont positifs
    succes = double_verif_ok and profit_net_usdt > 0
    if not double_verif_ok:
        _etat_papier["nb_trades_rejetes_double_verif"] += 1

    _etat_papier["profit_cumule_usdt"] += profit_net_usdt
    if succes:
        _etat_papier["nb_trades_reussis"] += 1

    _enregistrer_resultat_et_verifier_elimination(symbol, succes=succes)
    _verifier_circuit_breaker_global(succes=succes)
    _verifier_stop_loss_journalier(profit_net_usdt)
    _enregistrer_score_crypto(symbol, succes=succes)

    # Met à jour les soldes fictifs par exchange et vérifie si un
    # rééquilibrage simulé est nécessaire (toujours en simulation)
    _appliquer_mouvement_trade(ex_achat, ex_vente, montant_reel, profit_net_usdt)
    # Le token quitte la plateforme de vente et se reconstitue sur celle
    # d'achat — c'est ce déplacement qui impose le SECOND transfert.
    if SUIVI_STOCKS_ACTIF:
        _appliquer_mouvement_stock(ex_achat, ex_vente, symbol, quantite_requise)
    _verifier_besoin_reequilibrage()

    log.info(
        f"🧪 Trade papier {'✅' if succes else '❌'} : {symbol} "
        f"{ex_achat}->{ex_vente} | spread réel={spread_reel_pct:.3f}% "
        f"(affiché={opp.spread_net_pct:.3f}%, double vérif={'OK' if double_verif_ok else 'ÉCHEC'}) "
        f"| profit net={profit_net_usdt:+.3f}$ | vérif={duree_verif_ms:.0f}ms"
    )

    try:
        if not notifier:
            # Mode nuit : tout ce qui précède (CSV, stats, circuit breaker,
            # stocks, soldes, score par crypto) a bien été fait — seule la
            # notification saute. La collecte de données continue la nuit.
            return {"execute": True, "profit_usdt": profit_net_usdt, "spread_reel_pct": spread_reel_pct, "succes": succes}

        import telegram_notifier
        emoji = "✅" if succes else "❌"
        base_asset = symbol[:-4] if symbol.endswith("USDT") else symbol
        prix_achat = resultat["prix_achat_reel"]
        prix_vente = resultat["prix_vente_reel"]
        quantite = montant_reel / prix_achat if prix_achat else 0
        montant_vente_brut = quantite * prix_vente

        pct_dispo = min(100, montant_reel / montant_usdt * 100) if montant_usdt else 0
        emoji_liq = "🟢" if pct_dispo >= 95 else "🟡" if pct_dispo >= 20 else "🔴"
        ligne_liquidite = f"{emoji_liq} Liquidité : {montant_reel:.2f}$ / {montant_usdt:.0f}$ visés ({pct_dispo:.0f}%)\n"

        # Prix top-of-book annoncés dans l'alerte vs prix réellement obtenus en
        # profondeur de carnet. L'écart dit si l'opportunité a tenu ses promesses :
        # achat au-dessus de l'annoncé = payé plus cher que prévu (défavorable),
        # vente en dessous = revendu moins cher que prévu (défavorable aussi).
        p_achat_ann = getattr(opp, "prix_achat_annonce", None)
        p_vente_ann = getattr(opp, "prix_vente_annonce", None)
        if p_achat_ann and p_vente_ann:
            ecart_achat = (prix_achat - p_achat_ann) / p_achat_ann * 100
            ecart_vente = (prix_vente - p_vente_ann) / p_vente_ann * 100
            # Jugé sur l'EFFET NET, pas sur chaque prix isolément : payer un peu
            # plus cher à l'achat n'est pas grave si la vente compense largement.
            # C'est l'écart entre spread réel et spread annoncé qui compte vraiment.
            ecart_net = spread_reel_pct - opp.spread_net_pct
            emoji_ecart = "✅" if ecart_net >= -0.05 else "⚠️"
            ligne_annonce = (
                f"📢 Annoncé : achat {p_achat_ann:.6g}$ / vente {p_vente_ann:.6g}$\n"
                f"{emoji_ecart} Écart exécution : achat {ecart_achat:+.3f}% · vente {ecart_vente:+.3f}%\n"
            )
        else:
            ligne_annonce = ""

        marque = " (estimé)" if info_retrait["est_estime"] else f" via {info_retrait['reseau']}"
        ligne_frais = (
            f"Frais : transaction {frais_trading_usdt:.3f}$ + "
            f"retrait {frais_retrait_usdt:.3f}${marque} = {frais_usdt:.3f}$\n"
        )

        asyncio.create_task(telegram_notifier.envoyer_message_simple(
            f"🧪 <b>Trade papier {emoji}</b>\n\n"
            f"{symbol} : {ex_achat} → {ex_vente}\n"
            f"Acheté : {montant_reel:.2f}$ → {quantite:.6g} {base_asset} @ {prix_achat:.6g}$\n"
            f"Vendu : {quantite:.6g} {base_asset} → {montant_vente_brut:.2f}$ @ {prix_vente:.6g}$\n"
            f"{ligne_annonce}"
            f"{ligne_liquidite}"
            f"Spread réel : {spread_reel_pct:.3f}% (affiché {opp.spread_net_pct:.3f}%)\n"
            f"{ligne_frais}"
            f"Profit net : {profit_net_usdt:+.3f}$\n"
            f"⏱️ Temps de vérification : {duree_verif_ms:.0f}ms"
        ))
    except Exception as e:
        log.error(f"Échec notification trade papier : {e}")

    return {"execute": True, "profit_usdt": profit_net_usdt, "spread_reel_pct": spread_reel_pct, "succes": succes}


async def _notifier_mise_en_attente(symbole, ex_achat, ex_vente, montant_usdt, profit_initial):
    """Alerte Telegram quand une position est mise en attente au lieu d'être vendue à perte."""
    try:
        import telegram_notifier
        import positions_attente
        from config import MAX_POSITIONS_EN_ATTENTE, DUREE_MAX_ATTENTE_SEC, STOP_LOSS_POSITION_PCT

        await telegram_notifier.envoyer_message_simple(
            f"⏳ <b>Position mise en attente</b>\n\n"
            f"{symbole} : {ex_achat} → {ex_vente}\n"
            f"Montant : {montant_usdt:.2f}$\n"
            f"Vente immédiate donnerait : {profit_initial:+.3f}$\n"
            f"→ On attend que ça repasse positif\n\n"
            f"Emplacements : {positions_attente.nb_positions_ouvertes()}/{MAX_POSITIONS_EN_ATTENTE}\n"
            f"Sortie forcée après {DUREE_MAX_ATTENTE_SEC // 60} min "
            f"ou à {STOP_LOSS_POSITION_PCT}%"
        )
    except Exception as e:
        log.error(f"Échec notification mise en attente : {e}")


def enregistrer_resultat_position_attente(symbole, exchange_achat, exchange_vente,
                                          montant_usdt, prix_achat, prix_vente,
                                          profit_usdt, frais_usdt):
    """
    Enregistre dans les statistiques papier le résultat FINAL d'une position
    qui avait été mise en attente. Appelé par positions_attente._cloturer().

    Indispensable pour l'honnêteté des chiffres : sans ça, un trade dérouté
    vers l'attente disparaîtrait des statistiques s'il finit perdant, et le
    taux de réussite afficherait un chiffre flatteur mais faux.
    """
    _init_csv()

    succes = profit_usdt > 0
    _ecrire_ligne({
        "timestamp": time.time(), "symbole": symbole,
        "exchange_achat": exchange_achat, "exchange_vente": exchange_vente,
        "montant_usdt": round(montant_usdt, 4), "spread_affiche_pct": "",
        "prix_achat_reel": prix_achat, "prix_vente_reel": prix_vente,
        "spread_reel_pct": "", "liquidite_suffisante": True,
        "double_verification_ok": succes,
        "profit_usdt": round(profit_usdt, 4), "frais_usdt": round(frais_usdt, 4),
    })

    _etat_papier["nb_trades_reussis"] += 1 if succes else 0
    _etat_papier["profit_cumule_usdt"] += profit_usdt
    if not succes:
        _etat_papier["nb_trades_rejetes_double_verif"] += 1

    _enregistrer_resultat_et_verifier_elimination(symbole, succes=succes)
    _verifier_circuit_breaker_global(succes=succes)
    _verifier_stop_loss_journalier(profit_usdt)
    _enregistrer_score_crypto(symbole, succes=succes)
    _appliquer_mouvement_trade(exchange_achat, exchange_vente, montant_usdt, profit_usdt)


def stats_papier():
    """Résumé complet du mode papier — profit, taux de réussite, circuit breaker, stop-loss, derniers trades en détail."""
    _reset_jour_si_necessaire()
    e = _etat_papier
    capital_actuel = e["capital_initial"] + e["profit_cumule_usdt"]
    # Un trade rejeté n'a JAMAIS été exécuté : il ne doit pas compter au
    # dénominateur du taux de réussite. Les rejets pour liquidité étaient
    # bien soustraits, mais PAS ceux pour stock insuffisant (ajoutés plus
    # tard) — chacun faisait donc baisser le taux affiché comme s'il
    # s'agissait d'un trade perdant, alors qu'il n'a rien tenté du tout.
    nb_executes = (
        e["nb_trades_total"]
        - e["nb_trades_rejetes_liquidite"]
        - e.get("nb_trades_rejetes_stock", 0)
    )
    taux_reussite = (e["nb_trades_reussis"] / nb_executes * 100) if nb_executes > 0 else 0

    statut_cb = "🚨 ACTIF (trades papier suspendus)" if _circuit_breaker_actif else "🟢 OK"
    statut_sl = "🛑 ACTIF (trades suspendus)" if _jour_actuel["stop_loss_declenche"] else "🟢 OK"

    # Détail des derniers trades — même niveau d'info que l'alerte Telegram
    # envoyée au moment du trade (voir simuler_trade), limité à 3 pour
    # rester largement sous la limite de 4096 caractères d'un message Telegram
    derniers = historique_trades(limite=3)
    if derniers:
        blocs_trades = []
        for t in derniers:
            symbole = t["symbole"]
            ticker = symbole[:-4] if symbole.endswith("USDT") else symbole
            try:
                montant = float(t["montant_usdt"])
                prix_achat = float(t["prix_achat_reel"])
                prix_vente = float(t["prix_vente_reel"])
                spread_reel = float(t["spread_reel_pct"])
                spread_affiche = float(t["spread_affiche_pct"])
                frais = float(t["frais_usdt"])
            except (TypeError, ValueError):
                continue  # ligne CSV corrompue, on l'ignore plutôt que planter tout l'affichage
            profit = t["profit_usdt"]  # déjà float via historique_trades()
            verif_ok = str(t.get("double_verification_ok")) == "True"
            quantite = montant / prix_achat if prix_achat else 0
            montant_vente = quantite * prix_vente
            emoji = "✅" if profit >= 0 else "❌"

            blocs_trades.append(
                f"{emoji} <b>{symbole}</b> ({t['exchange_achat']} → {t['exchange_vente']})\n"
                f"Acheté : {montant:.2f}$ → {quantite:.6g} {ticker} @ {prix_achat:.6g}$\n"
                f"Vendu : {quantite:.6g} {ticker} → {montant_vente:.2f}$ @ {prix_vente:.6g}$\n"
                f"Spread réel : {spread_reel:.3f}% (affiché {spread_affiche:.3f}%)\n"
                f"Double vérif : {'OK' if verif_ok else 'ÉCHEC'}\n"
                f"Profit net : {profit:+.3f}$ (frais {frais:.3f}$ inclus)"
            )
        texte_trades = "\n\n📋 <b>DERNIERS TRADES</b>\n\n" + "\n\n".join(blocs_trades) if blocs_trades else ""
    else:
        texte_trades = "\n\n📋 Aucun trade exécuté pour l'instant."

    return (
        f"🧪 <b>MODE PAPIER (simulation)</b>\n\n"
        f"Capital fictif initial : {e['capital_initial']:.2f}$\n"
        f"Profit/perte cumulé : {e['profit_cumule_usdt']:+.3f}$\n"
        f"Capital fictif actuel : {capital_actuel:.2f}$\n\n"
        f"Trades tentés : {e['nb_trades_total']}\n"
        f"Rejetés (liquidité) : {e['nb_trades_rejetes_liquidite']}\n"
        f"Rejetés (double vérif) : {e['nb_trades_rejetes_double_verif']}\n"
        f"Trades exécutés : {nb_executes}\n"
        f"Taux de réussite : {taux_reussite:.1f}%\n"
        f"🗑️ Cryptos éliminées : {e['nb_cryptos_eliminees']}\n\n"
        f"⚙️ <b>Gestion du risque</b>\n"
        f"Circuit breaker : {statut_cb} ({_pertes_consecutives_globales}/{CIRCUIT_BREAKER_PERTES_CONSECUTIVES} pertes)\n"
        f"Stop-loss journalier : {statut_sl}\n"
        f"Profit du jour : {_jour_actuel['profit_du_jour']:+.3f}$ (seuil : {STOP_LOSS_JOURNALIER_USDT}$)\n\n"
        f"⚠️ Simulation uniquement — aucun argent réel engagé."
        f"{texte_trades}"
    )


def top_performers(limite=10):
    """Classement des cryptos par taux de réussite réel en mode papier."""
    if not _stats_par_crypto:
        return "🏆 Aucune donnée pour l'instant — laisse le bot tourner un peu."

    classement = []
    for symbole, stats in _stats_par_crypto.items():
        if stats["total"] < 2:  # ignore les cryptos avec trop peu de données
            continue
        taux = stats["reussis"] / stats["total"] * 100
        classement.append((symbole, taux, stats["reussis"], stats["total"]))

    if not classement:
        return "🏆 Pas assez de données par crypto pour l'instant (minimum 2 trades chacune)."

    classement.sort(key=lambda x: x[1], reverse=True)

    lignes = ["🏆 <b>TOP PERFORMERS</b> (taux de réussite mode papier)\n"]
    for symbole, taux, reussis, total in classement[:limite]:
        emoji = "✅" if taux >= 50 else "⚠️" if taux >= 20 else "❌"
        lignes.append(f"{emoji} {symbole} : {taux:.0f}% ({reussis}/{total})")

    return "\n".join(lignes)


# ============================================================
# MÉTRIQUES AVANCÉES — Profit Factor, Average R/R, courbe d'équité, historique
# ============================================================
# Calculées à partir de trades_papier.csv, PAS inventées. Si les données ne
# permettent pas un calcul fiable (pas assez de trades, pas encore de perte
# pour diviser, etc.), ces fonctions retournent None plutôt qu'un chiffre
# trompeur — mieux vaut ne rien afficher qu'afficher un nombre faux sur un
# dashboard de trading, même en mode simulation.

def _lire_trades_valides():
    """
    Lit trades_papier.csv et ne garde que les lignes avec un profit_usdt
    exploitable (trades réellement EXÉCUTÉS — les rejets pour liquidité
    insuffisante ont profit_usdt vide et sont ignorés ici). Triées par
    timestamp croissant (ordre chronologique).
    """
    if not os.path.exists(CSV_PATH):
        return []

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        lignes = list(csv.DictReader(f))

    valides = []
    for l in lignes:
        brut = l.get("profit_usdt", "")
        if brut in ("", None):
            continue
        try:
            l["profit_usdt"] = float(brut)
            l["timestamp"] = float(l["timestamp"])
            valides.append(l)
        except (TypeError, ValueError):
            continue

    valides.sort(key=lambda l: l["timestamp"])
    return valides


def historique_trades(limite=100):
    """Derniers trades EXÉCUTÉS (profit connu), les plus récents en premier — pour une future page Trades."""
    valides = _lire_trades_valides()
    return list(reversed(valides[-limite:]))


def calculer_profit_factor():
    """
    Profit Factor = somme des gains / somme des pertes (valeur absolue).
    Retourne None si aucune perte n'a encore eu lieu (ratio non défini —
    afficher "infini" serait trompeur) ou s'il n'y a aucun trade exécuté.
    """
    valides = _lire_trades_valides()
    if not valides:
        return None

    gains = sum(l["profit_usdt"] for l in valides if l["profit_usdt"] > 0)
    pertes = sum(l["profit_usdt"] for l in valides if l["profit_usdt"] < 0)

    if pertes == 0:
        return None

    return round(gains / abs(pertes), 2)


def calculer_average_rr():
    """
    Average R/R (payoff ratio) = gain moyen des trades gagnants / perte
    moyenne des trades perdants (valeur absolue).

    ⚠️ Ce bot n'a pas de stop-loss PAR TRADE (montant fixe, pas de distance
    de risque définie individuellement) — ce n'est donc pas un vrai ratio
    risque/récompense au sens classique (comme sur un compte prop avec SL
    fixé à l'ouverture). C'est l'approximation standard ("payoff ratio")
    utilisée par la plupart des dashboards de trading en son absence.
    Retourne None si pas de gains ou pas de pertes enregistrés.
    """
    valides = _lire_trades_valides()
    gains = [l["profit_usdt"] for l in valides if l["profit_usdt"] > 0]
    pertes = [l["profit_usdt"] for l in valides if l["profit_usdt"] < 0]

    if not gains or not pertes:
        return None

    gain_moyen = sum(gains) / len(gains)
    perte_moyenne = abs(sum(pertes) / len(pertes))

    if perte_moyenne == 0:
        return None

    return round(gain_moyen / perte_moyenne, 2)


def courbe_equity(limite_points=300):
    """
    Courbe d'équité réelle : capital cumulé (capital initial + profit net)
    après chaque trade EXÉCUTÉ, en ordre chronologique. Sous-échantillonne
    si trop de points pour ne pas envoyer un payload énorme au dashboard
    (garde toujours le tout premier et le tout dernier point).
    """
    valides = _lire_trades_valides()
    if not valides:
        return []

    points = []
    cumul = _etat_papier["capital_initial"]
    for l in valides:
        cumul += l["profit_usdt"]
        points.append({"timestamp": l["timestamp"], "capital": round(cumul, 3)})

    if len(points) <= limite_points:
        return points

    pas = len(points) / limite_points
    echantillon = [points[int(i * pas)] for i in range(limite_points - 1)]
    echantillon.append(points[-1])
    return echantillon


def stats_taille_trades() -> dict:
    """
    Sépare les trades exécutés en "taille pleine" (≥95% du montant visé) et
    "partiels" (carnet trop limité pour absorber tout le montant), avec le
    taux de réussite de chaque groupe séparément.

    Existe pour repérer un piège statistique : depuis qu'on trade même avec
    une liquidité faible, un bon taux de réussite global peut être tiré
    presque entièrement par des micro-trades faciles à valider (ne mangent
    que le tout premier niveau du carnet, celui qui a généré l'alerte),
    sans dire grand-chose de la performance à pleine taille (50$), qui elle
    doit creuser plus profond dans un carnet potentiellement dégradé.
    """
    valides = _lire_trades_valides()
    if not valides:
        return {
            "nb_trades": 0, "montant_moyen": None,
            "nb_pleins": 0, "nb_partiels": 0,
            "taux_reussite_pleins": None, "taux_reussite_partiels": None,
            "nb_gagnants": 0, "nb_perdants": 0,
        }

    seuil_plein = MONTANT_PAR_TRADE_USDT * 0.95  # tolérance 5% (arrondis de calcul)
    montants, pleins, partiels = [], [], []

    for l in valides:
        try:
            m = float(l["montant_usdt"])
        except (TypeError, ValueError, KeyError):
            continue
        montants.append(m)
        (pleins if m >= seuil_plein else partiels).append(l)

    def _taux(liste):
        if not liste:
            return None
        reussis = sum(1 for l in liste if l["profit_usdt"] > 0)
        return round(reussis / len(liste) * 100, 1)

    return {
        "nb_trades": len(montants),
        "montant_moyen": round(sum(montants) / len(montants), 2) if montants else None,
        "nb_pleins": len(pleins),
        "nb_partiels": len(partiels),
        "taux_reussite_pleins": _taux(pleins),
        "taux_reussite_partiels": _taux(partiels),
        # Décompte global sur l'historique complet (pas seulement la session)
        "nb_gagnants": sum(1 for l in valides if l["profit_usdt"] > 0),
        "nb_perdants": sum(1 for l in valides if l["profit_usdt"] < 0),
    }


def _agreger_profit_par_crypto() -> dict:
    """
    Agrège trades_papier.csv par symbole. Retourne
    {symbole: {profit_total, nb_trades, nb_gains, nb_pertes}} — utilisé à la
    fois par classement_profit_par_crypto() (top/bottom N) et
    stats_toutes_cryptos() (liste complète, pour la page Cryptos suivies).
    """
    valides = _lire_trades_valides()
    par_crypto = {}
    for l in valides:
        s = l["symbole"]
        entree = par_crypto.setdefault(s, {"profit_total": 0.0, "nb_trades": 0, "nb_gains": 0, "nb_pertes": 0})
        entree["profit_total"] += l["profit_usdt"]
        entree["nb_trades"] += 1
        if l["profit_usdt"] > 0:
            entree["nb_gains"] += 1
        elif l["profit_usdt"] < 0:
            entree["nb_pertes"] += 1
    return par_crypto


def stats_toutes_cryptos() -> dict:
    """
    {symbole: {taux_reussite, profit_total, nb_trades, nb_gains, nb_pertes}}
    pour TOUTE crypto ayant déjà généré au moins un trade papier.

    taux_reussite vient de la session en cours (_stats_par_crypto) ; les
    autres champs de l'historique complet persistant (trades_papier.csv).
    None si pas encore de donnée exploitable pour cette métrique précise
    (jamais un chiffre inventé).
    """
    profits = _agreger_profit_par_crypto()
    tous_symboles = set(profits.keys()) | set(_stats_par_crypto.keys())

    resultat = {}
    for s in tous_symboles:
        session = _stats_par_crypto.get(s, {"total": 0, "reussis": 0})
        p = profits.get(s, {"profit_total": 0.0, "nb_trades": 0, "nb_gains": 0, "nb_pertes": 0})
        resultat[s] = {
            "taux_reussite": round(session["reussis"] / session["total"] * 100, 1) if session["total"] > 0 else None,
            "profit_total": round(p["profit_total"], 3) if p["nb_trades"] > 0 else None,
            "nb_trades": p["nb_trades"],
            "nb_gains": p.get("nb_gains", 0),
            "nb_pertes": p.get("nb_pertes", 0),
        }
    return resultat


def classement_profit_par_crypto(limite: int = 10):
    """
    Classement des cryptos par PROFIT CUMULÉ (montant réel en $, pas juste
    taux de réussite) — calculé sur l'historique complet de trades_papier.csv,
    donc persistant entre les redémarrages (contrairement à _stats_par_crypto
    qui ne suit que la session en cours).

    Retourne (meilleures, pires) : deux listes triées, chacune au format
    {symbole, profit_total, nb_trades, nb_gains, nb_pertes}.
    """
    par_crypto = _agreger_profit_par_crypto()
    if not par_crypto:
        return [], []

    classement = [
        {"symbole": s, "profit_total": round(v["profit_total"], 3), "nb_trades": v["nb_trades"],
         "nb_gains": v["nb_gains"], "nb_pertes": v["nb_pertes"]}
        for s, v in par_crypto.items()
    ]
    classement.sort(key=lambda x: x["profit_total"], reverse=True)

    meilleures = [c for c in classement[:limite] if c["profit_total"] > 0]
    pires = [c for c in reversed(classement[-limite:]) if c["profit_total"] < 0]
    return meilleures, pires


if __name__ == "__main__":
    print(stats_papier())
    print()
    print(top_performers())
