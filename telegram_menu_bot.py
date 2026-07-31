"""
Menu Telegram interactif — Bot Arbitrage (version HTTP brut)
=================================================================
Réécrit sans la librairie python-telegram-bot (bug de compatibilité
avec Python 3.14 côté librairie). Utilise directement l'API Telegram
via aiohttp, comme telegram_notifier.py qui fonctionne déjà.

Fonctionne par "long polling" : on interroge Telegram toutes les
quelques secondes pour récupérer les nouveaux messages/clics de bouton.

Installation :
    pip install aiohttp python-dotenv --break-system-packages
"""

import asyncio
import logging
import os
import time
from collections import deque
from datetime import timedelta
import aiohttp
from dotenv import load_dotenv

import stockage
from config import SEUIL_MIN_INTER_EXCHANGE_PCT, SEUIL_MIN_TRIANGULAIRE_PCT

load_dotenv(stockage.chemin_donnees(".env"))

log = logging.getLogger("telegram_menu")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_API_BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"


def _session_avec_dns_force() -> aiohttp.ClientSession:
    """
    Sur Windows, pycares/aiodns échoue parfois à lire la config DNS système
    (bug connu), d'où le forçage explicite de Google DNS. Sur Linux/Mac
    (ex: déploiement Railway), ce forçage peut lui-même échouer selon la
    version de pycares installée ('Channel' object has no attribute
    'gethostbyname') — on retombe alors sur le résolveur par défaut, qui
    fonctionne normalement très bien sur ces systèmes.
    """
    import platform
    if platform.system() == "Windows":
        try:
            from aiohttp.resolver import AsyncResolver
            resolver = AsyncResolver(nameservers=["8.8.8.8", "8.8.4.4"])
            connector = aiohttp.TCPConnector(resolver=resolver)
            return aiohttp.ClientSession(connector=connector)
        except Exception:
            pass
    return aiohttp.ClientSession()


# ============================================================
# CAPTURE DES ERREURS/WARNINGS DE TOUTE L'APPLICATION
# ============================================================
# Handler de logging qui garde les 15 derniers WARNING/ERROR en mémoire,
# peu importe le module d'où ils viennent (websockets, health_manager, etc.)
# -> alimente le bouton "Dernières erreurs" du menu.
class _CaptureErreurs(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.WARNING)
        self.erreurs: deque = deque(maxlen=15)

    def emit(self, record):
        self.erreurs.append({
            "texte": self.format(record),
            "niveau": record.levelname,
            "timestamp": time.time(),
        })


_capture_erreurs = _CaptureErreurs()
_capture_erreurs.setFormatter(logging.Formatter("[%(name)s] %(message)s"))


def activer_capture_erreurs():
    """À appeler une fois au démarrage pour brancher la capture sur tous les logs de l'app."""
    logging.getLogger().addHandler(_capture_erreurs)


# ============================================================
# ÉTAT PARTAGÉ DU BOT
# ============================================================
class EtatBot:
    def __init__(self):
        self.en_marche = True
        self.en_pause = False
        self.heure_demarrage = time.time()
        self.opportunites_trouvees: list = []
        self.seuil_inter_exchange = SEUIL_MIN_INTER_EXCHANGE_PCT  # lu depuis config.py — avant : codé en dur à 0.5, ignorait config.py
        self.seuil_triangulaire = SEUIL_MIN_TRIANGULAIRE_PCT      # idem, était codé en dur à 0.4
        self.mode_nuit = False  # si True, les alertes Telegram sont mises en sourdine (le scan continue)

    def uptime_str(self) -> str:
        return str(timedelta(seconds=int(time.time() - self.heure_demarrage)))

    def enregistrer_opportunite(self, opp):
        self.opportunites_trouvees.append({"opp": opp, "timestamp": time.time()})
        if len(self.opportunites_trouvees) > 100:
            self.opportunites_trouvees.pop(0)


etat_bot = EtatBot()
prix_live_ref = {}  # assigné depuis bot_fusionne_v1.py


# ============================================================
# CONSTRUCTION DU MENU (format Telegram inline_keyboard brut)
# ============================================================
def build_main_menu() -> dict:
    def btn(texte, callback):
        return {"text": texte, "callback_data": callback}

    return {
        "inline_keyboard": [
            [btn("▶️ Démarrer", "demarrer"), btn("🔴 Arrêter", "arreter")],
            [btn("⏸️ Pause", "pause"), btn("▶️ Reprendre", "reprendre")],
            [btn("━━━ INFORMATIONS ━━━", "noop")],
            [btn("🛰️ État bot", "etat_bot")],
            [btn("📜 Historique", "historique"), btn("📊 Statistiques", "statistiques")],
            [btn("🏆 Top cryptos", "top_cryptos")],
            [btn("━━━ OUTILS PRO ━━━", "noop")],
            [btn("🚫 Blacklist", "blacklist"), btn("✅ Unblacklist", "unblacklist")],
            [btn("🎯 Seuil inter-exch.", "changer_seuil_inter"), btn("🔺 Seuil triangulaire", "changer_seuil_tri")],
            [btn("🌙 Mode nuit ON/OFF", "mode_nuit"), btn("🚨 Dernières erreurs", "dernieres_erreurs")],
            [btn("📩 Rapport maintenant", "rapport"), btn("♻️ Réinitialiser", "reinitialiser")],
            [btn("📡 Live signaux", "live_signaux"), btn("🔄 Top paires", "top_paires")],
            [btn("📈 Perf Détail", "perf_detail")],
            [btn("🤖 Stats ML", "stats_ml"), btn("🧪 Mode Papier", "mode_papier")],
            [btn("📤 Export CSV ML", "export_csv_ml")],
            [btn("🏆 Top Performers", "top_performers")],
            [btn("💼 Soldes fictifs", "soldes_papier"), btn("💸 Transferts", "historique_transferts")],
            [btn("🔑 Config API", "config_api")],
            [btn("🔄 Rafraîchir menu", "rafraichir")],
        ]
    }


A_VENIR = "🚧 Fonctionnalité pas encore implémentée — arrive dans une prochaine étape."

# Suivi de ce qu'on attend comme saisie texte après un clic bouton
# (ex: après "changer_seuil_inter", on attend un nombre dans le prochain message)
_attente_saisie: dict[int, str] = {}  # chat_id -> type de saisie attendue


def build_menu_config_api() -> dict:
    """Sous-menu : choix de l'exchange à configurer."""
    def btn(texte, callback):
        return {"text": texte, "callback_data": callback}

    import api_keys_manager
    configurees = api_keys_manager.lister_cles_configurees()

    boutons = []
    for exchange in api_keys_manager.EXCHANGES_SUPPORTES:
        emoji = "✅" if configurees.get(exchange) else "⬜"
        boutons.append([btn(f"{emoji} {exchange.capitalize()}", f"config_api_{exchange}")])
    boutons.append([btn("🔙 Retour au menu", "rafraichir")])
    return {"inline_keyboard": boutons}


def traiter_action(action: str, chat_id: int = None):
    """Retourne le texte à afficher pour une action de bouton, ou None si rien à faire."""
    if action == "noop":
        return None

    if action == "demarrer":
        etat_bot.en_marche = True
        etat_bot.en_pause = False
        return "✅ Bot démarré."

    if action == "arreter":
        etat_bot.en_marche = False
        return "🔴 Bot arrêté. Les connexions WebSocket restent actives mais le scanner ne détecte plus d'opportunités."

    if action == "pause":
        etat_bot.en_pause = True
        return "⏸️ Bot en pause."

    if action == "reprendre":
        etat_bot.en_pause = False
        return "▶️ Bot repris."

    if action == "etat_bot":
        nb_exchanges = len([e for e, s in prix_live_ref.items() if s])
        nb_prix = sum(len(s) for s in prix_live_ref.values())
        statut = "🟢 EN MARCHE" if etat_bot.en_marche and not etat_bot.en_pause else (
            "⏸️ EN PAUSE" if etat_bot.en_pause else "🔴 ARRÊTÉ"
        )
        return (
            f"🛰️ <b>ÉTAT DU BOT</b>\n\n"
            f"Statut : {statut}\n"
            f"Durée : {etat_bot.uptime_str()}\n"
            f"Exchanges connectés : {nb_exchanges}/6\n"
            f"Prix en cache : {nb_prix}\n"
            f"Opportunités trouvées (session) : {len(etat_bot.opportunites_trouvees)}\n"
            f"Seuil inter-exchange : {etat_bot.seuil_inter_exchange}%\n"
            f"Seuil triangulaire : {etat_bot.seuil_triangulaire}%\n"
            f"Mode nuit : {'🌙 ON' if etat_bot.mode_nuit else '☀️ OFF'}"
        )

    if action == "statistiques":
        return (
            f"📊 <b>STATISTIQUES</b>\n\n"
            f"Opportunités détectées : {len(etat_bot.opportunites_trouvees)}\n"
            f"Trades réels exécutés : 0 (exécution auto pas encore implémentée)\n"
            f"Durée de fonctionnement : {etat_bot.uptime_str()}"
        )

    if action == "live_signaux":
        dernieres = etat_bot.opportunites_trouvees[-5:]
        if not dernieres:
            return "📡 Aucun signal détecté pour l'instant."
        lignes = ["📡 <b>5 DERNIERS SIGNAUX</b>\n"]
        for item in reversed(dernieres):
            opp = item["opp"]
            il_y_a = int(time.time() - item["timestamp"])
            lignes.append(f"• {opp.description[:60]}... NET={opp.spread_net_pct:.3f}% (il y a {il_y_a}s)")
        return "\n".join(lignes)

    if action == "top_paires":
        compteur = {}
        for exchange, symbols in prix_live_ref.items():
            for symbol in symbols:
                compteur[symbol] = compteur.get(symbol, 0) + 1
        top = sorted(compteur.items(), key=lambda x: x[1], reverse=True)[:10]
        lignes = ["🔄 <b>TOP PAIRES</b> (nb d'exchanges où actives)\n"]
        for symbol, nb in top:
            lignes.append(f"• {symbol} : {nb}/5 exchanges")
        return "\n".join(lignes)

    if action == "changer_seuil_inter":
        _attente_saisie[chat_id] = "seuil_inter"
        return (
            f"🎯 Seuil inter-exchange actuel : <b>{etat_bot.seuil_inter_exchange}%</b>\n\n"
            f"Envoie le nouveau seuil (ex: <code>0.3</code>) dans ton prochain message."
        )

    if action == "changer_seuil_tri":
        _attente_saisie[chat_id] = "seuil_tri"
        return (
            f"🔺 Seuil triangulaire actuel : <b>{etat_bot.seuil_triangulaire}%</b>\n\n"
            f"Envoie le nouveau seuil (ex: <code>0.3</code>) dans ton prochain message."
        )

    if action == "stats_ml":
        try:
            import opportunity_logger
            return f"🤖 <b>DONNÉES ML</b>\n\n{opportunity_logger.stats_rapides()}"
        except Exception as e:
            return f"Erreur : {e}"

    if action == "mode_papier":
        try:
            import paper_trading
            return paper_trading.stats_papier()
        except Exception as e:
            return f"Erreur : {e}"

    if action == "top_performers":
        try:
            import paper_trading
            return paper_trading.top_performers()
        except Exception as e:
            return f"Erreur : {e}"

    if action == "soldes_papier":
        try:
            import paper_trading
            return paper_trading.stats_soldes()
        except Exception as e:
            return f"Erreur : {e}"

    if action == "historique_transferts":
        try:
            import paper_trading
            return paper_trading.historique_transferts()
        except Exception as e:
            return f"Erreur : {e}"

    if action == "blacklist":
        try:
            import health_manager
            bl = health_manager.charger_blacklist()
            if not bl:
                return "🚫 <b>BLACKLIST</b>\n\nAucune paire blacklistée pour l'instant."
            lignes = ["🚫 <b>PAIRES EN PANNE (blacklist)</b>\n"]
            for symbol, info in list(bl.items())[:15]:
                lignes.append(f"• {symbol} : {info['raison']}")
            lignes.append("\nEnvoie /exclure SYMBOLE pour en blacklister une manuellement.")
            return "\n".join(lignes)
        except Exception as e:
            return f"Erreur : {e}"

    if action == "reinitialiser":
        try:
            import health_manager
            import paper_trading
            health_manager.vider_blacklist()
            paper_trading.reinitialiser_circuit_breaker()
            return (
                "♻️ Réinitialisation complète :\n"
                "• Blacklist vidée — toutes les paires reprennent leur chance\n"
                "• Circuit breaker débloqué — le bot peut reprendre le trading papier"
            )
        except Exception as e:
            return f"Erreur : {e}"

    # --- HISTORIQUE (depuis le CSV, pas juste la mémoire de session) ---
    if action == "historique":
        try:
            import csv
            import opportunity_logger
            if not os.path.exists(opportunity_logger.CSV_PATH):
                return "📜 Aucun historique enregistré pour l'instant."
            with open(opportunity_logger.CSV_PATH, newline="", encoding="utf-8") as f:
                lignes = list(csv.DictReader(f))
            dernieres = lignes[-10:]
            if not dernieres:
                return "📜 Historique vide."
            texte = ["📜 <b>10 DERNIÈRES OPPORTUNITÉS LOGGÉES</b>\n"]
            for l in reversed(dernieres):
                texte.append(f"• {l['symboles']} : {l['spread_net_pct']}% net ({l['type_arbitrage']})")
            return "\n".join(texte)
        except Exception as e:
            return f"Erreur lecture historique : {e}"

    # --- TOP CRYPTOS (les plus fréquentes en opportunités, session en cours) ---
    if action == "top_cryptos":
        compteur = {}
        for item in etat_bot.opportunites_trouvees:
            symbol = item["opp"].symboles[0]
            compteur[symbol] = compteur.get(symbol, 0) + 1
        if not compteur:
            return "🏆 Aucune opportunité enregistrée pour l'instant cette session."
        top = sorted(compteur.items(), key=lambda x: x[1], reverse=True)[:10]
        lignes = ["🏆 <b>TOP CRYPTOS</b> (nb d'opportunités détectées, session)\n"]
        for symbol, nb in top:
            lignes.append(f"• {symbol} : {nb}x")
        return "\n".join(lignes)

    # --- UNBLACKLIST (demande le symbole à réintégrer) ---
    if action == "unblacklist":
        _attente_saisie[chat_id] = "unblacklist"
        return "✅ Envoie le symbole à retirer de la blacklist (ex: <code>BONKUSDT</code>)."

    # --- MODE NUIT (coupe les alertes Telegram, le scan continue) ---
    if action == "mode_nuit":
        etat_bot.mode_nuit = not etat_bot.mode_nuit
        statut = "🌙 ACTIVÉ" if etat_bot.mode_nuit else "☀️ DÉSACTIVÉ"
        detail = (
            "Les alertes Telegram sont maintenant en sourdine (le scan et la "
            "collecte ML continuent normalement en arrière-plan)."
            if etat_bot.mode_nuit else
            "Les alertes Telegram sont réactivées."
        )
        return f"🌙 <b>Mode nuit : {statut}</b>\n\n{detail}"

    # --- DERNIÈRES ERREURS ---
    if action == "dernieres_erreurs":
        if not _capture_erreurs.erreurs:
            return "🚨 Aucune erreur/avertissement enregistré pour l'instant. Bon signe !"
        lignes = ["🚨 <b>DERNIÈRES ERREURS/AVERTISSEMENTS</b>\n"]
        for err in list(_capture_erreurs.erreurs)[-10:][::-1]:
            il_y_a = int(time.time() - err["timestamp"])
            lignes.append(f"• [{err['niveau']}] {err['texte'][:100]} (il y a {il_y_a}s)")
        return "\n".join(lignes)

    # --- RAPPORT MAINTENANT (résumé combiné, envoyé immédiatement) ---
    if action == "rapport":
        nb_exchanges = len([e for e, s in prix_live_ref.items() if s])
        nb_prix = sum(len(s) for s in prix_live_ref.values())
        try:
            import opportunity_logger
            stats_ml = opportunity_logger.stats_rapides()
        except Exception:
            stats_ml = "indisponible"
        try:
            import health_manager
            nb_blackliste = len(health_manager.charger_blacklist())
        except Exception:
            nb_blackliste = "?"

        return (
            f"📩 <b>RAPPORT COMPLET</b>\n\n"
            f"⏱️ Durée : {etat_bot.uptime_str()}\n"
            f"🛰️ Exchanges connectés : {nb_exchanges}/6\n"
            f"💹 Prix en cache : {nb_prix}\n"
            f"💰 Opportunités (session) : {len(etat_bot.opportunites_trouvees)}\n"
            f"🚫 Paires blacklistées : {nb_blackliste}\n"
            f"🌙 Mode nuit : {'ON' if etat_bot.mode_nuit else 'OFF'}\n\n"
            f"🤖 <b>ML :</b>\n{stats_ml}"
        )

    # --- PERFORMANCE DÉTAILLÉE (répartition par type) ---
    if action == "perf_detail":
        opps = [item["opp"] for item in etat_bot.opportunites_trouvees]
        if not opps:
            return "📈 Aucune opportunité enregistrée pour l'instant cette session."
        inter = [o for o in opps if o.type_arbitrage == "inter_exchange"]
        tri = [o for o in opps if o.type_arbitrage == "triangulaire"]
        moy_inter = sum(o.spread_net_pct for o in inter) / len(inter) if inter else 0
        moy_tri = sum(o.spread_net_pct for o in tri) / len(tri) if tri else 0
        meilleure = max(opps, key=lambda o: o.spread_net_pct)

        return (
            f"📈 <b>PERFORMANCE DÉTAILLÉE</b> (session, {len(opps)} opportunités)\n\n"
            f"🔄 Inter-exchange : {len(inter)}x (moyenne {moy_inter:.3f}% net)\n"
            f"🔺 Triangulaire : {len(tri)}x (moyenne {moy_tri:.3f}% net)\n\n"
            f"🏆 Meilleure : {meilleure.symboles[0]} à {meilleure.spread_net_pct:.3f}% net"
        )

    if action == "rafraichir":
        return "🔄 Menu rafraîchi."

    return "Action inconnue."


# ============================================================
# APPELS API TELEGRAM BRUTS
# ============================================================
async def envoyer_menu(chat_id, texte="🤖 <b>BOT ARBITRAGE</b>\n\nChoisis une option :"):
    async with _session_avec_dns_force() as session:
        await session.post(f"{TELEGRAM_API_BASE}/sendMessage", json={
            "chat_id": chat_id, "text": texte, "parse_mode": "HTML",
            "reply_markup": build_main_menu(),
        })


async def editer_menu(chat_id, message_id, texte):
    async with _session_avec_dns_force() as session:
        await session.post(f"{TELEGRAM_API_BASE}/editMessageText", json={
            "chat_id": chat_id, "message_id": message_id, "text": texte,
            "parse_mode": "HTML", "reply_markup": build_main_menu(),
        })


async def editer_menu_custom(chat_id, message_id, texte, clavier: dict):
    """Comme editer_menu mais avec un clavier personnalisé (ex: sous-menu Config API)."""
    async with _session_avec_dns_force() as session:
        await session.post(f"{TELEGRAM_API_BASE}/editMessageText", json={
            "chat_id": chat_id, "message_id": message_id, "text": texte,
            "parse_mode": "HTML", "reply_markup": clavier,
        })


async def supprimer_message(chat_id, message_id):
    """Supprime un message — utilisé pour effacer les clés API dès qu'elles sont traitées."""
    try:
        async with _session_avec_dns_force() as session:
            await session.post(f"{TELEGRAM_API_BASE}/deleteMessage", json={
                "chat_id": chat_id, "message_id": message_id,
            })
    except Exception as e:
        log.warning(f"Impossible de supprimer le message {message_id} : {e}")


async def envoyer_document(chat_id, chemin_fichier: str, legende: str = ""):
    """
    Envoie un fichier en pièce jointe via l'API Telegram (multipart/form-data).
    Utilisé pour exporter opportunites_log.csv depuis Railway vers ton PC —
    le filesystem Railway étant éphémère, c'est le moyen le plus simple de
    récupérer les données avant qu'un redéploiement ne les efface.
    """
    async with _session_avec_dns_force() as session:
        with open(chemin_fichier, "rb") as f:
            form = aiohttp.FormData()
            form.add_field("chat_id", str(chat_id))
            if legende:
                form.add_field("caption", legende[:1024])  # limite Telegram sur les légendes
            form.add_field("document", f, filename=os.path.basename(chemin_fichier))
            async with session.post(f"{TELEGRAM_API_BASE}/sendDocument", data=form) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    log.error(f"Échec envoi document Telegram ({resp.status}) : {body[:200]}")
                    raise RuntimeError(f"Telegram a refusé l'envoi ({resp.status})")


async def gerer_export_csv(chat_id, message_id):
    """
    Nettoie le CSV (retire les symboles blacklistés a posteriori, pour ne pas
    entraîner un modèle sur des données polluées par des bugs de flux/collisions
    de ticker) puis l'envoie en pièce jointe. Callback du bouton "📤 Export CSV ML".
    """
    import opportunity_logger

    if not os.path.exists(opportunity_logger.CSV_PATH):
        await editer_menu(
            chat_id, message_id,
            "📤 Aucun fichier opportunites_log.csv pour l'instant — laisse le bot tourner un peu plus longtemps."
        )
        return

    avant = apres = None
    try:
        import health_manager
        symboles_a_retirer = health_manager.symboles_blacklistes()
        avant, apres = opportunity_logger.nettoyer_csv(symboles_a_retirer)
    except Exception as e:
        log.warning(f"Nettoyage CSV avant export échoué (envoi du fichier brut à la place) : {e}")

    taille_ko = os.path.getsize(opportunity_logger.CSV_PATH) / 1024
    legende = f"📤 opportunites_log.csv ({taille_ko:.0f} Ko)"
    if avant is not None:
        legende += f"\n🧹 Nettoyé : {avant} → {apres} lignes ({avant - apres} retirées, symboles blacklistés depuis)"

    try:
        await envoyer_document(chat_id, opportunity_logger.CSV_PATH, legende)
        await editer_menu(chat_id, message_id, "✅ CSV envoyé ci-dessus. Tu peux maintenant l'entraîner en local.")
    except Exception as e:
        await editer_menu(chat_id, message_id, f"❌ Échec de l'envoi : {e}")


async def repondre_callback(callback_query_id):
    """Accuse réception du clic (sinon le bouton reste 'en chargement' côté Telegram)."""
    async with _session_avec_dns_force() as session:
        await session.post(f"{TELEGRAM_API_BASE}/answerCallbackQuery", json={
            "callback_query_id": callback_query_id,
        })


# ============================================================
# BOUCLE DE POLLING PRINCIPALE
# ============================================================
async def demarrer_bot_telegram():
    if not TELEGRAM_BOT_TOKEN:
        log.warning("TELEGRAM_BOT_TOKEN manquant — menu Telegram désactivé")
        return

    activer_capture_erreurs()
    log.info("✅ Menu Telegram démarré — envoie /start à ton bot pour l'afficher")
    offset = 0

    while True:
        try:
            async with _session_avec_dns_force() as session:
                async with session.get(
                    f"{TELEGRAM_API_BASE}/getUpdates",
                    params={"offset": offset, "timeout": 25},
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    data = await resp.json()

            for update in data.get("result", []):
                offset = update["update_id"] + 1

                if "message" in update:
                    texte = update["message"].get("text", "")
                    chat_id = update["message"]["chat"]["id"]

                    if texte == "/start":
                        await envoyer_menu(chat_id)

                    elif texte.startswith("/exclure "):
                        symbole = texte.replace("/exclure ", "").strip().upper()
                        import health_manager
                        health_manager.blacklister_manuellement(symbole, "Exclu manuellement via Telegram")
                        await envoyer_menu(chat_id, f"🚫 <b>{symbole}</b> blacklisté manuellement.")

                    elif chat_id in _attente_saisie:
                        cible = _attente_saisie.pop(chat_id)

                        # --- Saisie d'une clé API : cas sensible, message supprimé après traitement ---
                        if cible.startswith("api_"):
                            exchange = cible[len("api_"):]
                            message_id_a_supprimer = update["message"]["message_id"]
                            try:
                                import api_keys_manager
                                api_key, api_secret, passphrase = api_keys_manager.parser_message_cle(exchange, texte)
                                api_keys_manager.sauvegarder_cle(exchange, api_key, api_secret, passphrase)
                                await supprimer_message(chat_id, message_id_a_supprimer)
                                await envoyer_menu(
                                    chat_id,
                                    f"✅ Clé API <b>{exchange.capitalize()}</b> enregistrée.\n"
                                    f"🗑️ Ton message a été supprimé par sécurité."
                                )
                            except ValueError as e:
                                await supprimer_message(chat_id, message_id_a_supprimer)
                                await envoyer_menu(chat_id, f"❌ {e}\n(message supprimé par sécurité, réessaie)")

                        elif cible == "unblacklist":
                            import health_manager
                            symbole = texte.strip().upper()
                            health_manager.retirer_de_la_blacklist(symbole)
                            await envoyer_menu(chat_id, f"✅ <b>{symbole}</b> retiré de la blacklist.")

                        else:
                            # seuil_inter / seuil_tri
                            try:
                                valeur = float(texte.replace(",", ".").replace("%", "").strip())
                                if not (0 < valeur < 20):
                                    raise ValueError("hors limites")
                                if cible == "seuil_inter":
                                    etat_bot.seuil_inter_exchange = valeur
                                    confirmation = f"✅ Seuil inter-exchange mis à jour : <b>{valeur}%</b>"
                                else:
                                    etat_bot.seuil_triangulaire = valeur
                                    confirmation = f"✅ Seuil triangulaire mis à jour : <b>{valeur}%</b>"
                                await envoyer_menu(chat_id, confirmation)
                            except ValueError:
                                await envoyer_menu(
                                    chat_id,
                                    f"❌ Valeur invalide : « {texte} ». Envoie juste un nombre entre 0 et 20 (ex: 0.3)."
                                )

                elif "callback_query" in update:
                    cq = update["callback_query"]
                    action = cq.get("data", "")
                    chat_id = cq["message"]["chat"]["id"]
                    message_id = cq["message"]["message_id"]

                    await repondre_callback(cq["id"])

                    # --- Sous-menu Config API (clavier différent du menu principal) ---
                    if action == "config_api":
                        await editer_menu_custom(
                            chat_id, message_id,
                            "🔑 <b>CONFIGURATION API</b>\n\n"
                            "✅ = clé configurée, ⬜ = non configurée\n"
                            "Choisis un exchange :",
                            build_menu_config_api(),
                        )
                        continue

                    if action.startswith("config_api_"):
                        exchange = action[len("config_api_"):]
                        import api_keys_manager
                        _attente_saisie[chat_id] = f"api_{exchange}"
                        format_attendu = (
                            "CLE:SECRET:PASSPHRASE" if exchange in api_keys_manager.EXCHANGES_AVEC_PASSPHRASE
                            else "CLE:SECRET"
                        )
                        await editer_menu(
                            chat_id, message_id,
                            f"🔑 <b>{exchange.upper()}</b>\n\n"
                            f"Envoie tes identifiants au format :\n<code>{format_attendu}</code>\n\n"
                            f"⚠️ Utilise une clé <b>sans permission de retrait</b>.\n"
                            f"Ton message sera supprimé automatiquement après traitement."
                        )
                        continue

                    if action == "export_csv_ml":
                        await gerer_export_csv(chat_id, message_id)
                        continue

                    resultat = traiter_action(action, chat_id)
                    if resultat:
                        await editer_menu(chat_id, message_id, resultat)

        except asyncio.TimeoutError:
            continue  # normal avec le long polling, on relance juste
        except Exception as e:
            log.error(f"Erreur polling Telegram : {e}")
            await asyncio.sleep(3)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    print("Lance ce fichier seul pour tester le menu, puis envoie /start à ton bot sur Telegram.")
    asyncio.run(demarrer_bot_telegram())
