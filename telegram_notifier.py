"""
Module d'alertes Telegram
=============================
Envoie une notification Telegram dès qu'une opportunité d'arbitrage
dépasse le seuil configuré, avec un système anti-spam pour ne pas
envoyer 50 fois la même alerte en quelques secondes.

Configuration requise dans .env :
    TELEGRAM_BOT_TOKEN=xxx
    TELEGRAM_CHAT_ID=xxx

Installation :
    pip install aiohttp python-dotenv --break-system-packages
"""

import asyncio
import logging
import time
import os
import aiohttp
from dotenv import load_dotenv

import stockage

load_dotenv(stockage.chemin_donnees(".env"))

log = logging.getLogger("telegram")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"


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

# Anti-spam : ne renvoie pas la même opportunité avant ce délai (secondes)
COOLDOWN_PAR_OPPORTUNITE_SEC = 60

# ============================================================
# LIMITEUR DE DÉBIT TELEGRAM
# ============================================================
# Telegram limite les envois vers un même chat à environ 20 messages par
# minute. Au-delà, il répond 429 avec un "retry_after" qui peut atteindre
# plusieurs HEURES de blocage total — et continuer à envoyer pendant ce
# temps ne fait qu'entretenir le problème.
#
# Tous les envois passent donc par _envoyer_payload(), qui :
#   1. espace les messages d'au moins _INTERVALLE_MIN_ENVOI_SEC
#   2. respecte scrupuleusement le retry_after renvoyé par Telegram
#   3. ABANDONNE les messages pendant un blocage plutôt que de les empiler
#      (une alerte d'arbitrage vieille de 2h n'a aucun intérêt, et les
#      accumuler relancerait le flood dès la fin du blocage)
_INTERVALLE_MIN_ENVOI_SEC = 3.0  # 3s -> ~20 messages/minute maximum

_verrou_envoi = asyncio.Lock()
_dernier_envoi = 0.0
_bloque_jusqua = 0.0
_nb_abandonnes = 0
_dernier_log_blocage = 0.0


def etat_limiteur() -> dict:
    """Diagnostic : blocage en cours et nombre de messages abandonnés."""
    restant = max(0.0, _bloque_jusqua - time.time())
    return {
        "bloque": restant > 0,
        "secondes_restantes": round(restant),
        "messages_abandonnes": _nb_abandonnes,
    }


async def _envoyer_payload(payload: dict) -> int | None:
    """
    Point de passage UNIQUE pour tout envoi Telegram.

    Retourne le message_id attribué par Telegram si le message est parti,
    None sinon (limiteur actif, erreur réseau, 429...).

    ⚠️ Retournait auparavant un booléen. Le message_id est nécessaire pour
    rattacher un message de suivi à l'alerte qu'il analyse
    (reply_to_message_id). Comme un message_id est toujours un entier > 0,
    tout code existant qui fait `if envoye:` continue de fonctionner
    exactement pareil — None et 0 sont faux, un id est vrai.
    """
    global _dernier_envoi, _bloque_jusqua, _nb_abandonnes, _dernier_log_blocage

    maintenant = time.time()
    if maintenant < _bloque_jusqua:
        _nb_abandonnes += 1
        # Un seul log par minute pendant le blocage, sinon on remplit les
        # journaux avec des milliers de lignes identiques (ce qui s'est passé)
        if maintenant - _dernier_log_blocage > 60:
            _dernier_log_blocage = maintenant
            restant = int(_bloque_jusqua - maintenant)
            log.warning(
                f"⏳ Telegram bloqué encore {restant}s ({restant // 60} min) — "
                f"{_nb_abandonnes} message(s) abandonné(s) depuis le début du blocage"
            )
        return None

    async with _verrou_envoi:
        # Espacement minimum entre deux envois
        attente = _INTERVALLE_MIN_ENVOI_SEC - (time.time() - _dernier_envoi)
        if attente > 0:
            await asyncio.sleep(attente)

        try:
            async with _session_avec_dns_force() as session:
                async with session.post(
                    TELEGRAM_API_URL, json=payload, timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    _dernier_envoi = time.time()

                    if resp.status == 200:
                        try:
                            donnees = await resp.json()
                            return donnees.get("result", {}).get("message_id")
                        except Exception:
                            # Message bien parti mais réponse illisible : on ne
                            # peut pas fournir d'id, sans que ce soit un échec.
                            # -1 reste "vrai" pour `if envoye:`, tout en étant
                            # inutilisable comme reply_to (Telegram le refuserait).
                            return -1

                    if resp.status == 429:
                        try:
                            donnees = await resp.json()
                            retry = float(donnees.get("parameters", {}).get("retry_after", 60))
                        except Exception:
                            retry = 60.0
                        _bloque_jusqua = time.time() + retry
                        _nb_abandonnes = 0
                        _dernier_log_blocage = time.time()
                        log.error(
                            f"🚫 Telegram : limite de débit atteinte — envois suspendus "
                            f"{int(retry)}s ({int(retry) // 60} min). Les messages de cette "
                            f"période seront abandonnés, pas mis en file."
                        )
                        return None

                    body = await resp.text()
                    log.error(f"Échec envoi Telegram ({resp.status}) : {body[:200]}")
                    return None
        except Exception as e:
            _dernier_envoi = time.time()
            log.error(f"Erreur envoi Telegram : {e}")
            return None


# Garde en mémoire la dernière fois qu'une opportunité a été notifiée
_dernieres_notifications: dict[str, float] = {}

# Purge périodique : une clé est du type "inter_exchange:gateio-binance:XYZUSDT".
# Avec 300+ paires x toutes les permutations d'exchanges, ce dictionnaire
# grossissait indéfiniment sur un service qui tourne des semaines sans
# redémarrage (Railway). Les entrées plus vieilles que le cooldown ne
# servent plus jamais à rien : elles sont supprimées.
_dernier_purge_notifications = 0.0
_INTERVALLE_PURGE_SEC = 300  # toutes les 5 minutes, coût négligeable


def _purger_notifications(maintenant: float):
    global _dernier_purge_notifications
    if maintenant - _dernier_purge_notifications < _INTERVALLE_PURGE_SEC:
        return
    _dernier_purge_notifications = maintenant

    perimees = [
        cle for cle, horodatage in _dernieres_notifications.items()
        if maintenant - horodatage > COOLDOWN_PAR_OPPORTUNITE_SEC
    ]
    for cle in perimees:
        del _dernieres_notifications[cle]
    if perimees:
        log.debug(
            f"Purge anti-spam : {len(perimees)} entrée(s) périmée(s) retirée(s), "
            f"{len(_dernieres_notifications)} conservée(s)"
        )


def _cle_opportunite(opp) -> str:
    """Génère une clé unique pour une opportunité (pour l'anti-spam)."""
    return f"{opp.type_arbitrage}:{'-'.join(opp.exchanges)}:{'-'.join(opp.symboles)}"


def _formater_message(opp) -> str:
    emoji = "🔺" if opp.type_arbitrage == "triangulaire" else "🔄"
    lignes = [
        f"{emoji} <b>Opportunité {opp.type_arbitrage}</b>",
        f"<code>{opp.description}</code>",
        f"Spread brut : {opp.spread_brut_pct:.3f}%",
        f"Frais totaux : {opp.frais_total_pct:.3f}%",
        f"<b>Spread NET : {opp.spread_net_pct:.3f}%</b>",
    ]
    score = getattr(opp, "score_ml", None)
    if score is not None:
        emoji_score = "🟢" if score >= 0.5 else "🟡" if score >= 0.2 else "🔴"
        lignes.append(f"{emoji_score} Confiance ML : {score:.0%} (probabilité que ça tienne 5s)")

    liquidite = getattr(opp, "liquidite_info", None)
    if liquidite and liquidite.get("montant_demande"):
        montant_exec = liquidite.get("montant_executable", 0) or 0
        montant_vise = liquidite["montant_demande"]
        pct_dispo = min(100, montant_exec / montant_vise * 100) if montant_vise else 0
        emoji_liq = "🟢" if pct_dispo >= 95 else "🟡" if pct_dispo >= 20 else "🔴"
        lignes.append(f"{emoji_liq} Liquidité : {montant_exec:.2f}$ / {montant_vise:.0f}$ visés ({pct_dispo:.0f}%)")

    return "\n".join(lignes)


async def envoyer_alerte(opp, forcer: bool = False) -> int | None:
    """
    Envoie une alerte Telegram pour une opportunité, sauf si elle a déjà
    été notifiée récemment (anti-spam) — sauf si forcer=True.

    Retourne le message_id Telegram si un message a été envoyé, None sinon.
    (Un id est toujours "vrai" et None est "faux", donc `if envoyer_alerte(...)`
    se comporte comme avant avec l'ancien booléen.)

    Ce message_id sert à rattacher le résumé du suivi 10s à l'alerte qu'il
    analyse, et surtout à savoir si l'alerte est VRAIMENT partie : sans ça,
    un suivi pouvait être publié alors que l'alerte correspondante avait été
    silencieusement abandonnée (cooldown ou blocage 429) — d'où des messages
    de suivi orphelins, sans opportunité visible à laquelle les rattacher.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("TELEGRAM_BOT_TOKEN ou TELEGRAM_CHAT_ID manquant dans .env — alerte ignorée")
        return None

    cle = _cle_opportunite(opp)
    maintenant = time.time()
    derniere_fois = _dernieres_notifications.get(cle, 0)

    if not forcer and (maintenant - derniere_fois) < COOLDOWN_PAR_OPPORTUNITE_SEC:
        return None  # déjà notifié récemment, on ignore pour éviter le spam

    message = _formater_message(opp)
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
    }

    message_id = await _envoyer_payload(payload)
    if message_id:
        _dernieres_notifications[cle] = maintenant
        _purger_notifications(maintenant)
        log.info(f"Alerte envoyée : {cle}")
    return message_id


async def envoyer_message_simple(texte: str, repondre_a: int | None = None) -> int | None:
    """
    Envoie un message texte simple (ex: démarrage du bot, résumé quotidien).

    repondre_a : message_id auquel rattacher ce message (fil de réponse
    Telegram). Utilisé par le suivi 10s pour s'afficher directement sous
    l'alerte qu'il analyse, au lieu d'un message isolé qu'on ne sait plus
    relier à rien.

    Retourne le message_id envoyé, ou None.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return None

    payload = {
        "chat_id": TELEGRAM_CHAT_ID, "text": texte, "parse_mode": "HTML",
    }
    # allow_sending_without_reply : si le message d'origine a été supprimé
    # entre-temps, Telegram envoie quand même le message au lieu de tout
    # refuser avec une erreur "message to reply not found".
    if repondre_a and repondre_a > 0:
        payload["reply_to_message_id"] = repondre_a
        payload["allow_sending_without_reply"] = True

    return await _envoyer_payload(payload)


# ============================================================
# TEST AUTONOME
# ============================================================
async def _test():
    """Lance ce fichier seul pour tester ta config Telegram :
    python3 telegram_notifier.py
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("❌ TELEGRAM_BOT_TOKEN ou TELEGRAM_CHAT_ID absent de ton .env")
        print("   Ajoute-les puis relance ce test.")
        return

    ok = await envoyer_message_simple("✅ Test de connexion — ton bot Telegram fonctionne !")
    if ok:
        print("✅ Message envoyé avec succès — vérifie Telegram.")
    else:
        print("❌ Échec de l'envoi — vérifie ton token et ton chat_id.")


if __name__ == "__main__":
    asyncio.run(_test())
