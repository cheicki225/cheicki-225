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

load_dotenv()

log = logging.getLogger("telegram")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"


def _session_avec_dns_force() -> aiohttp.ClientSession:
    """
    Force explicitement Google DNS (8.8.8.8/8.8.4.4) — pycares/aiodns échoue
    parfois à lire la config DNS depuis le registre Windows, ce qui cause
    l'erreur "Could not contact DNS servers" même quand le PC résout bien
    l'adresse ailleurs (nslookup, navigateur, etc.)
    """
    from aiohttp.resolver import AsyncResolver
    resolver = AsyncResolver(nameservers=["8.8.8.8", "8.8.4.4"])
    connector = aiohttp.TCPConnector(resolver=resolver)
    return aiohttp.ClientSession(connector=connector)

# Anti-spam : ne renvoie pas la même opportunité avant ce délai (secondes)
COOLDOWN_PAR_OPPORTUNITE_SEC = 60

# Garde en mémoire la dernière fois qu'une opportunité a été notifiée
_dernieres_notifications: dict[str, float] = {}


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
    return "\n".join(lignes)


async def envoyer_alerte(opp, forcer: bool = False) -> bool:
    """
    Envoie une alerte Telegram pour une opportunité, sauf si elle a déjà
    été notifiée récemment (anti-spam) — sauf si forcer=True.
    Retourne True si un message a été envoyé.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("TELEGRAM_BOT_TOKEN ou TELEGRAM_CHAT_ID manquant dans .env — alerte ignorée")
        return False

    cle = _cle_opportunite(opp)
    maintenant = time.time()
    derniere_fois = _dernieres_notifications.get(cle, 0)

    if not forcer and (maintenant - derniere_fois) < COOLDOWN_PAR_OPPORTUNITE_SEC:
        return False  # déjà notifié récemment, on ignore pour éviter le spam

    message = _formater_message(opp)
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
    }

    try:
        async with _session_avec_dns_force() as session:
            async with session.post(TELEGRAM_API_URL, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    _dernieres_notifications[cle] = maintenant
                    log.info(f"Alerte envoyée : {cle}")
                    return True
                else:
                    body = await resp.text()
                    log.error(f"Échec envoi Telegram ({resp.status}) : {body[:200]}")
                    return False
    except Exception as e:
        log.error(f"Erreur envoi Telegram : {e}")
        return False


async def envoyer_message_simple(texte: str) -> bool:
    """Envoie un message texte simple (ex: démarrage du bot, résumé quotidien)."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False

    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": texte, "parse_mode": "HTML"}
    try:
        async with _session_avec_dns_force() as session:
            async with session.post(TELEGRAM_API_URL, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                return resp.status == 200
    except Exception as e:
        log.error(f"Erreur envoi Telegram : {e}")
        return False


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
