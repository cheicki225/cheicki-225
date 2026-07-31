"""
Gestionnaire de clés API — configuration via Telegram
==========================================================
Permet d'ajouter/retirer des clés API d'exchange directement depuis
Telegram, stockées localement dans .env (jamais loggées, jamais renvoyées
en clair une fois enregistrées).

⚠️ SÉCURITÉ :
- Crée toujours des clés API en lecture seule ou trading seul, JAMAIS avec
  la permission de retrait (withdraw) — même si ce fichier ou Telegram
  étaient compromis, personne ne pourrait sortir tes fonds.
- Le message Telegram contenant la clé est supprimé automatiquement après
  traitement (voir telegram_menu_bot.py), mais Telegram garde ses propres
  logs côté serveur — évite de considérer ce canal comme un coffre-fort.
"""

import os
import re

import stockage

ENV_PATH = stockage.chemin_donnees(".env")

# Certains exchanges (OKX, KuCoin) demandent une passphrase en plus de key/secret
EXCHANGES_AVEC_PASSPHRASE = {"okx", "kucoin"}

EXCHANGES_SUPPORTES = ["binance", "bybit", "okx", "kucoin", "bitget", "gateio"]


def _lire_env() -> list[str]:
    if not os.path.exists(ENV_PATH):
        return []
    with open(ENV_PATH, "r", encoding="utf-8") as f:
        return f.readlines()


def _ecrire_env(lignes: list[str]):
    with open(ENV_PATH, "w", encoding="utf-8") as f:
        f.writelines(lignes)


def _upsert_variable(lignes: list[str], nom_variable: str, valeur: str) -> list[str]:
    """Remplace la ligne NOM=valeur si elle existe déjà, sinon l'ajoute à la fin."""
    pattern = re.compile(rf"^{re.escape(nom_variable)}=")
    trouve = False
    nouvelles_lignes = []
    for ligne in lignes:
        if pattern.match(ligne):
            nouvelles_lignes.append(f"{nom_variable}={valeur}\n")
            trouve = True
        else:
            nouvelles_lignes.append(ligne)
    if not trouve:
        if nouvelles_lignes and not nouvelles_lignes[-1].endswith("\n"):
            nouvelles_lignes.append("\n")
        nouvelles_lignes.append(f"{nom_variable}={valeur}\n")
    return nouvelles_lignes


def sauvegarder_cle(exchange: str, api_key: str, api_secret: str, passphrase: str = "") -> None:
    """Enregistre les identifiants d'un exchange dans .env (créé si absent)."""
    exchange = exchange.lower()
    prefixe = exchange.upper()

    lignes = _lire_env()
    lignes = _upsert_variable(lignes, f"{prefixe}_API_KEY", api_key)
    lignes = _upsert_variable(lignes, f"{prefixe}_API_SECRET", api_secret)
    if exchange in EXCHANGES_AVEC_PASSPHRASE and passphrase:
        lignes = _upsert_variable(lignes, f"{prefixe}_PASSPHRASE", passphrase)
    _ecrire_env(lignes)

    # Recharge immédiatement dans l'environnement du process en cours
    os.environ[f"{prefixe}_API_KEY"] = api_key
    os.environ[f"{prefixe}_API_SECRET"] = api_secret
    if passphrase:
        os.environ[f"{prefixe}_PASSPHRASE"] = passphrase


def supprimer_cle(exchange: str) -> None:
    """Retire les identifiants d'un exchange de .env."""
    prefixe = exchange.upper()
    lignes = _lire_env()
    lignes = [
        l for l in lignes
        if not l.startswith(f"{prefixe}_API_KEY=")
        and not l.startswith(f"{prefixe}_API_SECRET=")
        and not l.startswith(f"{prefixe}_PASSPHRASE=")
    ]
    _ecrire_env(lignes)
    os.environ.pop(f"{prefixe}_API_KEY", None)
    os.environ.pop(f"{prefixe}_API_SECRET", None)
    os.environ.pop(f"{prefixe}_PASSPHRASE", None)


def lister_cles_configurees() -> dict:
    """
    Retourne {exchange: True/False} selon si une clé est configurée —
    ne révèle JAMAIS la valeur, juste sa présence.
    """
    resultat = {}
    for exchange in EXCHANGES_SUPPORTES:
        prefixe = exchange.upper()
        a_cle = bool(os.getenv(f"{prefixe}_API_KEY")) and bool(os.getenv(f"{prefixe}_API_SECRET"))
        if exchange in EXCHANGES_AVEC_PASSPHRASE:
            a_cle = a_cle and bool(os.getenv(f"{prefixe}_PASSPHRASE"))
        resultat[exchange] = a_cle
    return resultat


def parser_message_cle(exchange: str, texte: str):
    """
    Parse un message du type "APIKEY:SECRET" ou "APIKEY:SECRET:PASSPHRASE".
    Lève ValueError si le format est incorrect.
    """
    parties = [p.strip() for p in texte.split(":")]
    attendu = 3 if exchange.lower() in EXCHANGES_AVEC_PASSPHRASE else 2

    if len(parties) < attendu:
        if attendu == 3:
            raise ValueError("Format attendu : CLE:SECRET:PASSPHRASE (séparés par ':')")
        raise ValueError("Format attendu : CLE:SECRET (séparés par ':')")

    api_key, api_secret = parties[0], parties[1]
    passphrase = parties[2] if attendu == 3 else ""

    if not api_key or not api_secret or (attendu == 3 and not passphrase):
        raise ValueError("Un des champs est vide.")

    return api_key, api_secret, passphrase
