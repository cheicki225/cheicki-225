"""
BOT ARBITRAGE — Fusion Bloc 2 (WebSockets) + Bloc 3 (Détection d'arbitrage)
===============================================================================
Fait tourner les connexions WebSocket ET le scanner d'arbitrage en parallèle,
sur le même dictionnaire prix_live partagé.

Installation :
    pip install websockets aiohttp aiodns --break-system-packages
"""

import asyncio
import json
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from functools import partial
from itertools import permutations
import websockets
import aiohttp
from telegram_notifier import envoyer_alerte, envoyer_message_simple
import telegram_menu_bot
import opportunity_logger
import symbol_discovery
import health_manager
import paper_trading
import api_server
import suivi_opportunite
import positions_attente
import verif_retraits
import classement_exchanges
import selection_exchanges
import arbitrage_perpetuel
import duree_spread
import nouveaux_listings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)

# ============================================================
# DICTIONNAIRE PARTAGÉ — le cœur de la fusion
# ============================================================
prix_live = {}


# ============================================================
# ================  PARTIE 1 : WEBSOCKETS (Bloc 2)  ==========
# ============================================================
class ExchangeWebSocket(ABC):
    name: str = "base"
    ping_interval: int = 20
    ping_timeout: int = 25  # augmenté de 10 à 25s — plus de marge quand le scanner (300+ paires) sature brièvement la boucle

    def __init__(self, symbols: list[str]):
        self.symbols = symbols
        self.log = logging.getLogger(self.name)
        prix_live.setdefault(self.name, {})

    @abstractmethod
    async def get_url(self) -> str: ...

    async def get_subscribe_message(self) -> dict | None:
        return None

    async def get_subscribe_messages(self) -> list[dict]:
        """
        Messages d'abonnement à envoyer. Par défaut, un seul (celui de
        get_subscribe_message). Les exchanges qui limitent le nombre de
        symboles par message (KuCoin : 100 max) surchargent cette méthode
        pour découper en lots — sans quoi l'abonnement entier est rejeté
        et l'exchange reste à zéro paire active.
        """
        msg = await self.get_subscribe_message()
        return [msg] if msg else []

    @abstractmethod
    def parse_message(self, raw_message: str): ...

    async def handle_ping(self, ws, raw_message: str) -> bool:
        return False

    async def run(self):
        reconnect_delay = 1
        max_delay = 30
        while True:
            try:
                url = await self.get_url()
                self.log.info("Tentative de connexion...")
                async with websockets.connect(
                    url, ping_interval=self.ping_interval, ping_timeout=self.ping_timeout,
                ) as ws:
                    self.log.info("✅ Connecté")
                    reconnect_delay = 1
                    sub_messages = await self.get_subscribe_messages()
                    for sub_msg in sub_messages:
                        await ws.send(json.dumps(sub_msg))
                        self.log.info(f"Abonnement envoyé : {str(sub_msg)[:200]}")
                        if len(sub_messages) > 1:
                            await asyncio.sleep(0.3)  # respecte les limites de débit d'abonnement
                    async for message in ws:
                        if await self.handle_ping(ws, message):
                            continue
                        parsed = self.parse_message(message)
                        if parsed:
                            symbol, bid, ask = parsed
                            ancien = prix_live[self.name].get(symbol)
                            prix_a_change = ancien is None or ancien["bid"] != bid or ancien["ask"] != ask
                            prix_live[self.name][symbol] = {
                                "bid": bid, "ask": ask, "timestamp": time.time(),
                            }
                            # Réaction immédiate uniquement si le prix a VRAIMENT changé
                            # (beaucoup d'exchanges renvoient des messages même sans
                            # changement de prix — pas la peine de tout revérifier)
                            if prix_a_change:
                                asyncio.create_task(verifier_opportunites_symbole(symbol))
                                if symbol in TRIANGLE_LEG_SYMBOLS:
                                    asyncio.create_task(verifier_triangles_exchange(self.name))
            except websockets.exceptions.ConnectionClosed as e:
                self.log.warning(f"Connexion fermée : {e}")
            except Exception as e:
                self.log.error(f"Erreur : {e}")
            self.log.info(f"Reconnexion dans {reconnect_delay}s...")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, max_delay)


class BinanceWS(ExchangeWebSocket):
    name = "binance"

    async def get_url(self) -> str:
        streams = "/".join(f"{s.lower()}@bookTicker" for s in self.symbols)
        return f"wss://stream.binance.com:9443/ws/{streams}"

    def parse_message(self, raw_message: str):
        try:
            data = json.loads(raw_message)
            if "s" in data and "b" in data and "a" in data:
                return data["s"], float(data["b"]), float(data["a"])
        except (json.JSONDecodeError, ValueError):
            pass
        return None


class BybitWS(ExchangeWebSocket):
    name = "bybit"

    async def get_url(self) -> str:
        return "wss://stream.bybit.com/v5/public/spot"

    async def get_subscribe_messages(self) -> list[dict]:
        """Bybit limite à 10 paires MAX par message -> on découpe en lots."""
        topics = [f"orderbook.1.{s}" for s in self.symbols]
        lots = [topics[i:i + 10] for i in range(0, len(topics), 10)]
        return [{"op": "subscribe", "args": lot} for lot in lots]

    async def handle_ping(self, ws, raw_message: str) -> bool:
        try:
            data = json.loads(raw_message)
            if data.get("op") == "pong":
                return True
            if "op" in data or "success" in data:
                if not data.get("success", True):
                    self.log.warning(f"Abonnement refusé : {raw_message[:300]}")
                return True
        except json.JSONDecodeError:
            pass
        return False

    def parse_message(self, raw_message: str):
        try:
            data = json.loads(raw_message)
            if data.get("topic", "").startswith("orderbook.") and "data" in data:
                d = data["data"]
                bids, asks = d.get("b"), d.get("a")
                if bids and asks:
                    return d["s"], float(bids[0][0]), float(asks[0][0])
        except (json.JSONDecodeError, ValueError, KeyError, IndexError):
            pass
        return None

    async def run(self):
        async def ping_loop(ws):
            while True:
                await asyncio.sleep(20)
                try:
                    await ws.send(json.dumps({"op": "ping"}))
                except Exception:
                    return

        reconnect_delay = 1
        max_delay = 30
        while True:
            try:
                url = await self.get_url()
                self.log.info("Tentative de connexion...")
                async with websockets.connect(url) as ws:
                    self.log.info("✅ Connecté")
                    reconnect_delay = 1
                    sub_messages = await self.get_subscribe_messages()
                    for msg in sub_messages:
                        await ws.send(json.dumps(msg))
                        self.log.info(f"Abonnement envoyé ({len(msg['args'])} paires) : {msg['args']}")
                        await asyncio.sleep(0.2)  # évite de spammer trop vite
                    ping_task = asyncio.create_task(ping_loop(ws))
                    try:
                        async for message in ws:
                            if await self.handle_ping(ws, message):
                                continue
                            parsed = self.parse_message(message)
                            if parsed:
                                symbol, bid, ask = parsed
                                ancien = prix_live[self.name].get(symbol)
                                prix_a_change = ancien is None or ancien["bid"] != bid or ancien["ask"] != ask
                                prix_live[self.name][symbol] = {
                                    "bid": bid, "ask": ask, "timestamp": time.time(),
                                }
                                if prix_a_change:
                                    asyncio.create_task(verifier_opportunites_symbole(symbol))
                                    if symbol in TRIANGLE_LEG_SYMBOLS:
                                        asyncio.create_task(verifier_triangles_exchange(self.name))
                    finally:
                        ping_task.cancel()
            except websockets.exceptions.ConnectionClosed as e:
                self.log.warning(f"Connexion fermée : {e}")
            except Exception as e:
                self.log.error(f"Erreur : {e}")
            self.log.info(f"Reconnexion dans {reconnect_delay}s...")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, max_delay)


class OKXWS(ExchangeWebSocket):
    name = "okx"

    async def get_url(self) -> str:
        return "wss://ws.okx.com:8443/ws/v5/public"

    async def get_subscribe_message(self) -> dict:
        return {"op": "subscribe", "args": [{"channel": "tickers", "instId": s} for s in self.symbols]}

    async def handle_ping(self, ws, raw_message: str) -> bool:
        if raw_message == "ping":
            await ws.send("pong")
            return True
        return False

    def parse_message(self, raw_message: str):
        try:
            data = json.loads(raw_message)
            if "data" in data and data.get("arg", {}).get("channel") == "tickers":
                t = data["data"][0]
                bid, ask = t.get("bidPx"), t.get("askPx")
                if bid and ask:
                    symbol_normalise = t["instId"].replace("-", "")  # BTC-USDT -> BTCUSDT
                    return symbol_normalise, float(bid), float(ask)
        except (json.JSONDecodeError, ValueError, KeyError, IndexError):
            pass
        return None


class KuCoinWS(ExchangeWebSocket):
    name = "kucoin"

    async def get_token_and_endpoint(self) -> tuple[str, str]:
        """
        Sur Windows, pycares/aiodns échoue parfois à lire la config DNS
        système, d'où le forçage de Google DNS. Sur Linux (ex: Railway),
        ce forçage peut lui-même échouer selon la version de pycares —
        on retombe alors sur le résolveur par défaut.
        """
        import platform
        session = None
        if platform.system() == "Windows":
            try:
                from aiohttp.resolver import AsyncResolver
                resolver = AsyncResolver(nameservers=["8.8.8.8", "8.8.4.4"])
                connector = aiohttp.TCPConnector(resolver=resolver)
                session = aiohttp.ClientSession(connector=connector)
            except Exception:
                session = None
        if session is None:
            session = aiohttp.ClientSession()

        async with session:
            async with session.post("https://api.kucoin.com/api/v1/bullet-public") as resp:
                data = await resp.json()
                token = data["data"]["token"]
                endpoint = data["data"]["instanceServers"][0]["endpoint"]
                return token, endpoint

    async def get_url(self) -> str:
        token, endpoint = await self.get_token_and_endpoint()
        connect_id = str(int(time.time() * 1000))
        return f"{endpoint}?token={token}&connectId={connect_id}"

    async def get_subscribe_messages(self) -> list[dict]:
        """
        KuCoin limite à 100 symboles par abonnement — au-delà, il rejette le
        message ENTIER en silence (d'où les « 0 paires actives » observées le
        01/08 avec ~144 symboles par connexion). On découpe donc en lots de 90,
        avec une marge de sécurité sous la limite.

        Chaque connexion obtient son propre jeton via bullet-public, donc son
        propre quota de session : le découpage suffit, pas besoin de réduire
        le nombre de paires surveillées.
        """
        TAILLE_LOT = 90
        lots = [self.symbols[i:i + TAILLE_LOT] for i in range(0, len(self.symbols), TAILLE_LOT)]
        return [
            {
                "id": str(int(time.time() * 1000) + i),
                "type": "subscribe",
                "topic": f"/market/ticker:{','.join(lot)}",
                "privateChannel": False,
                "response": True,
            }
            for i, lot in enumerate(lots)
        ]

    async def handle_ping(self, ws, raw_message: str) -> bool:
        try:
            data = json.loads(raw_message)
            type_msg = data.get("type")
            if type_msg in ("welcome", "pong", "ack"):
                return True
            # KuCoin signale les abonnements refusés par un message d'erreur —
            # sans ce log, un rejet passait totalement inaperçu
            if type_msg == "error":
                self.log.warning(f"Abonnement refusé : {raw_message[:300]}")
                return True
        except json.JSONDecodeError:
            pass
        return False

    def parse_message(self, raw_message: str):
        try:
            data = json.loads(raw_message)
            if data.get("type") == "message" and "data" in data:
                d = data["data"]
                bid, ask = d.get("bestBid"), d.get("bestAsk")
                symbol = data.get("topic", "").split(":")[-1].replace("-", "")  # BTC-USDT -> BTCUSDT
                if bid and ask:
                    return symbol, float(bid), float(ask)
        except (json.JSONDecodeError, ValueError, KeyError):
            pass
        return None


class BitgetWS(ExchangeWebSocket):
    name = "bitget"

    async def get_url(self) -> str:
        return "wss://ws.bitget.com/v2/ws/public"

    async def get_subscribe_message(self) -> dict:
        return {"op": "subscribe", "args": [{"instType": "SPOT", "channel": "ticker", "instId": s} for s in self.symbols]}

    async def handle_ping(self, ws, raw_message: str) -> bool:
        return raw_message == "pong"

    def parse_message(self, raw_message: str):
        try:
            data = json.loads(raw_message)
            if "data" in data and data.get("arg", {}).get("channel") == "ticker":
                t = data["data"][0]
                bid, ask = t.get("bidPr"), t.get("askPr")
                if bid and ask:
                    return t.get("instId"), float(bid), float(ask)
        except (json.JSONDecodeError, ValueError, KeyError, IndexError):
            pass
        return None

    async def run(self):
        """Bitget a besoin d'un ping texte périodique en plus de la boucle de base,
        sinon le serveur ferme la connexion après quelques minutes d'inactivité."""
        async def ping_loop(ws):
            while True:
                await asyncio.sleep(25)
                try:
                    await ws.send("ping")
                except Exception:
                    return

        reconnect_delay = 1
        max_delay = 30
        while True:
            try:
                url = await self.get_url()
                self.log.info("Tentative de connexion...")
                async with websockets.connect(url) as ws:
                    self.log.info("✅ Connecté")
                    reconnect_delay = 1
                    sub_msg = await self.get_subscribe_message()
                    await ws.send(json.dumps(sub_msg))
                    self.log.info(f"Abonnement envoyé : {sub_msg}")
                    ping_task = asyncio.create_task(ping_loop(ws))
                    try:
                        async for message in ws:
                            if await self.handle_ping(ws, message):
                                continue
                            parsed = self.parse_message(message)
                            if parsed:
                                symbol, bid, ask = parsed
                                ancien = prix_live[self.name].get(symbol)
                                prix_a_change = ancien is None or ancien["bid"] != bid or ancien["ask"] != ask
                                prix_live[self.name][symbol] = {
                                    "bid": bid, "ask": ask, "timestamp": time.time(),
                                }
                                if prix_a_change:
                                    asyncio.create_task(verifier_opportunites_symbole(symbol))
                                    if symbol in TRIANGLE_LEG_SYMBOLS:
                                        asyncio.create_task(verifier_triangles_exchange(self.name))
                    finally:
                        ping_task.cancel()
            except websockets.exceptions.ConnectionClosed as e:
                self.log.warning(f"Connexion fermée : {e}")
            except Exception as e:
                self.log.error(f"Erreur : {e}")
            self.log.info(f"Reconnexion dans {reconnect_delay}s...")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, max_delay)


class CoinexWS(ExchangeWebSocket):
    """
    CoinEx v2 — ajouté le 04/08.

    POURQUOI CETTE PLATEFORME
    Mesure du 04/08 sur données publiques : coinex arrive PREMIER du
    classement de circulation avec 97.7% de retraits ouverts et 74.3% de
    dépôts — le seul à ne pas s'effondrer côté dépôt. Le trajet
    bitget->coinex laisse passer ~65% des tokens contre ~36% pour le
    meilleur trajet entre les 3 plateformes historiques.

    ⚠️ PARTICULARITÉ : COMPRESSION GZIP
    Contrairement aux 6 autres, CoinEx peut envoyer ses trames compressées
    en gzip (trames binaires). C'est exactement le genre de piège qui avait
    fait abandonner MEXC (Protobuf). Ici c'est gérable : parse_message
    accepte str ET bytes, et décompresse si nécessaire. Si la trame binaire
    n'est pas du gzip, on l'ignore proprement plutôt que de planter.

    ⚠️ NON TESTÉ CONTRE LE VRAI SERVEUR — l'environnement de développement
    n'a pas accès aux plateformes crypto. Surveille les logs au premier
    démarrage : si « coinex » reste à 0 paire active, c'est ici qu'il faut
    regarder.
    """
    name = "coinex"

    # CoinEx limite la taille des messages d'abonnement. On découpe par
    # prudence, comme KuCoin (bug #4 : au-delà de sa limite, KuCoin
    # rejetait SILENCIEUSEMENT le message entier, 0 paire active).
    MAX_SYMBOLES_PAR_MESSAGE = 100

    async def get_url(self) -> str:
        return "wss://socket.coinex.com/v2/spot"

    async def get_subscribe_messages(self) -> list[dict]:
        messages = []
        for i in range(0, len(self.symbols), self.MAX_SYMBOLES_PAR_MESSAGE):
            lot = self.symbols[i:i + self.MAX_SYMBOLES_PAR_MESSAGE]
            messages.append({
                "method": "bbo.subscribe",
                "params": {"market_list": lot},
                "id": i + 1,
            })
        return messages

    @staticmethod
    def _decoder(raw_message):
        """Renvoie le texte JSON, en décompressant si la trame est binaire."""
        if isinstance(raw_message, (bytes, bytearray)):
            import gzip
            try:
                return gzip.decompress(raw_message).decode("utf-8")
            except (OSError, EOFError, UnicodeDecodeError):
                try:
                    return raw_message.decode("utf-8")
                except UnicodeDecodeError:
                    return None
        return raw_message

    async def handle_ping(self, ws, raw_message) -> bool:
        texte = self._decoder(raw_message)
        if not texte:
            return True  # trame illisible : on l'ignore sans la parser
        try:
            data = json.loads(texte)
        except (json.JSONDecodeError, TypeError):
            return False
        if data.get("method") == "server.ping":
            try:
                await ws.send(json.dumps({"method": "server.pong", "params": {}, "id": data.get("id")}))
            except Exception:
                pass
            return True
        return False

    def parse_message(self, raw_message):
        texte = self._decoder(raw_message)
        if not texte:
            return None
        try:
            data = json.loads(texte)
            if data.get("method") != "bbo.update":
                return None
            d = data.get("data") or {}
            marche = d.get("market")
            bid, ask = d.get("best_bid_price"), d.get("best_ask_price")
            if marche and bid and ask:
                return marche, float(bid), float(ask)
        except (json.JSONDecodeError, ValueError, TypeError, AttributeError):
            pass
        return None

    async def run(self):
        """
        CoinEx exige un ping applicatif périodique, comme Bitget et Bybit,
        sinon le serveur ferme la connexion après quelques minutes.
        """
        async def ping_loop(ws):
            identifiant = 10_000
            while True:
                await asyncio.sleep(25)
                identifiant += 1
                try:
                    await ws.send(json.dumps({"method": "server.ping", "params": {}, "id": identifiant}))
                except Exception:
                    return

        reconnect_delay = 1
        max_delay = 30
        while True:
            try:
                url = await self.get_url()
                self.log.info("Tentative de connexion...")
                async with websockets.connect(url) as ws:
                    self.log.info("✅ Connecté")
                    reconnect_delay = 1
                    for sub_msg in await self.get_subscribe_messages():
                        await ws.send(json.dumps(sub_msg))
                        self.log.info(f"Abonnement envoyé : {str(sub_msg)[:200]}")
                        await asyncio.sleep(0.3)
                    ping_task = asyncio.create_task(ping_loop(ws))
                    try:
                        async for message in ws:
                            if await self.handle_ping(ws, message):
                                continue
                            parsed = self.parse_message(message)
                            if parsed:
                                symbol, bid, ask = parsed
                                ancien = prix_live[self.name].get(symbol)
                                prix_a_change = ancien is None or ancien["bid"] != bid or ancien["ask"] != ask
                                prix_live[self.name][symbol] = {
                                    "bid": bid, "ask": ask, "timestamp": time.time(),
                                }
                                if prix_a_change:
                                    asyncio.create_task(verifier_opportunites_symbole(symbol))
                                    if symbol in TRIANGLE_LEG_SYMBOLS:
                                        asyncio.create_task(verifier_triangles_exchange(self.name))
                    finally:
                        ping_task.cancel()
            except websockets.exceptions.ConnectionClosed as e:
                self.log.warning(f"Connexion fermée : {e}")
            except Exception as e:
                self.log.error(f"Erreur : {e}")
            self.log.info(f"Reconnexion dans {reconnect_delay}s...")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, max_delay)


class BitvavoWS(ExchangeWebSocket):
    """
    Bitvavo — ajouté le 07/08. Score de circulation 84,2 (score le plus
    élevé après Kraken parmi les plateformes candidates), site confirmé
    accessible par l'utilisateur.

    ⚠️ PARTICULARITÉ MAJEURE : BITVAVO COTE EN EUROS, PAS EN USDT
    Toutes les paires Bitvavo sont du type BTC-EUR — il n'existe aucune
    paire USDT sur cette plateforme (confirmé par leur centre d'aide).
    Comparer directement un prix EUR à un prix USDT donnerait un résultat
    n'importe quoi (souvent une "opportunité" énorme et fausse).

    SOLUTION : conversion à l'ingestion, via taux_change.py. Chaque prix
    reçu est immédiatement transformé en équivalent USDT AVANT d'être
    écrit dans prix_live — de sorte que tout le reste du bot (détection,
    alertes, mode papier, positions en attente, suivi 10s) traite Bitvavo
    exactement comme les 7 autres plateformes, sans aucun cas particulier.

    ⚠️ RISQUE ASSUMÉ : le taux EUR/USDT utilisé pour convertir est celui
    observé AU MOMENT du tick Bitvavo, pas exactement au même instant que
    le prix de la plateforme USDT en face. C'est une source d'erreur
    supplémentaire, inexistante pour les 7 autres plateformes entre elles.
    Si le taux de change n'est pas disponible ou trop vieux (>15s), le tick
    est IGNORÉ plutôt que publié avec un taux de repli approximatif — mieux
    vaut un prix manquant qu'un prix faussement précis qui déclenche une
    fausse alerte.

    ⚠️ NON TESTÉ CONTRE LE VRAI SERVEUR — mon environnement de développement
    n'a pas accès aux plateformes crypto. Le format des messages est
    documenté et confirmé par plusieurs sources indépendantes (SDK officiels
    Bitvavo en Python/Node/Go/PHP), mais surveille les logs au premier
    démarrage : si "bitvavo" reste à 0 paire active, c'est ici qu'il faut
    regarder en premier.
    """
    name = "bitvavo"

    async def get_url(self) -> str:
        return "wss://ws.bitvavo.com/v2/"

    async def get_subscribe_message(self) -> dict:
        # Format Bitvavo : marchés en EUR ("BTC-EUR"), pas en USDT — la
        # conversion des symboles se fait ici, à l'abonnement.
        marches_eur = [self._vers_marche_bitvavo(s) for s in self.symbols]
        return {
            "action": "subscribe",
            "channels": [{"name": "ticker", "markets": marches_eur}],
        }

    @staticmethod
    def _vers_marche_bitvavo(symbole_usdt: str) -> str:
        """BTCUSDT -> BTC-EUR (Bitvavo n'a pas de paire USDT, on demande l'EUR)"""
        base = symbole_usdt[:-4] if symbole_usdt.endswith("USDT") else symbole_usdt
        return f"{base}-EUR"

    @staticmethod
    def _vers_symbole_bot(marche_bitvavo: str) -> str | None:
        """BTC-EUR -> BTCUSDT, pour réintégrer dans prix_live au format commun"""
        if not marche_bitvavo.endswith("-EUR"):
            return None
        base = marche_bitvavo[:-4]
        return f"{base}USDT"

    def parse_message(self, raw_message: str):
        """
        Ne retourne PAS directement (symbol, bid, ask) comme les autres
        connecteurs — la conversion EUR->USDT a besoin d'accéder à
        prix_live, donc elle est faite dans run() plutôt qu'ici. Cette
        méthode extrait seulement les valeurs brutes en EUR.
        """
        try:
            data = json.loads(raw_message)
            if data.get("event") != "ticker":
                return None
            marche = data.get("market")
            bid_eur = data.get("bestBid")
            ask_eur = data.get("bestAsk")
            if marche and bid_eur and ask_eur:
                return marche, float(bid_eur), float(ask_eur)
        except (json.JSONDecodeError, ValueError, TypeError, AttributeError):
            pass
        return None

    async def run(self):
        """
        Surcharge complète de run() (plutôt que parse_message seul) : la
        conversion EUR->USDT doit se faire APRÈS réception mais AVANT
        écriture dans prix_live, ce que le contrat standard de la classe de
        base ne permet pas facilement.
        """
        import taux_change

        reconnect_delay = 1
        max_delay = 30
        while True:
            try:
                url = await self.get_url()
                self.log.info("Tentative de connexion...")
                async with websockets.connect(url) as ws:
                    self.log.info("✅ Connecté")
                    reconnect_delay = 1
                    sub_msg = await self.get_subscribe_message()
                    await ws.send(json.dumps(sub_msg))
                    self.log.info(f"Abonnement envoyé : {str(sub_msg)[:200]}")

                    async for message in ws:
                        parsed = self.parse_message(message)
                        if not parsed:
                            continue
                        marche_eur, bid_eur, ask_eur = parsed
                        symbol = self._vers_symbole_bot(marche_eur)
                        if not symbol:
                            continue

                        taux, fiable = taux_change.taux_eur_vers_usdt(prix_live)
                        if not fiable:
                            # Pas de taux EUR/USDT frais disponible : on
                            # préfère ne rien publier plutôt que convertir
                            # avec un repli approximatif qui pourrait
                            # déclencher une fausse opportunité.
                            continue

                        bid_usdt = bid_eur * taux
                        ask_usdt = ask_eur * taux

                        ancien = prix_live[self.name].get(symbol)
                        prix_a_change = (
                            ancien is None or ancien["bid"] != bid_usdt or ancien["ask"] != ask_usdt
                        )
                        prix_live[self.name][symbol] = {
                            "bid": bid_usdt, "ask": ask_usdt, "timestamp": time.time(),
                        }
                        if prix_a_change:
                            asyncio.create_task(verifier_opportunites_symbole(symbol))
                            if symbol in TRIANGLE_LEG_SYMBOLS:
                                asyncio.create_task(verifier_triangles_exchange(self.name))
            except websockets.exceptions.ConnectionClosed as e:
                self.log.warning(f"Connexion fermée : {e}")
            except Exception as e:
                self.log.error(f"Erreur : {e}")
            self.log.info(f"Reconnexion dans {reconnect_delay}s...")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, max_delay)


class WhitebitWS(ExchangeWebSocket):
    """
    WhiteBit — ajouté le 07/08. Score de circulation 73,0, site confirmé
    accessible par l'utilisateur (le HTTP 403 rencontré par le script de
    classement était bien une protection anti-bot, pas un géoblocage).
    Cote en USDT — aucune conversion de devise nécessaire, contrairement
    à Bitvavo.

    ⚠️ PARTICULARITÉ : PING À L'INITIATIVE DU CLIENT
    Contrairement à Bitget/Bybit (le serveur envoie un ping, le client
    répond), WhiteBit ferme la connexion après 60s d'inactivité SANS
    jamais envoyer de ping lui-même — c'est au client d'envoyer un ping
    toutes les 50s de sa propre initiative. D'où la surcharge complète de
    run() plutôt qu'un simple handle_ping().

    ⚠️ Un abonnement bookTicker_subscribe par symbole (pas de liste dans
    params, contrairement à lastprice_subscribe) — la base run() gère déjà
    l'envoi séquentiel avec 0.3s de délai entre chaque message via
    get_subscribe_messages(), donc pas de risque de dépasser la limite de
    200 requêtes/minute même avec plusieurs dizaines de symboles.

    ⚠️ NON TESTÉ CONTRE LE VRAI SERVEUR — format confirmé par la
    documentation officielle GitHub (whitebit-exchange/api-docs), mais
    surveille les logs au premier démarrage : si "whitebit" reste à 0 paire
    active, c'est ici qu'il faut regarder.
    """
    name = "whitebit"

    async def get_url(self) -> str:
        return "wss://api.whitebit.com/ws"

    async def get_subscribe_messages(self) -> list[dict]:
        # Format WhiteBit : BTC_USDT (underscore), un marché par requête
        # d'abonnement — contrairement à d'autres canaux WhiteBit qui
        # acceptent une liste, bookTicker_subscribe n'en documente qu'un.
        return [
            {"method": "bookTicker_subscribe", "params": [self._vers_marche(s)], "id": i + 1}
            for i, s in enumerate(self.symbols)
        ]

    @staticmethod
    def _vers_marche(symbole_usdt: str) -> str:
        """BTCUSDT -> BTC_USDT"""
        base = symbole_usdt[:-4] if symbole_usdt.endswith("USDT") else symbole_usdt
        return f"{base}_USDT"

    def parse_message(self, raw_message: str):
        """
        Ne traite ici que le premier ticker du batch — utilisé seulement en
        secours. run() est surchargé pour traiter TOUS les tickers d'un
        message, au cas où WhiteBit en groupe plusieurs dans une seule
        mise à jour (la documentation n'en montre qu'un, par prudence on
        gère quand même le cas général dans run()).
        """
        try:
            data = json.loads(raw_message)
            if data.get("method") != "bookTicker_update":
                return None
            lots = data.get("params") or []
            if not lots or not isinstance(lots[0], list):
                return None
            marche = lots[0][2]
            bid = lots[0][4]
            ask = lots[0][6]
            base = marche.split("_")[0] if marche else None
            if base and bid and ask:
                return f"{base}USDT", float(bid), float(ask)
        except (json.JSONDecodeError, ValueError, TypeError, IndexError, AttributeError):
            pass
        return None

    async def run(self):
        async def ping_loop(ws):
            while True:
                await asyncio.sleep(50)  # WhiteBit ferme après 60s sans message client
                try:
                    await ws.send(json.dumps({"id": 0, "method": "ping", "params": []}))
                except Exception:
                    return

        reconnect_delay = 1
        max_delay = 30
        while True:
            try:
                url = await self.get_url()
                self.log.info("Tentative de connexion...")
                async with websockets.connect(url) as ws:
                    self.log.info("✅ Connecté")
                    reconnect_delay = 1
                    sub_messages = await self.get_subscribe_messages()
                    for sub_msg in sub_messages:
                        await ws.send(json.dumps(sub_msg))
                        await asyncio.sleep(0.3)
                    self.log.info(f"{len(sub_messages)} abonnement(s) bookTicker envoyé(s)")

                    ping_task = asyncio.create_task(ping_loop(ws))
                    try:
                        async for message in ws:
                            try:
                                data = json.loads(message)
                            except json.JSONDecodeError:
                                continue
                            if data.get("method") != "bookTicker_update":
                                continue
                            # Traite CHAQUE ticker du batch, pas seulement le premier
                            for lot in data.get("params") or []:
                                if not isinstance(lot, list) or len(lot) < 8:
                                    continue
                                marche, bid, ask = lot[2], lot[4], lot[6]
                                if not (marche and bid and ask):
                                    continue
                                symbol = f"{marche.split('_')[0]}USDT"
                                try:
                                    bid_f, ask_f = float(bid), float(ask)
                                except (TypeError, ValueError):
                                    continue
                                ancien = prix_live[self.name].get(symbol)
                                prix_a_change = (
                                    ancien is None or ancien["bid"] != bid_f or ancien["ask"] != ask_f
                                )
                                prix_live[self.name][symbol] = {
                                    "bid": bid_f, "ask": ask_f, "timestamp": time.time(),
                                }
                                if prix_a_change:
                                    asyncio.create_task(verifier_opportunites_symbole(symbol))
                                    if symbol in TRIANGLE_LEG_SYMBOLS:
                                        asyncio.create_task(verifier_triangles_exchange(self.name))
                    finally:
                        ping_task.cancel()
            except websockets.exceptions.ConnectionClosed as e:
                self.log.warning(f"Connexion fermée : {e}")
            except Exception as e:
                self.log.error(f"Erreur : {e}")
            self.log.info(f"Reconnexion dans {reconnect_delay}s...")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, max_delay)


class KrakenWS(ExchangeWebSocket):
    """
    Kraken — ajouté le 07/08. Score de circulation 99,8, le plus élevé de
    tout le classement.

    ⚠️ RÉSERVE IMPORTANTE, DÉJÀ SIGNALÉE : Kraken n'expose son état de
    retraits/dépôts qu'à un niveau GLOBAL par actif (statut "enabled" /
    "deposit_only" / "withdrawal_only" / "disabled"), sans aucun détail par
    réseau — contrairement à kucoin/bitget/gateio. Le filtre de retraits
    (verif_retraits.py) peut donc dire "le retrait est ouvert" mais jamais
    confirmer qu'un réseau PRÉCIS est commun avec la plateforme en face.
    En mode strict, ça limite fortement l'utilité pratique de Kraken pour
    l'arbitrage malgré son excellent score — le score mesure la capacité du
    token à circuler en général, pas la capacité du bot à le VÉRIFIER.

    Cote en USDT (en plus de USD) — utilisable directement, sans conversion
    comme pour Bitvavo.

    ⚠️ NON TESTÉ CONTRE LE VRAI SERVEUR — format confirmé par la
    documentation officielle Kraken (docs.kraken.com/api/docs/websocket-v2).
    """
    name = "kraken"

    async def get_url(self) -> str:
        return "wss://ws.kraken.com/v2"

    async def get_subscribe_message(self) -> dict:
        # Format Kraken v2 : "BTC/USDT" (slash), pas "BTCUSDT"
        marches = [self._vers_marche(s) for s in self.symbols]
        return {"method": "subscribe", "params": {"channel": "ticker", "symbol": marches}}

    @staticmethod
    def _vers_marche(symbole_usdt: str) -> str:
        """BTCUSDT -> BTC/USDT"""
        base = symbole_usdt[:-4] if symbole_usdt.endswith("USDT") else symbole_usdt
        return f"{base}/USDT"

    def parse_message(self, raw_message: str):
        try:
            data = json.loads(raw_message)
            if data.get("channel") != "ticker":
                return None
            for item in data.get("data") or []:
                if not isinstance(item, dict):
                    continue
                marche = item.get("symbol", "")
                bid, ask = item.get("bid"), item.get("ask")
                if "/" in marche and bid and ask:
                    base = marche.split("/")[0]
                    return f"{base}USDT", float(bid), float(ask)
        except (json.JSONDecodeError, ValueError, TypeError, AttributeError):
            pass
        return None


class GateioWS(ExchangeWebSocket):
    """
    Gate.io — JSON classique (pas de protobuf comme MEXC), ping/pong géré
    automatiquement par la librairie websockets standard (protocole natif,
    pas besoin de ping applicatif custom). Format de symbole : BTC_USDT
    (underscore, normalisé en BTCUSDT au décodage).
    """
    name = "gateio"

    async def get_url(self) -> str:
        return "wss://api.gateio.ws/ws/v4/"

    async def get_subscribe_message(self) -> dict:
        return {
            "time": int(time.time()),
            "channel": "spot.book_ticker",
            "event": "subscribe",
            "payload": self.symbols,
        }

    def parse_message(self, raw_message: str):
        try:
            data = json.loads(raw_message)
            if data.get("channel") == "spot.book_ticker" and data.get("event") == "update":
                result = data.get("result", {})
                symbol = result.get("s")
                bid, ask = result.get("b"), result.get("a")
                if symbol and bid and ask:
                    return symbol.replace("_", ""), float(bid), float(ask)
        except (json.JSONDecodeError, ValueError, KeyError):
            pass
        return None


# ============================================================
# ================  PARTIE 2 : ARBITRAGE (Bloc 3)  ===========
# ============================================================
from config import (
    SEUIL_MIN_INTER_EXCHANGE_PCT, SEUIL_MIN_TRIANGULAIRE_PCT, SEUIL_MIN_COLLECTE_ML_PCT,
    FRAIS_TRADING_PCT, SEUIL_ECART_ABSURDE_PCT, RESEAU_PREFERE, RESEAU_FALLBACK,
    NB_CONNEXIONS_PAR_EXCHANGE, MIN_EXCHANGES, VOLUME_MIN_USDT,
    SEUIL_CHUTE_PAIRES_ALERTE_PCT, COOLDOWN_ALERTE_CHUTE_SEC,
    MAX_ALERTES_PAR_MINUTE, COOLDOWN_PAR_CRYPTO_SEC,
    FILTRAGE_ML_ACTIF, SEUIL_ML_CONFIANCE_MIN,
    SUIVI_ACTIF, SEUIL_BENEFICE_REEL_ACTIF, SEUIL_BENEFICE_REEL_PCT,
    MONTANT_PAR_TRADE_USDT, FILTRE_RETRAITS_ACTIF, FILTRE_RETRAITS_MODE,
    FILTRE_RETRAITS_PANNEAU_LIVE,
    PERP_ACTIF, LISTINGS_ACTIF, LISTINGS_INTERVALLE_SEC,
)
import filtre_ml
import spreads_live
import prix_24h
import logos_crypto
import frais_retrait
import orderbook_depth


@dataclass
class OpportuniteArbitrage:
    type_arbitrage: str
    description: str
    spread_brut_pct: float
    frais_total_pct: float
    spread_net_pct: float
    exchanges: list = field(default_factory=list)
    symboles: list = field(default_factory=list)
    timestamp: float = field(default_factory=time.time)
    score_ml: float | None = None  # probabilité (0-1) que l'opportunité tienne 5s, voir filtre_ml.py
    liquidite_info: dict | None = None  # vraie profondeur de carnet au moment de l'alerte (inter-exchange uniquement)
    # Prix top-of-book au moment de la DÉTECTION (ceux annoncés dans l'alerte).
    # Servent à comparer avec les prix d'exécution réels calculés plus tard en
    # profondeur de carnet — c'est l'écart entre les deux qui dit si une
    # opportunité tient vraiment ses promesses à l'exécution.
    prix_achat_annonce: float | None = None
    prix_vente_annonce: float | None = None
    # Bénéfice RÉELLEMENT attendu : spread_net_pct MOINS les frais de retrait
    # nécessaires pour rapatrier les fonds et boucler le cycle. C'est ce
    # chiffre qui décide de l'alerte quand SEUIL_BENEFICE_REEL_ACTIF=True.
    #
    # ⚠️ Volontairement SÉPARÉ de spread_net_pct plutôt que de le remplacer :
    # spread_net_pct est la cible du pipeline ML (opportunity_logger le
    # relogge, le modèle est entraîné dessus). En changer la définition
    # rendrait incohérentes toutes les données déjà collectées.
    # Depuis combien de temps l'écart existe sans interruption (secondes).
    # Signal INVERSÉ : un écart ancien est suspect, pas prometteur.
    age_spread_sec: float | None = None
    benefice_reel_pct: float | None = None
    frais_retrait_pct: float | None = None
    reseau_retrait: str | None = None
    frais_retrait_estime: bool = False

    def __str__(self):
        return (
            f"[{self.type_arbitrage.upper()}] {self.description} | "
            f"brut={self.spread_brut_pct:.3f}% frais={self.frais_total_pct:.3f}% "
            f"NET={self.spread_net_pct:.3f}%"
        )


def meilleur_spread_net(symbol: str):
    """
    Meilleur écart NET pour une crypto, SANS aucun seuil — y compris négatif.

    Sert uniquement à l'affichage temps réel du panneau « Cryptos suivies » :
    detecter_arbitrage_inter_exchange() écarte les spreads négatifs et ceux
    sous le seuil de collecte, ce qui laissait la majorité des cryptos à « — ».
    Ici on veut la valeur réelle, même défavorable (ex: -0.01%).

    Les écarts absurdes (> SEUIL_ECART_ABSURDE_PCT) restent exclus : ce sont
    des collisions de ticker, les afficher n'aurait aucun sens.

    Retourne (spread_net_pct, [exchange_achat, exchange_vente]) ou None si
    moins de deux exchanges ont un prix frais pour cette crypto.
    """
    prix_par_exchange = {}
    for exchange, symbols_data in prix_live.items():
        if symbol in symbols_data:
            data = symbols_data[symbol]
            if time.time() - data["timestamp"] < 3:
                prix_par_exchange[exchange] = data

    if len(prix_par_exchange) < 2:
        return None

    meilleur = None
    for ex_achat, ex_vente in permutations(prix_par_exchange.keys(), 2):
        prix_achat = prix_par_exchange[ex_achat]["ask"]
        prix_vente = prix_par_exchange[ex_vente]["bid"]
        if prix_achat <= 0:
            continue

        spread_brut_pct = ((prix_vente - prix_achat) / prix_achat) * 100
        # Valeur ABSOLUE : une collision de ticker produit un écart énorme
        # dans un sens (+9800%) et son miroir dans l'autre (-99%). Sans le
        # abs(), le miroir négatif passerait le filtre et s'afficherait.
        if abs(spread_brut_pct) > SEUIL_ECART_ABSURDE_PCT:
            continue  # collision de ticker, déjà traitée par la détection principale

        frais = FRAIS_TRADING_PCT.get(ex_achat, 0.10) + FRAIS_TRADING_PCT.get(ex_vente, 0.10)
        spread_net_pct = spread_brut_pct - frais

        if meilleur is None or spread_net_pct > meilleur[0]:
            meilleur = (spread_net_pct, [ex_achat, ex_vente])

    return meilleur


def detecter_arbitrage_inter_exchange(symbol: str, seuil_pct: float = SEUIL_MIN_INTER_EXCHANGE_PCT) -> list[OpportuniteArbitrage]:
    opportunites = []
    prix_par_exchange = {}
    for exchange, symbols_data in prix_live.items():
        # Filtre de sélection : une plateforme désactivée continue de
        # recevoir ses prix en arrière-plan (le WebSocket tourne toujours),
        # mais ne participe jamais à la détection — ni comme achat, ni
        # comme vente. Voir selection_exchanges.py pour le détail.
        if not selection_exchanges.est_actif(exchange):
            continue
        if symbol in symbols_data:
            data = symbols_data[symbol]
            if time.time() - data["timestamp"] < 3:
                prix_par_exchange[exchange] = data

    if len(prix_par_exchange) < 2:
        return opportunites

    for ex_achat, ex_vente in permutations(prix_par_exchange.keys(), 2):
        prix_achat = prix_par_exchange[ex_achat]["ask"]
        prix_vente = prix_par_exchange[ex_vente]["bid"]
        if prix_achat <= 0:
            continue

        spread_brut_pct = ((prix_vente - prix_achat) / prix_achat) * 100
        if spread_brut_pct <= 0:
            continue

        # Filtre de bon sens : un écart énorme dès la première détection
        # est presque toujours une collision de ticker, pas une vraie
        # opportunité -> blacklisté immédiatement, sans attendre 20s
        if spread_brut_pct > SEUIL_ECART_ABSURDE_PCT:
            if symbol not in health_manager.symboles_blacklistes():
                health_manager.blacklister_manuellement(
                    symbol,
                    f"Écart absurde détecté immédiatement ({spread_brut_pct:.1f}% brut entre "
                    f"{ex_achat} et {ex_vente}) — probable collision de ticker"
                )
            continue

        frais_total_pct = FRAIS_TRADING_PCT.get(ex_achat, 0.10) + FRAIS_TRADING_PCT.get(ex_vente, 0.10)
        spread_net_pct = spread_brut_pct - frais_total_pct

        if spread_net_pct >= seuil_pct:
            opportunite = OpportuniteArbitrage(
                type_arbitrage="inter_exchange",
                description=f"Acheter {symbol} sur {ex_achat} @ {prix_achat} -> Vendre sur {ex_vente} @ {prix_vente}",
                spread_brut_pct=spread_brut_pct, frais_total_pct=frais_total_pct,
                spread_net_pct=spread_net_pct, exchanges=[ex_achat, ex_vente], symboles=[symbol],
                prix_achat_annonce=prix_achat, prix_vente_annonce=prix_vente,
            )
            _renseigner_benefice_reel(opportunite)
            # Depuis combien de temps cet écart existe-t-il sans interruption ?
            # Un écart qui dure est SUSPECT, pas prometteur : s'il tenait
            # vraiment, quelqu'un l'aurait déjà refermé en l'exploitant.
            opportunite.age_spread_sec = duree_spread.enregistrer(symbol, ex_achat, ex_vente)
            opportunites.append(opportunite)

    return opportunites


def _renseigner_benefice_reel(opp: OpportuniteArbitrage):
    """
    Complète une opportunité inter-exchange avec son bénéfice RÉEL, frais de
    retrait compris (le seul chiffre qui dit si le cycle serait rentable).

    Les frais de retrait sont FIXES en dollars : leur poids en pourcentage
    dépend donc entièrement de MONTANT_PAR_TRADE_USDT. C'est pour ça qu'un
    seuil unique en % ne peut pas être juste pour toutes les paires à la
    fois, et que ce calcul est fait par paire plutôt qu'approximé.

    frais_retrait.frais_transfert() est purement local (données chargées au
    démarrage, rafraîchies toutes les 6h) et mis en cache — appelable sans
    souci dans la boucle de détection.
    """
    try:
        ex_achat, ex_vente = opp.exchanges
        # Sens du transfert : les fonds reviennent de la plateforme de VENTE
        # vers celle d'ACHAT pour pouvoir recommencer un cycle.
        info = frais_retrait.frais_transfert(ex_vente, ex_achat, MONTANT_PAR_TRADE_USDT)
        frais_pct = (info["frais"] / MONTANT_PAR_TRADE_USDT) * 100 if MONTANT_PAR_TRADE_USDT else 0.0

        opp.frais_retrait_pct = round(frais_pct, 4)
        opp.benefice_reel_pct = round(opp.spread_net_pct - frais_pct, 4)
        opp.reseau_retrait = info.get("reseau")
        opp.frais_retrait_estime = bool(info.get("est_estime"))
    except Exception as e:
        # Ne doit JAMAIS faire échouer une détection : sans cette info, on
        # retombe simplement sur l'ancien comportement (seuil sur spread_net_pct)
        logging.getLogger("arbitrage_engine").warning(
            f"⚠️ Calcul du bénéfice réel impossible pour {opp.symboles} : {e}"
        )
        opp.benefice_reel_pct = None


def detecter_arbitrage_triangulaire(exchange: str, triangle: tuple[str, str, str], seuil_pct: float = SEUIL_MIN_TRIANGULAIRE_PCT) -> OpportuniteArbitrage | None:
    """
    Vérifie un triangle de paires sur UN SEUL exchange.
    triangle = (paire_1, paire_2, paire_3), ex: ("BTCUSDT", "ETHBTC", "ETHUSDT")
    Chemin : USDT -> BTC (achat) -> ETH (achat) -> USDT (vente)
    """
    if exchange not in prix_live:
        return None

    data_exchange = prix_live[exchange]
    paire_1, paire_2, paire_3 = triangle

    if not all(p in data_exchange for p in triangle):
        return None

    for p in triangle:
        if time.time() - data_exchange[p]["timestamp"] > 3:
            return None

    try:
        montant = 1.0
        montant = montant / data_exchange[paire_1]["ask"]
        montant = montant / data_exchange[paire_2]["ask"]
        montant_final = montant * data_exchange[paire_3]["bid"]

        spread_brut_pct = (montant_final - 1.0) * 100
        if spread_brut_pct <= 0:
            return None

        frais_total_pct = FRAIS_TRADING_PCT.get(exchange, 0.10) * 3
        spread_net_pct = spread_brut_pct - frais_total_pct

        if spread_net_pct >= seuil_pct:
            return OpportuniteArbitrage(
                type_arbitrage="triangulaire",
                description=f"{exchange} : {' -> '.join(triangle)}",
                spread_brut_pct=spread_brut_pct, frais_total_pct=frais_total_pct,
                spread_net_pct=spread_net_pct, exchanges=[exchange], symboles=list(triangle),
            )
    except (ZeroDivisionError, KeyError):
        return None

    return None


# BUG CORRIGÉ : detecter_arbitrage_inter_exchange / detecter_arbitrage_triangulaire
# utilisent par défaut le seuil d'ALERTE (SEUIL_MIN_INTER_EXCHANGE_PCT / TRIANGULAIRE,
# 0.5%/0.4%), pas le seuil bas de collecte ML (0.05%). Or opportunity_logger.py les
# rappelle SANS préciser de seuil pour revérifier si une opportunité loggée (souvent
# sous le seuil d'alerte, puisqu'elle vient de la collecte ML) est encore là 0.5s/2.5s
# plus tard. Résultat avant fix : la revérification comparait au seuil d'alerte au lieu
# du seuil de collecte -> quasi aucune opportunité ne pouvait jamais être "confirmée",
# peu importe si son spread avait vraiment bougé ou pas (confirmee_2s/5s ~100% à zéro,
# CSV inexploitable pour le ML). Ces deux partials figent le bon seuil (ML) une fois
# pour toutes, pour que la revérification compare des pommes avec des pommes.
_detecter_inter_exchange_ml = partial(detecter_arbitrage_inter_exchange, seuil_pct=SEUIL_MIN_COLLECTE_ML_PCT)
_detecter_triangulaire_ml = partial(detecter_arbitrage_triangulaire, seuil_pct=SEUIL_MIN_COLLECTE_ML_PCT)


# ============================================================
# TRAITEMENT ÉVÉNEMENTIEL — déclenché à CHAQUE prix reçu, pas à intervalle fixe
# ============================================================
# Symboles qui font partie d'un triangle (BTC/ETH/SOL) — inutile de vérifier
# les triangles à chaque tick d'un altcoin qui n'a rien à voir
TRIANGLE_LEG_SYMBOLS = {"BTCUSDT", "ETHUSDT", "SOLUSDT", "ETHBTC", "SOLBTC"}

TRIANGLES_STANDARD = [("BTCUSDT", "ETHBTC", "ETHUSDT"), ("BTCUSDT", "SOLBTC", "SOLUSDT")]


# Mémorise la dernière valeur (prix_achat, prix_vente) loggée pour chaque
# combinaison précise (type + exchanges + symbole) — évite de reloguer une
# opportunité identique quand la vérification est déclenchée par un AUTRE
# exchange que ceux impliqués dans cette combinaison précise
_dernieres_valeurs_opportunites: dict[str, tuple] = {}

# Limiteur de débit : cooldown de COOLDOWN_PAR_CRYPTO_SEC (15s par défaut)
# par crypto individuelle, + quota global de MAX_ALERTES_PAR_MINUTE cryptos
# différentes par minute glissante (protège contre une saturation globale).
#
# Important : cette fonction ne fait AUCUN await, donc elle s'exécute de
# façon atomique même si plusieurs vérifications tournent en parallèle.
_dernier_alerte_par_symbole: dict[str, float] = {}
_fenetre_alertes: list = []  # liste de (timestamp, symbol) pour le quota global


def _peut_alerter(symbol: str) -> bool:
    """True si on peut envoyer une alerte pour ce symbole maintenant."""
    maintenant = time.time()

    # 1. Cooldown par crypto : minimum COOLDOWN_PAR_CRYPTO_SEC entre deux alertes
    dernier = _dernier_alerte_par_symbole.get(symbol, 0)
    if maintenant - dernier < COOLDOWN_PAR_CRYPTO_SEC:
        return False

    # 2. Quota global : max MAX_ALERTES_PAR_MINUTE cryptos différentes par minute
    _fenetre_alertes[:] = [(t, s) for t, s in _fenetre_alertes if maintenant - t < 60]
    symboles_deja_alertes = {s for _, s in _fenetre_alertes}
    if symbol not in symboles_deja_alertes and len(symboles_deja_alertes) >= MAX_ALERTES_PAR_MINUTE:
        return False

    # Réserve immédiatement (avant tout await ailleurs) pour éviter la course
    _dernier_alerte_par_symbole[symbol] = maintenant
    _fenetre_alertes.append((maintenant, symbol))

    _purger_etats_periodiquement(maintenant)
    return True


# ============================================================
# PURGE MÉMOIRE
# ============================================================
# _dernieres_valeurs_opportunites est indexé par (type + exchanges + symbole) :
# avec 300+ paires et toutes les permutations d'exchanges, ça fait des milliers
# de clés qui n'étaient JAMAIS supprimées. Sur un service qui tourne des
# semaines sans redémarrage (Railway), la mémoire ne fait que monter.
# _dernier_alerte_par_symbole a le même souci, en plus lent.
_dernier_purge_etats = 0.0
_INTERVALLE_PURGE_SEC = 600  # toutes les 10 minutes
_AGE_MAX_VALEUR_OPPORTUNITE_SEC = 3600  # une valeur vieille d'1h ne sert plus à dédupliquer


def _purger_etats_periodiquement(maintenant: float):
    """Supprime les entrées trop vieilles pour encore servir. Sans await."""
    global _dernier_purge_etats
    if maintenant - _dernier_purge_etats < _INTERVALLE_PURGE_SEC:
        return
    _dernier_purge_etats = maintenant

    avant_valeurs = len(_dernieres_valeurs_opportunites)
    avant_symboles = len(_dernier_alerte_par_symbole)

    # Cooldown par symbole : une entrée plus vieille que le cooldown
    # n'empêchera plus jamais rien, elle peut partir.
    for sym in [
        s for s, t in _dernier_alerte_par_symbole.items()
        if maintenant - t > COOLDOWN_PAR_CRYPTO_SEC * 10
    ]:
        del _dernier_alerte_par_symbole[sym]

    # Déduplication des valeurs : on ne stocke pas d'horodatage ici (juste la
    # dernière valeur vue), donc pas de purge par âge possible sans changer la
    # structure. On borne simplement la taille : au-delà, on vide entièrement.
    # Conséquence : au pire, chaque opportunité est reloguée UNE fois de plus
    # après une purge — sans aucun impact sur les alertes, qui sont protégées
    # par _peut_alerter et par l'anti-spam de telegram_notifier.
    if len(_dernieres_valeurs_opportunites) > 50_000:
        _dernieres_valeurs_opportunites.clear()

    if avant_valeurs != len(_dernieres_valeurs_opportunites) or avant_symboles != len(_dernier_alerte_par_symbole):
        logging.getLogger("arbitrage_engine").info(
            f"🧹 Purge mémoire : valeurs {avant_valeurs}→{len(_dernieres_valeurs_opportunites)}, "
            f"symboles {avant_symboles}→{len(_dernier_alerte_par_symbole)}"
        )


_symboles_en_cours = set()  # évite l'accumulation de tâches sur un symbole très volatil


async def verifier_opportunites_symbole(symbol: str):
    """
    Appelée immédiatement à chaque fois qu'un nouveau prix arrive pour ce
    symbole (depuis n'importe quel exchange) — remplace l'ancien scan
    périodique par une réaction en temps réel, au moment même où le prix
    change, plutôt que d'attendre le prochain tick d'une boucle.
    """
    log = logging.getLogger("arbitrage_engine")

    if not telegram_menu_bot.etat_bot.en_marche or telegram_menu_bot.etat_bot.en_pause:
        return
    if symbol in health_manager.symboles_blacklistes():
        return

    # Anti-accumulation : si ce symbole est DÉJÀ en train d'être traité
    # (cas des tokens très volatils avec des dizaines de ticks/seconde),
    # on ignore ce nouveau déclenchement plutôt que d'empiler les tâches
    if symbol in _symboles_en_cours:
        return
    _symboles_en_cours.add(symbol)

    try:
        await _traiter_opportunites_symbole(symbol, log)
    finally:
        _symboles_en_cours.discard(symbol)


async def _traiter_opportunites_symbole(symbol: str, log):
    # Un seul calcul au seuil bas (ML) — sert à la fois pour l'alerte réelle
    # (filtrée ensuite) et la collecte ML, au lieu de calculer deux fois
    seuil_inter_actif = telegram_menu_bot.etat_bot.seuil_inter_exchange

    # Diffusion live (WebSocket, panneau "Cryptos suivies") — le VRAI meilleur
    # écart net de cette crypto, même négatif (ex: -0.01%) et même sous le
    # seuil de collecte. Fait AVANT le filtre de seuil ci-dessous : sinon la
    # grande majorité des cryptos, dont l'écart est sous 0.05%, resteraient
    # à « — ». diffuser_spread() n'envoie rien si la valeur n'a pas changé.
    #
    # ⚠️ Filtre de circulation ajouté le 07/08 : n'affiche QUE les cryptos
    # dont le retrait/dépôt entre les deux plateformes est VÉRIFIÉ ouvert —
    # même critère que celui qui décide des alertes (verif_retraits.py),
    # pour que le panneau ne montre jamais une crypto "disponible" que le
    # bot lui-même refuserait ensuite de trader.
    live = meilleur_spread_net(symbol)
    if live is not None:
        spread_live, exchanges_live = live
        circulable = True
        if FILTRE_RETRAITS_ACTIF and FILTRE_RETRAITS_PANNEAU_LIVE:
            ex_achat, ex_vente = exchanges_live
            circulable, _motif = verif_retraits.autorise_trade(
                symbol, ex_achat, ex_vente, FILTRE_RETRAITS_MODE
            )
        if circulable:
            asyncio.create_task(spreads_live.diffuser_spread(
                symbol, spread_live, exchanges_live, seuil_inter_actif
            ))
        else:
            # Retrait actif, pas juste une absence de mise à jour — voir
            # spreads_live.retirer_spread() pour la raison.
            asyncio.create_task(spreads_live.retirer_spread(symbol))

    toutes = detecter_arbitrage_inter_exchange(symbol, seuil_pct=SEUIL_MIN_COLLECTE_ML_PCT)
    if not toutes:
        return

    for opp in toutes:
        cle = f"{opp.type_arbitrage}:{'-'.join(opp.exchanges)}:{'-'.join(opp.symboles)}"

        # Suivi de persistance — TOUJOURS exécuté, même si la valeur affichée
        # n'a pas changé, car c'est justement un prix qui ne bouge jamais
        # (figé) qu'on veut pouvoir détecter et blacklister au bout de 20s
        health_manager.signaler_opportunite_active(cle, opp.symboles[0])

        # Ignore le LOG/ALERTE si rien n'a changé pour cette combinaison
        # précise depuis la dernière fois (même si la vérification a été
        # déclenchée par un exchange tiers non impliqué dans cette paire)
        valeur_actuelle = round(opp.spread_net_pct, 6)
        if _dernieres_valeurs_opportunites.get(cle) == valeur_actuelle:
            continue
        _dernieres_valeurs_opportunites[cle] = valeur_actuelle

        # Alerte réelle uniquement si ça dépasse le vrai seuil ET le quota de débit
        # Condition d'alerte.
        # Mode "bénéfice réel" (par défaut) : on compare le bénéfice
        # RÉELLEMENT attendu — frais de retrait de CETTE paire d'exchanges
        # inclus — à ton objectif. C'est exact pour chaque paire, là où un
        # seuil unique en % ne peut pas l'être (les frais de retrait étant
        # fixes en dollars, leur poids en % varie d'une paire à l'autre).
        # Repli automatique sur l'ancien comportement si le calcul du
        # bénéfice réel a échoué (benefice_reel_pct is None).
        if SEUIL_BENEFICE_REEL_ACTIF and opp.benefice_reel_pct is not None:
            franchit_le_seuil = opp.benefice_reel_pct >= SEUIL_BENEFICE_REEL_PCT
        else:
            franchit_le_seuil = opp.spread_net_pct >= seuil_inter_actif

        if franchit_le_seuil:
            # FILTRE DE RETIRABILITÉ — le token peut-il PHYSIQUEMENT circuler
            # entre ces deux plateformes ? Sans retrait ouvert côté achat et
            # dépôt ouvert côté vente, l'arbitrage ne peut pas se boucler : le
            # bon écart affiché n'est alors qu'un blocage structurel.
            # Placé APRÈS le seuil et AVANT l'alerte, donc la collecte ML en
            # amont continue d'enregistrer ces cas — ils restent instructifs.
            if FILTRE_RETRAITS_ACTIF:
                autorise_retrait, motif_retrait = verif_retraits.autorise_trade(
                    opp.symboles[0], opp.exchanges[0], opp.exchanges[1], FILTRE_RETRAITS_MODE
                )
                if not autorise_retrait:
                    log.info(
                        f"🚫 Écarté (retraits) : {opp.symboles[0]} "
                        f"{opp.exchanges[0]}->{opp.exchanges[1]} — {motif_retrait}"
                    )
                    continue

            # Score ML (probabilité que l'opportunité tienne 5s) — None si le
            # modèle n'est pas encore entraîné/chargé, aucun impact dans ce cas
            opp.score_ml = filtre_ml.score_opportunite(opp)
            telegram_menu_bot.etat_bot.enregistrer_opportunite(opp)

            # Ne bloque QUE si FILTRAGE_ML_ACTIF=True dans config.py (par défaut
            # False — le score est affiché mais ne filtre rien tant que tu ne
            # l'as pas toi-même activé après avoir jugé le modèle fiable)
            bloque_par_ml = (
                FILTRAGE_ML_ACTIF and opp.score_ml is not None and opp.score_ml < SEUIL_ML_CONFIANCE_MIN
            )

            # Le mode nuit ne doit couper QUE les notifications Telegram, pas
            # le traitement lui-même : sa documentation dit explicitement « le
            # scan continue ». Avant, il était inclus dans `autorise` et
            # arrêtait donc AUSSI le trade papier, la vérif de liquidité et la
            # collecte de données — soit toute la mesure, silencieusement.
            mode_nuit = telegram_menu_bot.etat_bot.mode_nuit

            # Un seul appel à _peut_alerter (il réserve le créneau) — réutilisé
            # pour LE LOG, l'alerte Telegram ET le trade papier, pour que tout
            # respecte la même limite d'une fois par crypto par minute
            autorise = _peut_alerter(opp.symboles[0]) and not bloque_par_ml

            if autorise:
                # Vraie profondeur de carnet, pour l'afficher DANS l'alerte —
                # vérification indépendante de celle refaite juste après par
                # simuler_trade() (volontaire, pas un doublon inutile : le
                # carnet peut bouger entre les deux, à quelques ms d'écart)
                try:
                    opp.liquidite_info = await orderbook_depth.estimer_execution_reelle(
                        opp.exchanges[0], opp.exchanges[1], opp.symboles[0], paper_trading.MONTANT_PAR_TRADE_USDT
                    )
                except Exception as e:
                    opp.liquidite_info = None
                    log.warning(f"⚠️ Vérif liquidité pour l'alerte échouée ({opp.symboles[0]}) : {e}")

                score_txt = f" | score ML={opp.score_ml:.0%}" if opp.score_ml is not None else ""
                log.info(f"💰 OPPORTUNITÉ : {opp}{score_txt}")

                # message_id de l'alerte réellement envoyée, ou None.
                # envoyer_alerte() peut ne RIEN envoyer (cooldown 60s sur cette
                # combinaison, ou blocage 429 en cours) : sa valeur de retour
                # était ignorée, d'où des messages de suivi orphelins publiés
                # sans alerte correspondante visible dans le fil.
                message_id_alerte = None
                if not mode_nuit:
                    message_id_alerte = await envoyer_alerte(opp)

                # Mode papier : simule l'exécution en arrière-plan (aucun
                # argent réel, juste pour mesurer objectivement ce que le
                # bot aurait vraiment gagné/perdu, profondeur réelle incluse).
                # Tourne MÊME en mode nuit — seule sa notification est coupée.
                frais_total = FRAIS_TRADING_PCT.get(opp.exchanges[0], 0.10) + FRAIS_TRADING_PCT.get(opp.exchanges[1], 0.10)
                asyncio.create_task(paper_trading.simuler_trade(
                    opp, frais_total, notifier=not mode_nuit
                ))

                # Suivi de persistance : relit le prix chaque seconde pendant
                # SUIVI_DUREE_SEC (depuis le cache WebSocket, sans appel réseau)
                # pour savoir si le spread affiché ici tient réellement dans le
                # temps ou s'effondre avant qu'un transfert ait pu se boucler.
                # Le CSV est rempli dans tous les cas ; le résumé Telegram n'est
                # envoyé que si une alerte est réellement partie (sinon il n'y
                # aurait aucun message auquel le rattacher).
                if SUIVI_ACTIF:
                    asyncio.create_task(suivi_opportunite.suivre_opportunite(
                        opp, prix_live,
                        message_id_alerte=message_id_alerte,
                        notifier=bool(message_id_alerte),
                    ))
            elif bloque_par_ml:
                log.debug(f"(ignoré, score ML {opp.score_ml:.0%} < seuil {SEUIL_ML_CONFIANCE_MIN:.0%}) {opp}")
            else:
                log.debug(f"(ignoré, cooldown 1x/min) {opp}")

        # Collecte ML systématique (même sous le seuil réel), en arrière-plan
        asyncio.create_task(opportunity_logger.logger_avec_suivi(
            opp, _detecter_inter_exchange_ml, _detecter_triangulaire_ml, None
        ))


async def verifier_triangles_exchange(exchange: str):
    """Appelée quand un prix impliqué dans un triangle (BTC/ETH/SOL) change sur un exchange."""
    log = logging.getLogger("arbitrage_engine")

    if not telegram_menu_bot.etat_bot.en_marche or telegram_menu_bot.etat_bot.en_pause:
        return

    seuil_tri_actif = telegram_menu_bot.etat_bot.seuil_triangulaire

    for triangle in TRIANGLES_STANDARD:
        opp = detecter_arbitrage_triangulaire(exchange, triangle, seuil_pct=SEUIL_MIN_COLLECTE_ML_PCT)
        if not opp:
            continue

        cle = f"{opp.type_arbitrage}:{'-'.join(opp.exchanges)}:{'-'.join(opp.symboles)}"

        # Suivi de persistance TOUJOURS exécuté (voir explication dans
        # verifier_opportunites_symbole ci-dessus)
        health_manager.signaler_opportunite_active(cle, opp.symboles[0])

        valeur_actuelle = round(opp.spread_net_pct, 6)
        if _dernieres_valeurs_opportunites.get(cle) == valeur_actuelle:
            continue
        _dernieres_valeurs_opportunites[cle] = valeur_actuelle

        if opp.spread_net_pct >= seuil_tri_actif:
            opp.score_ml = filtre_ml.score_opportunite(opp)
            telegram_menu_bot.etat_bot.enregistrer_opportunite(opp)

            bloque_par_ml = (
                FILTRAGE_ML_ACTIF and opp.score_ml is not None and opp.score_ml < SEUIL_ML_CONFIANCE_MIN
            )

            # Même principe que pour l'inter-exchange : le mode nuit coupe la
            # notification, pas le traitement ni le log (le scan continue).
            mode_nuit = telegram_menu_bot.etat_bot.mode_nuit

            if _peut_alerter(opp.symboles[0]) and not bloque_par_ml:
                score_txt = f" | score ML={opp.score_ml:.0%}" if opp.score_ml is not None else ""
                log.info(f"💰 OPPORTUNITÉ : {opp}{score_txt}")
                if not mode_nuit:
                    await envoyer_alerte(opp)
            elif bloque_par_ml:
                log.debug(f"(ignoré, score ML {opp.score_ml:.0%} < seuil {SEUIL_ML_CONFIANCE_MIN:.0%}) {opp}")
            else:
                log.debug(f"(ignoré, cooldown 1x/min) {opp}")

        asyncio.create_task(opportunity_logger.logger_avec_suivi(
            opp, _detecter_inter_exchange_ml, _detecter_triangulaire_ml, triangle
        ))


async def nettoyage_periodique():
    """Tâche légère en arrière-plan : purge les entrées de suivi expirées, toutes les 5s."""
    while True:
        await asyncio.sleep(5)
        health_manager.nettoyer_opportunites_expirees()


async def monitor_prix():
    log = logging.getLogger("monitor")
    max_paires_observe = 0
    derniere_alerte = 0
    derniers_comptes = {}  # dernier nombre de paires connu par exchange

    while True:
        await asyncio.sleep(20)

        total_actuel = 0
        comptes_actuels = {}
        for exchange, symbols in prix_live.items():
            comptes_actuels[exchange] = len(symbols)
            total_actuel += len(symbols)

        # Garde en mémoire le maximum observé (ligne de référence "normale")
        if total_actuel > max_paires_observe:
            max_paires_observe = total_actuel

        chute_pct = (1 - total_actuel / max_paires_observe) * 100 if max_paires_observe > 0 else 0
        y_a_un_probleme = chute_pct >= SEUIL_CHUTE_PAIRES_ALERTE_PCT
        ca_a_change = comptes_actuels != derniers_comptes

        # N'affiche le résumé QUE si quelque chose a changé (nouvelle connexion,
        # paire perdue...) ou s'il y a un vrai problème — pas toutes les 20s
        # systématiquement si tout est stable
        if ca_a_change or y_a_un_probleme:
            print("\n" + "=" * 60)
            for exchange, nb in comptes_actuels.items():
                status = "✅" if nb else "⏳"
                print(f"{exchange:10s} {status}  {nb} paires actives")
            print("=" * 60)
            derniers_comptes = comptes_actuels

        # Alerte si chute anormale par rapport au max observé cette session
        if y_a_un_probleme:
            maintenant = time.time()
            if maintenant - derniere_alerte > COOLDOWN_ALERTE_CHUTE_SEC:
                derniere_alerte = maintenant
                log.warning(
                    f"🚨 Chute anormale : {total_actuel}/{max_paires_observe} paires actives "
                    f"(-{chute_pct:.0f}%) — possible bug (blacklist qui explose, exchange down...)"
                )
                await envoyer_message_simple(
                    f"🚨 <b>ALERTE : chute anormale des paires actives</b>\n\n"
                    f"Actuellement : {total_actuel} paires\n"
                    f"Maximum observé cette session : {max_paires_observe}\n"
                    f"Baisse : -{chute_pct:.0f}%\n\n"
                    f"Vérifie les logs ou tape /start puis \"Blacklist\" pour voir si "
                    f"trop de paires ont été exclues par erreur."
                )


def repartir_en_connexions(classe_ws, symboles: list[str], nb_connexions: int = 4) -> list:
    """
    Découpe une liste de symboles en N connexions WebSocket séparées pour
    le même exchange. Toutes les instances partagent le même prix_live[exchange]
    (car .name est identique), donc aucune autre modification n'est nécessaire
    ailleurs dans le code — le scanner d'arbitrage voit tout comme avant.
    """
    if not symboles:
        return []
    taille_par_connexion = max(1, (len(symboles) + nb_connexions - 1) // nb_connexions)
    lots = [symboles[i:i + taille_par_connexion] for i in range(0, len(symboles), taille_par_connexion)]
    return [classe_ws(lot) for lot in lots if lot]


# ============================================================
# POINT D'ENTRÉE
# ============================================================
async def main():
    # NB_CONNEXIONS_PAR_EXCHANGE et MIN_EXCHANGES viennent de config.py

    # La blacklist ne doit vivre que pendant que le bot tourne — on repart
    # toujours à zéro au démarrage, sinon les paires blacklistées lors d'une
    # session précédente restent exclues indéfiniment même après redémarrage
    health_manager.vider_blacklist()
    print("🔄 Blacklist réinitialisée pour cette session (repart toujours à zéro au démarrage)")

    def vers_format_tiret(symbol: str) -> str:
        """BTCUSDT -> BTC-USDT, ETHBTC -> ETH-BTC (format attendu par OKX/KuCoin)."""
        if symbol.endswith("USDT"):
            return f"{symbol[:-4]}-USDT"
        if symbol.endswith("BTC") and symbol != "BTCUSDT":
            return f"{symbol[:-3]}-BTC"
        return symbol

    def vers_format_underscore(symbol: str) -> str:
        """BTCUSDT -> BTC_USDT, ETHBTC -> ETH_BTC (format attendu par Gate.io)."""
        if symbol.endswith("USDT"):
            return f"{symbol[:-4]}_USDT"
        if symbol.endswith("BTC") and symbol != "BTCUSDT":
            return f"{symbol[:-3]}_BTC"
        return symbol

    # Découverte dynamique : toutes les paires disponibles sur au moins
    # MIN_EXCHANGES exchanges avec un volume suffisant, en excluant les pannes connues
    try:
        exclues = health_manager.symboles_blacklistes()
        disponibilite = await symbol_discovery.calculer_disponibilite_min(
            min_exchanges=MIN_EXCHANGES, exclure=exclues, volume_min_usdt=VOLUME_MIN_USDT
        )
        print(f"✅ {len(disponibilite)} paires disponibles sur au moins {MIN_EXCHANGES} exchanges "
              f"avec volume >= {VOLUME_MIN_USDT:,.0f}$ ({len(exclues)} exclues pour panne)")
    except Exception as e:
        print(f"⚠️ Découverte automatique échouée ({e}) — fallback sur une liste fixe")
        disponibilite = {
            s: {"binance", "bybit", "okx", "kucoin", "bitget", "gateio"}
            for s in ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT"]
        }

    # BTC/ETH/SOL toujours en tête (nécessaires pour les triangles), + ETHBTC/SOLBTC
    # ⚠️ EURUSDT est forcé même hors intersection naturelle : c'est le taux
    # dont taux_change.py a besoin pour convertir les prix Bitvavo (coté en
    # EUR) vers USDT. Sans cette ligne, si EURUSDT n'apparaît pas parmi les
    # paires découvertes normalement, la conversion retomberait tout le
    # temps sur le taux de repli — imprécis, alors qu'un vrai flux existe.
    for essentiel in ("BTCUSDT", "ETHUSDT", "SOLUSDT", "ETHBTC", "SOLBTC", "EURUSDT"):
        disponibilite.setdefault(essentiel, {"binance", "bybit", "okx", "kucoin", "bitget", "gateio"})

    # Construit, PAR EXCHANGE, la liste réelle de paires qu'il doit recevoir
    # (au lieu de forcer la même liste partout, ce qui génère des abonnements
    # inutiles/erreurs pour des paires absentes sur tel ou tel exchange)
    # ⚠️ Corrigé le 07/08 : "coinex" manquait ici depuis l'ajout du
    # connecteur CoinexWS — provoquait un KeyError('coinex') dès qu'un token
    # disponible sur cette plateforme apparaissait dans `disponibilite`,
    # faisant planter le bot au démarrage.
    symboles_par_exchange: dict[str, list[str]] = {
        "binance": [], "bybit": [], "okx": [], "kucoin": [], "bitget": [], "gateio": [],
        "coinex": [], "bitvavo": [], "whitebit": [], "kraken": [],
    }
    for symbole, exchanges in disponibilite.items():
        for ex in exchanges:
            # .setdefault plutôt qu'un accès direct : si jamais une future
            # plateforme est ajoutée à symbol_discovery sans qu'on pense à
            # l'ajouter ICI, on ne perd que ses paires (liste vide, log
            # silencieux) plutôt que de faire planter tout le bot.
            symboles_par_exchange.setdefault(ex, []).append(symbole)

    total_uniques = len(disponibilite)
    print(f"Répartition : " + ", ".join(f"{ex}={len(s)}" for ex, s in symboles_par_exchange.items()))

    connexions = (
        repartir_en_connexions(BinanceWS, symboles_par_exchange["binance"], NB_CONNEXIONS_PAR_EXCHANGE)
        + repartir_en_connexions(BybitWS, symboles_par_exchange["bybit"], NB_CONNEXIONS_PAR_EXCHANGE)
        + repartir_en_connexions(OKXWS, [vers_format_tiret(s) for s in symboles_par_exchange["okx"]], NB_CONNEXIONS_PAR_EXCHANGE)
        + repartir_en_connexions(KuCoinWS, [vers_format_tiret(s) for s in symboles_par_exchange["kucoin"]], NB_CONNEXIONS_PAR_EXCHANGE)
        + repartir_en_connexions(BitgetWS, symboles_par_exchange["bitget"], NB_CONNEXIONS_PAR_EXCHANGE)
        + repartir_en_connexions(GateioWS, [vers_format_underscore(s) for s in symboles_par_exchange["gateio"]], NB_CONNEXIONS_PAR_EXCHANGE)
        # CoinEx utilise le format sans séparateur (BTCUSDT), comme Binance.
        # .get() plutôt qu'un accès direct : si la découverte coinex échoue,
        # le bot démarre quand même avec les 6 autres plateformes.
        + repartir_en_connexions(CoinexWS, symboles_par_exchange.get("coinex", []), NB_CONNEXIONS_PAR_EXCHANGE)
        # Bitvavo : symboles au format BTCUSDT ici (comme le reste), mais
        # BitvavoWS._vers_marche_bitvavo() les reconvertit en BTC-EUR au
        # moment de l'abonnement — c'est le seul endroit dans tout le bot
        # où le format EUR de Bitvavo doit être connu.
        + repartir_en_connexions(BitvavoWS, symboles_par_exchange.get("bitvavo", []), NB_CONNEXIONS_PAR_EXCHANGE)
        # WhiteBit : cote en USDT (contrairement à Bitvavo), format BTCUSDT
        # ici, converti en BTC_USDT au moment de l'abonnement dans la classe.
        + repartir_en_connexions(WhitebitWS, symboles_par_exchange.get("whitebit", []), NB_CONNEXIONS_PAR_EXCHANGE)
        # Kraken : score le plus élevé (99.8) mais réserve connue — pas de
        # détail par réseau pour le filtre de retraits (voir KrakenWS).
        + repartir_en_connexions(KrakenWS, symboles_par_exchange.get("kraken", []), NB_CONNEXIONS_PAR_EXCHANGE)
    )

    triangles_par_exchange = {
        ex: [("BTCUSDT", "ETHBTC", "ETHUSDT"), ("BTCUSDT", "SOLBTC", "SOLUSDT")]
        for ex in ("binance", "bybit", "bitget", "okx", "kucoin", "gateio")
    }

    symbols_a_surveiller = list(disponibilite.keys())

    # Connecte le dictionnaire de prix partagé au menu Telegram (pour "État bot", "Top paires", etc.)
    telegram_menu_bot.prix_live_ref = prix_live

    await envoyer_message_simple(
        f"🚀 Bot d'arbitrage démarré (mode temps réel event-driven)\n"
        f"{total_uniques} paires uniques (min. {MIN_EXCHANGES} exchanges) surveillées\n"
        f"Envoie /start pour afficher le menu de contrôle."
    )

    tasks = [asyncio.create_task(c.run()) for c in connexions]
    tasks.append(asyncio.create_task(monitor_prix()))
    tasks.append(asyncio.create_task(nettoyage_periodique()))
    tasks.append(asyncio.create_task(telegram_menu_bot.demarrer_bot_telegram()))
    tasks.append(asyncio.create_task(health_manager.surveiller_sante(prix_live)))
    tasks.append(asyncio.create_task(api_server.demarrer_serveur_web()))
    tasks.append(asyncio.create_task(prix_24h.boucle_rafraichissement()))
    tasks.append(asyncio.create_task(logos_crypto.boucle_rafraichissement()))
    tasks.append(asyncio.create_task(frais_retrait.boucle_rafraichissement()))
    tasks.append(asyncio.create_task(verif_retraits.boucle_rafraichissement()))
    # Classement des plateformes candidates (mesure, ne branche rien
    # automatiquement sur les WebSockets) — consultable via /classement
    # sur Telegram ou l'endpoint /api/classement
    tasks.append(asyncio.create_task(classement_exchanges.boucle_rafraichissement()))
    if LISTINGS_ACTIF:
        # Nouvelles cotations : les écarts y sont larges et parfois réels,
        # contrairement à ceux qui traînent depuis des heures
        tasks.append(asyncio.create_task(
            nouveaux_listings.boucle(LISTINGS_INTERVALLE_SEC)))
    if PERP_ACTIF:
        # Arbitrage spot-futures : les deux jambes sur la MÊME plateforme,
        # donc aucun transfert — contourne le problème des retraits fermés
        tasks.append(asyncio.create_task(arbitrage_perpetuel.boucle(prix_live)))

    # Positions en attente : la surveillance a besoin du cache de prix
    # WebSocket pour savoir quand une position repasse positive. On le lui
    # transmet explicitement plutôt que par import croisé.
    positions_attente.definir_source_prix(prix_live)
    positions_attente.charger_positions()
    selection_exchanges.charger()
    tasks.append(asyncio.create_task(positions_attente.boucle_surveillance()))

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[ARRÊT] Bot arrêté manuellement (Ctrl+C)")
