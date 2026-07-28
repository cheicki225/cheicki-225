"""
Serveur web API — expose les données du bot pour l'app mobile
====================================================================
Petit serveur HTTP intégré au bot, qui tourne EN PARALLÈLE des connexions
WebSocket (même processus, même boucle asyncio). Sert :
- Une API JSON pour les données en temps réel
- La page web mobile elle-même (fichiers statiques)

⚠️ SÉCURITÉ : protégé par un token secret (API_SECRET dans .env). Sans
ce token dans l'en-tête Authorization, aucune donnée n'est accessible —
important car Railway rend cette URL accessible publiquement sur internet.
"""

import json
import logging
import os
from pathlib import Path
from aiohttp import web

log = logging.getLogger("api_server")

API_SECRET = os.getenv("API_SECRET", "")
DOSSIER_STATIC = Path(__file__).parent / "webapp"


def _verifier_auth(request) -> bool:
    if not API_SECRET:
        return True  # pas de secret configuré = pas de protection (déconseillé en prod)
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    return token == API_SECRET


def _reponse_json(data, statut=200):
    return web.Response(
        text=json.dumps(data, default=str),
        status=statut,
        content_type="application/json",
        headers={"Access-Control-Allow-Origin": "*"},
    )


async def handler_etat(request):
    if not _verifier_auth(request):
        return _reponse_json({"erreur": "non autorisé"}, 401)

    import telegram_menu_bot
    etat = telegram_menu_bot.etat_bot
    prix_live_ref = telegram_menu_bot.prix_live_ref

    nb_exchanges = len([e for e, s in prix_live_ref.items() if s])
    nb_prix = sum(len(s) for s in prix_live_ref.values())

    return _reponse_json({
        "en_marche": etat.en_marche,
        "en_pause": etat.en_pause,
        "mode_nuit": etat.mode_nuit,
        "uptime": etat.uptime_str(),
        "nb_exchanges_connectes": nb_exchanges,
        "nb_prix_en_cache": nb_prix,
        "nb_opportunites_session": len(etat.opportunites_trouvees),
        "seuil_inter_exchange": etat.seuil_inter_exchange,
        "seuil_triangulaire": etat.seuil_triangulaire,
        "exchanges": {ex: len(symbols) for ex, symbols in prix_live_ref.items()},
    })


async def handler_opportunites(request):
    if not _verifier_auth(request):
        return _reponse_json({"erreur": "non autorisé"}, 401)

    import telegram_menu_bot
    dernieres = telegram_menu_bot.etat_bot.opportunites_trouvees[-30:]
    resultat = []
    for item in reversed(dernieres):
        opp = item["opp"]
        resultat.append({
            "timestamp": item["timestamp"],
            "type": opp.type_arbitrage,
            "symbole": opp.symboles[0],
            "exchanges": opp.exchanges,
            "spread_net_pct": round(opp.spread_net_pct, 3),
        })
    return _reponse_json(resultat)


async def handler_papier(request):
    if not _verifier_auth(request):
        return _reponse_json({"erreur": "non autorisé"}, 401)

    import paper_trading
    e = paper_trading._etat_papier
    return _reponse_json({
        "capital_initial": e["capital_initial"],
        "profit_cumule": round(e["profit_cumule_usdt"], 3),
        "nb_trades_total": e["nb_trades_total"],
        "nb_trades_reussis": e["nb_trades_reussis"],
        "nb_trades_rejetes_liquidite": e["nb_trades_rejetes_liquidite"],
        "nb_cryptos_eliminees": e["nb_cryptos_eliminees"],
        "circuit_breaker_actif": paper_trading.circuit_breaker_actif(),
        "stop_loss_actif": paper_trading.stop_loss_journalier_actif(),
        "soldes": paper_trading.obtenir_soldes(),
    })


async def handler_top_performers(request):
    if not _verifier_auth(request):
        return _reponse_json({"erreur": "non autorisé"}, 401)

    import paper_trading
    classement = []
    for symbole, stats in paper_trading._stats_par_crypto.items():
        if stats["total"] >= 2:
            taux = stats["reussis"] / stats["total"] * 100
            classement.append({"symbole": symbole, "taux": round(taux, 1), **stats})
    classement.sort(key=lambda x: x["taux"], reverse=True)
    return _reponse_json(classement[:20])


async def handler_controle(request):
    """POST /api/controle avec {"action": "demarrer"|"arreter"|"pause"|"reprendre"}"""
    if not _verifier_auth(request):
        return _reponse_json({"erreur": "non autorisé"}, 401)

    import telegram_menu_bot
    try:
        data = await request.json()
    except Exception:
        return _reponse_json({"erreur": "JSON invalide"}, 400)

    action = data.get("action")
    etat = telegram_menu_bot.etat_bot

    if action == "demarrer":
        etat.en_marche = True
        etat.en_pause = False
    elif action == "arreter":
        etat.en_marche = False
    elif action == "pause":
        etat.en_pause = True
    elif action == "reprendre":
        etat.en_pause = False
    elif action == "mode_nuit_toggle":
        etat.mode_nuit = not etat.mode_nuit
    else:
        return _reponse_json({"erreur": "action inconnue"}, 400)

    return _reponse_json({"ok": True, "action": action})


async def handler_index(request):
    fichier = DOSSIER_STATIC / "index.html"
    if fichier.exists():
        return web.FileResponse(fichier)
    return web.Response(text="App web pas encore déployée", status=404)


async def demarrer_serveur_web(port: int = None):
    port = port or int(os.getenv("PORT", 8080))

    app = web.Application()
    app.router.add_get("/", handler_index)
    app.router.add_get("/api/etat", handler_etat)
    app.router.add_get("/api/opportunites", handler_opportunites)
    app.router.add_get("/api/papier", handler_papier)
    app.router.add_get("/api/top_performers", handler_top_performers)
    app.router.add_post("/api/controle", handler_controle)

    if DOSSIER_STATIC.exists():
        app.router.add_static("/static/", DOSSIER_STATIC, name="static")

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log.info(f"✅ Serveur web démarré sur le port {port}")

    if not API_SECRET:
        log.warning("⚠️ API_SECRET non défini — l'API est accessible SANS protection !")
