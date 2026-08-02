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


def _verifier_auth_ws(request) -> bool:
    """Les navigateurs ne peuvent pas mettre de header Authorization custom sur le
    handshake WebSocket — le token passe donc par la query string à la place."""
    if not API_SECRET:
        return True
    return request.query.get("token", "") == API_SECRET


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
    import health_manager
    import paper_trading
    etat = telegram_menu_bot.etat_bot
    prix_live_ref = telegram_menu_bot.prix_live_ref

    nb_exchanges = len([e for e, s in prix_live_ref.items() if s])
    nb_prix = sum(len(s) for s in prix_live_ref.values())

    return _reponse_json({
        "en_marche": etat.en_marche,
        "en_pause": etat.en_pause,
        "mode_nuit": etat.mode_nuit,
        "blacklist_active": health_manager.blacklist_active(),
        "elimination_active": paper_trading.elimination_active(),
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
    dispo = _exchanges_par_symbole()
    resultat = []
    for item in reversed(dernieres):
        opp = item["opp"]
        resultat.append({
            "timestamp": item["timestamp"],
            "type": opp.type_arbitrage,
            "symbole": opp.symboles[0],
            # "exchanges" = le TRAJET du trade (achat -> vente)
            "exchanges": opp.exchanges,
            # "exchanges_dispo" = TOUS les exchanges où la crypto est suivie
            # (deux notions différentes, à ne pas confondre à l'affichage)
            "exchanges_dispo": dispo.get(opp.symboles[0], []),
            "spread_net_pct": round(opp.spread_net_pct, 3),
        })
    return _reponse_json(resultat)


async def handler_papier(request):
    if not _verifier_auth(request):
        return _reponse_json({"erreur": "non autorisé"}, 401)

    import paper_trading
    e = paper_trading._etat_papier
    # Trades RÉELLEMENT exécutés = tentés moins tous les rejets. Un rejet
    # (liquidité nulle ou stock manquant) n'a jamais rien tenté : le compter
    # au dénominateur du taux de réussite le fait baisser mécaniquement,
    # comme s'il s'agissait d'un trade perdant.
    nb_executes = (
        e["nb_trades_total"]
        - e["nb_trades_rejetes_liquidite"]
        - e.get("nb_trades_rejetes_stock", 0)
    )
    return _reponse_json({
        "capital_initial": e["capital_initial"],
        "profit_cumule": round(e["profit_cumule_usdt"], 3),
        "nb_trades_total": e["nb_trades_total"],
        "nb_trades_executes": nb_executes,
        "nb_trades_reussis": e["nb_trades_reussis"],
        "nb_trades_rejetes_liquidite": e["nb_trades_rejetes_liquidite"],
        "nb_trades_rejetes_stock": e.get("nb_trades_rejetes_stock", 0),
        "nb_cryptos_eliminees": e["nb_cryptos_eliminees"],
        "circuit_breaker_actif": paper_trading.circuit_breaker_actif(),
        "stop_loss_actif": paper_trading.stop_loss_journalier_actif(),
        "soldes": paper_trading.obtenir_soldes(),
        # None (-> null en JSON) si pas encore calculable de façon fiable —
        # le frontend doit afficher "—" plutôt qu'un chiffre inventé.
        "profit_factor": paper_trading.calculer_profit_factor(),
        "average_rr": paper_trading.calculer_average_rr(),
        # Positions en attente : sans ça, du capital peut être immobilisé
        # sans qu'aucun écran ne le montre (le problème exact qu'ont eu les
        # stocks de tokens, invisibles jusqu'à ce qu'ils bloquent tout).
        "positions_attente": _statistiques_attente(),
    })


def _statistiques_attente() -> dict:
    """Bilan des positions en attente — {} si le module est absent ou désactivé."""
    try:
        import positions_attente
        stats = positions_attente.statistiques()
        stats["liste"] = positions_attente.positions_ouvertes()
        stats["places_disponibles"] = positions_attente.places_disponibles()
        return stats
    except Exception:
        return {}


async def handler_equity_curve(request):
    """Courbe d'équité réelle (capital cumulé après chaque trade papier exécuté) — pour le futur graphique Equity Curve."""
    if not _verifier_auth(request):
        return _reponse_json({"erreur": "non autorisé"}, 401)

    import paper_trading
    return _reponse_json(paper_trading.courbe_equity())


async def handler_trades(request):
    """Historique des trades papier exécutés (les plus récents en premier) — pour la future page Trades."""
    if not _verifier_auth(request):
        return _reponse_json({"erreur": "non autorisé"}, 401)

    import paper_trading
    return _reponse_json(paper_trading.historique_trades(limite=100))


def _exchanges_par_symbole() -> dict:
    """
    {symbole: [exchanges où il est suivi]} — construit depuis les prix live.
    Partagé par plusieurs endpoints pour que chaque liste du dashboard puisse
    afficher le bouton « ? » de disponibilité, pas seulement /api/cryptos.
    """
    import telegram_menu_bot
    resultat = {}
    for exchange, symbols in telegram_menu_bot.prix_live_ref.items():
        for symbole in symbols:
            resultat.setdefault(symbole, []).append(exchange)
    for symbole in resultat:
        resultat[symbole].sort()
    return resultat


async def handler_top_performers(request):
    if not _verifier_auth(request):
        return _reponse_json({"erreur": "non autorisé"}, 401)

    import paper_trading
    dispo = _exchanges_par_symbole()
    classement = []
    for symbole, stats in paper_trading._stats_par_crypto.items():
        if stats["total"] >= 2:
            taux = stats["reussis"] / stats["total"] * 100
            classement.append({
                "symbole": symbole, "taux": round(taux, 1),
                "exchanges": dispo.get(symbole, []),
                **stats,
            })
    classement.sort(key=lambda x: x["taux"], reverse=True)
    return _reponse_json(classement[:20])


async def handler_classement_profit(request):
    """Classement des cryptos par profit CUMULÉ réel (historique complet, pas juste la session)."""
    if not _verifier_auth(request):
        return _reponse_json({"erreur": "non autorisé"}, 401)

    import paper_trading
    meilleures, pires = paper_trading.classement_profit_par_crypto(limite=10)
    dispo = _exchanges_par_symbole()
    for groupe in (meilleures, pires):
        for c in groupe:
            c["exchanges"] = dispo.get(c["symbole"], [])
    return _reponse_json({"meilleures": meilleures, "pires": pires})


async def handler_logos(request):
    """
    Table {TICKER: url_du_logo} pour toutes les cryptos connues de CoinGecko.
    Le frontend la récupère une fois au démarrage et l'utilise en priorité,
    avec repli sur la bibliothèque cryptocurrency-icons puis sur une initiale.
    """
    if not _verifier_auth(request):
        return _reponse_json({"erreur": "non autorisé"}, 401)

    import logos_crypto
    return _reponse_json(logos_crypto.tous())


async def handler_taille_trades(request):
    """Diagnostic : trades à taille pleine vs partiels, avec taux de réussite séparés."""
    if not _verifier_auth(request):
        return _reponse_json({"erreur": "non autorisé"}, 401)

    import paper_trading
    return _reponse_json(paper_trading.stats_taille_trades())


async def handler_cryptos(request):
    """
    Toutes les cryptos ACTUELLEMENT suivies par le bot (peu importe si elles
    ont déjà généré un trade), avec le nombre d'exchanges où chacune est
    disponible et ses stats de rentabilité si elle en a (taux_reussite/
    profit_total à null sinon — pas de fausse donnée). Le logo est géré
    côté frontend (CDN d'icônes), pas besoin de le stocker ici.
    """
    if not _verifier_auth(request):
        return _reponse_json({"erreur": "non autorisé"}, 401)

    import telegram_menu_bot
    import paper_trading
    import prix_24h

    compteur_exchanges = {}
    for exchange, symbols in telegram_menu_bot.prix_live_ref.items():
        for symbole in symbols:
            compteur_exchanges.setdefault(symbole, []).append(exchange)

    stats = paper_trading.stats_toutes_cryptos()

    resultat = []
    for symbole, liste_exchanges in compteur_exchanges.items():
        s = stats.get(symbole, {})
        p24 = prix_24h.obtenir(symbole)
        resultat.append({
            "symbole": symbole,
            "nb_exchanges": len(liste_exchanges),
            "exchanges": sorted(liste_exchanges),  # pour les liens directs vers chaque exchange
            "taux_reussite": s.get("taux_reussite"),
            "profit_total": s.get("profit_total"),
            "nb_trades": s.get("nb_trades", 0),
            "nb_gains": s.get("nb_gains", 0),
            "nb_pertes": s.get("nb_pertes", 0),
            "prix": p24["prix"] if p24 else None,
            "variation_24h_pct": round(p24["variation_24h_pct"], 2) if p24 else None,
        })

    # Cryptos avec des vraies données de profit en premier (triées par profit
    # décroissant), puis le reste par ordre alphabétique
    resultat.sort(key=lambda x: (x["profit_total"] is None, -(x["profit_total"] or 0), x["symbole"]))
    return _reponse_json(resultat)


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
    elif action == "blacklist_toggle":
        import health_manager
        health_manager.definir_blacklist_active(not health_manager.blacklist_active())
    elif action == "elimination_toggle":
        import paper_trading
        paper_trading.definir_elimination_active(not paper_trading.elimination_active())
    else:
        return _reponse_json({"erreur": "action inconnue"}, 400)

    return _reponse_json({"ok": True, "action": action})


async def handler_blacklist(request):
    if not _verifier_auth(request):
        return _reponse_json({"erreur": "non autorisé"}, 401)

    import health_manager
    bl = health_manager.charger_blacklist()
    resultat = [
        {"symbole": s, "raison": info.get("raison", ""), "detecte_le": info.get("detecte_le")}
        for s, info in bl.items()
    ]
    resultat.sort(key=lambda x: x["detecte_le"] or 0, reverse=True)
    return _reponse_json(resultat)


async def handler_historique(request):
    """Dernières lignes du CSV complet d'opportunités (persistant, pas juste la session)."""
    if not _verifier_auth(request):
        return _reponse_json({"erreur": "non autorisé"}, 401)

    import csv
    import opportunity_logger
    if not os.path.exists(opportunity_logger.CSV_PATH):
        return _reponse_json([])

    with open(opportunity_logger.CSV_PATH, newline="", encoding="utf-8") as f:
        lignes = list(csv.DictReader(f))

    return _reponse_json(lignes[-50:][::-1])


async def handler_ml_stats(request):
    if not _verifier_auth(request):
        return _reponse_json({"erreur": "non autorisé"}, 401)

    import csv
    import opportunity_logger
    if not os.path.exists(opportunity_logger.CSV_PATH):
        return _reponse_json({"total": 0, "confirmees_5s": 0, "taux_confirmation": 0})

    with open(opportunity_logger.CSV_PATH, newline="", encoding="utf-8") as f:
        lignes = list(csv.DictReader(f))

    total = len(lignes)
    confirmees = sum(1 for l in lignes if l.get("confirmee_5s") == "1")
    taux = (confirmees / total * 100) if total else 0

    return _reponse_json({
        "total": total,
        "confirmees_5s": confirmees,
        "taux_confirmation": round(taux, 1),
        "objectif_atteint": total >= 500,
    })


async def handler_transferts(request):
    if not _verifier_auth(request):
        return _reponse_json({"erreur": "non autorisé"}, 401)

    import csv
    import paper_trading
    if not os.path.exists(paper_trading.TRANSFERTS_CSV_PATH):
        return _reponse_json([])

    with open(paper_trading.TRANSFERTS_CSV_PATH, newline="", encoding="utf-8") as f:
        lignes = list(csv.DictReader(f))

    return _reponse_json(lignes[-30:][::-1])


async def handler_erreurs(request):
    if not _verifier_auth(request):
        return _reponse_json({"erreur": "non autorisé"}, 401)

    import telegram_menu_bot
    erreurs = list(telegram_menu_bot._capture_erreurs.erreurs)[::-1]
    return _reponse_json(erreurs)


async def handler_config(request):
    """Seuils et paramètres actuels — aucune donnée sensible (pas de clés API)."""
    if not _verifier_auth(request):
        return _reponse_json({"erreur": "non autorisé"}, 401)

    import config
    return _reponse_json({
        "seuil_min_inter_exchange_pct": config.SEUIL_MIN_INTER_EXCHANGE_PCT,
        "seuil_min_triangulaire_pct": config.SEUIL_MIN_TRIANGULAIRE_PCT,
        "seuil_min_collecte_ml_pct": config.SEUIL_MIN_COLLECTE_ML_PCT,
        "seuil_ecart_absurde_pct": config.SEUIL_ECART_ABSURDE_PCT,
        "seuil_persistance_suspecte_sec": config.SEUIL_PERSISTANCE_SUSPECTE_SEC,
        "ttl_blacklist_sec": config.TTL_BLACKLIST_SEC,
        "volume_min_usdt": config.VOLUME_MIN_USDT,
        "min_exchanges": config.MIN_EXCHANGES,
        "nb_connexions_par_exchange": config.NB_CONNEXIONS_PAR_EXCHANGE,
        "max_alertes_par_minute": config.MAX_ALERTES_PAR_MINUTE,
        "cooldown_par_crypto_sec": config.COOLDOWN_PAR_CRYPTO_SEC,
        "circuit_breaker_active": config.CIRCUIT_BREAKER_ACTIVE,
        "circuit_breaker_pertes_consecutives": config.CIRCUIT_BREAKER_PERTES_CONSECUTIVES,
        "stop_loss_journalier_usdt": config.STOP_LOSS_JOURNALIER_USDT,
        "capital_par_exchange_papier": config.CAPITAL_PAR_EXCHANGE_PAPIER,
        "seuil_reequilibrage_pct": config.SEUIL_REEQUILIBRAGE_PCT,
        "frais_transfert_simule_usdt": config.FRAIS_TRANSFERT_SIMULE_USDT,
        "frais_trading_pct": config.FRAIS_TRADING_PCT,
    })


async def handler_perf_detail(request):
    if not _verifier_auth(request):
        return _reponse_json({"erreur": "non autorisé"}, 401)

    import telegram_menu_bot
    opps = [item["opp"] for item in telegram_menu_bot.etat_bot.opportunites_trouvees]
    inter = [o for o in opps if o.type_arbitrage == "inter_exchange"]
    tri = [o for o in opps if o.type_arbitrage == "triangulaire"]

    def _moy(liste):
        return round(sum(o.spread_net_pct for o in liste) / len(liste), 3) if liste else 0

    meilleure = max(opps, key=lambda o: o.spread_net_pct) if opps else None

    return _reponse_json({
        "total": len(opps),
        "inter_exchange": {"nb": len(inter), "moyenne_pct": _moy(inter)},
        "triangulaire": {"nb": len(tri), "moyenne_pct": _moy(tri)},
        "meilleure": {
            "symbole": meilleure.symboles[0], "spread_net_pct": round(meilleure.spread_net_pct, 3),
        } if meilleure else None,
    })


async def handler_config_modifier(request):
    """POST /api/config avec {"seuil_inter_exchange": 0.5, "seuil_triangulaire": 0.4}"""
    if not _verifier_auth(request):
        return _reponse_json({"erreur": "non autorisé"}, 401)

    import telegram_menu_bot
    try:
        data = await request.json()
    except Exception:
        return _reponse_json({"erreur": "JSON invalide"}, 400)

    etat = telegram_menu_bot.etat_bot
    modifie = {}

    if "seuil_inter_exchange" in data:
        try:
            valeur = float(data["seuil_inter_exchange"])
            if not (0 < valeur < 20):
                raise ValueError()
            etat.seuil_inter_exchange = valeur
            modifie["seuil_inter_exchange"] = valeur
        except (ValueError, TypeError):
            return _reponse_json({"erreur": "seuil_inter_exchange invalide (0-20)"}, 400)

    if "seuil_triangulaire" in data:
        try:
            valeur = float(data["seuil_triangulaire"])
            if not (0 < valeur < 20):
                raise ValueError()
            etat.seuil_triangulaire = valeur
            modifie["seuil_triangulaire"] = valeur
        except (ValueError, TypeError):
            return _reponse_json({"erreur": "seuil_triangulaire invalide (0-20)"}, 400)

    return _reponse_json({"ok": True, "modifie": modifie})


async def handler_systeme(request):
    """Métriques système réelles (CPU/RAM). Pas de GPU — ce bot n'en utilise aucun."""
    if not _verifier_auth(request):
        return _reponse_json({"erreur": "non autorisé"}, 401)

    try:
        import psutil
        process = psutil.Process(os.getpid())
        cpu_pct = psutil.cpu_percent(interval=0.3)
        cpu_pct_process = process.cpu_percent(interval=0.3)
        mem = psutil.virtual_memory()
        mem_process_mb = process.memory_info().rss / (1024 * 1024)

        return _reponse_json({
            "cpu_systeme_pct": cpu_pct,
            "cpu_processus_pct": cpu_pct_process,
            "ram_systeme_pct": mem.percent,
            "ram_systeme_utilisee_mb": round(mem.used / (1024 * 1024)),
            "ram_systeme_totale_mb": round(mem.total / (1024 * 1024)),
            "ram_processus_mb": round(mem_process_mb, 1),
            "nb_threads": process.num_threads(),
            "gpu": "N/A — ce bot n'utilise aucun GPU (détection réseau uniquement, pas de calcul IA en direct)",
        })
    except ImportError:
        return _reponse_json({"erreur": "psutil non installé — ajoute-le à requirements.txt"}, 500)
    except Exception as e:
        return _reponse_json({"erreur": str(e)}, 500)


async def handler_reinitialiser(request):
    """POST /api/reinitialiser — vide la blacklist + débloque le circuit breaker."""
    if not _verifier_auth(request):
        return _reponse_json({"erreur": "non autorisé"}, 401)

    import health_manager
    import paper_trading
    health_manager.vider_blacklist()
    paper_trading.reinitialiser_circuit_breaker()
    return _reponse_json({"ok": True})


async def handler_unblacklist(request):
    """POST /api/unblacklist avec {"symbole": "BTCUSDT"}"""
    if not _verifier_auth(request):
        return _reponse_json({"erreur": "non autorisé"}, 401)

    import health_manager
    try:
        data = await request.json()
    except Exception:
        return _reponse_json({"erreur": "JSON invalide"}, 400)

    symbole = (data.get("symbole") or "").strip().upper()
    if not symbole:
        return _reponse_json({"erreur": "symbole manquant"}, 400)

    health_manager.retirer_de_la_blacklist(symbole)
    return _reponse_json({"ok": True, "symbole": symbole})


async def handler_top_cryptos(request):
    """Classement par NOMBRE d'opportunités détectées (différent de top_performers = taux de réussite)."""
    if not _verifier_auth(request):
        return _reponse_json({"erreur": "non autorisé"}, 401)

    import telegram_menu_bot
    compteur = {}
    for item in telegram_menu_bot.etat_bot.opportunites_trouvees:
        symbole = item["opp"].symboles[0]
        compteur[symbole] = compteur.get(symbole, 0) + 1
    classement = sorted(compteur.items(), key=lambda x: x[1], reverse=True)[:20]
    return _reponse_json([{"symbole": s, "nb": n} for s, n in classement])


async def handler_top_paires(request):
    """Classement des paires par nombre d'exchanges où elles sont actives simultanément."""
    if not _verifier_auth(request):
        return _reponse_json({"erreur": "non autorisé"}, 401)

    import telegram_menu_bot
    compteur = {}
    for exchange, symbols in telegram_menu_bot.prix_live_ref.items():
        for symbole in symbols:
            compteur[symbole] = compteur.get(symbole, 0) + 1
    classement = sorted(compteur.items(), key=lambda x: x[1], reverse=True)[:20]
    return _reponse_json([{"symbole": s, "nb_exchanges": n} for s, n in classement])


async def handler_ws_spreads(request):
    """
    WebSocket — diffusion en direct des écarts d'arbitrage pour le panneau
    "Cryptos suivies" (comme un exchange, pas un rafraîchissement périodique).
    Auth via ?token=... dans l'URL (pas de header custom possible sur un
    handshake WebSocket depuis un navigateur).
    """
    if not _verifier_auth_ws(request):
        return web.Response(status=401, text="non autorisé")

    import spreads_live
    import json as _json

    ws = web.WebSocketResponse(heartbeat=25)
    await ws.prepare(request)
    spreads_live.enregistrer_connexion(ws)
    log.info(f"WebSocket spreads connecté ({spreads_live.nb_connexions()} client(s) actif(s))")

    try:
        etat = spreads_live.obtenir_etat_actuel()
        if etat:
            await ws.send_str(_json.dumps({"type": "snapshot", "data": etat}))

        async for _msg in ws:
            pass  # rien n'est attendu du client — juste garder la connexion ouverte
    except Exception as e:
        log.debug(f"WebSocket spreads fermé ({e})")
    finally:
        spreads_live.retirer_connexion(ws)
        log.info(f"WebSocket spreads déconnecté ({spreads_live.nb_connexions()} client(s) restant(s))")

    return ws


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
    app.router.add_get("/api/blacklist", handler_blacklist)
    app.router.add_get("/api/historique", handler_historique)
    app.router.add_get("/api/ml_stats", handler_ml_stats)
    app.router.add_get("/api/transferts", handler_transferts)
    app.router.add_get("/api/erreurs", handler_erreurs)
    app.router.add_get("/api/config", handler_config)
    app.router.add_get("/api/perf_detail", handler_perf_detail)
    app.router.add_post("/api/config", handler_config_modifier)
    app.router.add_get("/api/systeme", handler_systeme)
    app.router.add_post("/api/reinitialiser", handler_reinitialiser)
    app.router.add_post("/api/unblacklist", handler_unblacklist)
    app.router.add_get("/api/top_cryptos", handler_top_cryptos)
    app.router.add_get("/api/top_paires", handler_top_paires)
    app.router.add_get("/api/equity_curve", handler_equity_curve)
    app.router.add_get("/api/trades", handler_trades)
    app.router.add_get("/api/classement_profit", handler_classement_profit)
    app.router.add_get("/api/taille_trades", handler_taille_trades)
    app.router.add_get("/api/logos", handler_logos)
    app.router.add_get("/api/cryptos", handler_cryptos)
    app.router.add_get("/ws/spreads", handler_ws_spreads)

    if DOSSIER_STATIC.exists():
        app.router.add_static("/static/", DOSSIER_STATIC, name="static")

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log.info(f"✅ Serveur web démarré sur le port {port}")

    if not API_SECRET:
        log.warning("⚠️ API_SECRET non défini — l'API est accessible SANS protection !")
