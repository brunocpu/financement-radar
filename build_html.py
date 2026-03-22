"""
build_html.py — Génère data.js avec les données embarquées
Inclut l'index PORTEURS pour la recherche par bénéficiaire.

Usage : python build_html.py
"""

import json
import duckdb
import pandas as pd
from pathlib import Path
from collections import defaultdict

BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / "data" / "financement_radar.duckdb"
OUT_DATA = BASE_DIR / "data.js"

def sf(v, d=0):
    try: f=float(v); return f if pd.notna(f) else d
    except: return d
def si(v, d=0):
    try: return int(v) if pd.notna(v) else d
    except: return d
def ss(v):
    if v is None or (isinstance(v,float) and pd.isna(v)): return ""
    return str(v).strip()

def build_data_js():
    print("📊 Extraction...")
    con = duckdb.connect(str(DB_PATH), read_only=True)
    objs = [t[0] for t in con.execute("SELECT table_name FROM information_schema.tables").fetchall()]
    has_ctx = "v_commune_context" in objs
    print(f"  Context view: {'✓' if has_ctx else '✗'}")

    # 1. Stats
    s = con.execute("SELECT COUNT(*),COUNT(DISTINCT code_insee),SUM(subvention),MIN(exercice),MAX(exercice),COUNT(DISTINCT LEFT(code_insee,2)) FROM dgcl").fetchone()
    gstats = {"nb_projets":s[0],"nb_communes":s[1],"total_subventions":s[2],"annee_min":s[3],"annee_max":s[4],"nb_departements":s[5]}

    # 2. Communes
    if has_ctx:
        df = con.execute("SELECT * FROM v_commune_context ORDER BY subventions_total DESC").fetchdf()
    else:
        df = con.execute("""SELECT *,NULL AS population,NULL AS strate,NULL AS region,
            NULL AS rural,NULL AS montagne,NULL AS touristique,
            NULL AS epargne_brute,NULL AS dette,NULL AS depenses_equip,NULL AS recettes_fonct,
            0 AS pvd,0 AS acv,0 AS va,0 AS ti,0 AS fs,0 AS cite_edu,
            NULL AS revenu_median,NULL AS nb_menages,NULL AS gini,
            NULL AS pct_retraites,NULL AS pct_prestations,NULL AS pct_activite
            FROM v_commune_resume ORDER BY subventions_total DESC""").fetchdf()

    print(f"  {len(df):,} communes")
    clist = []
    for _,r in df.iterrows():
        c = {"code":r["code_insee"],"nom":ss(r.get("nom_commune")),"dep":ss(r.get("departement")),
             "projets":si(r.get("nb_projets_total")),"subv":round(sf(r.get("subventions_total")),0),
             "cout":round(sf(r.get("cout_total")),0),"an_min":si(r.get("premiere_annee")),
             "an_max":si(r.get("derniere_annee")),"nb_disp":si(r.get("nb_dispositifs_utilises"))}
        pop=sf(r.get("population"),None)
        if pop and pop>0: c["pop"]=round(pop)
        st=ss(r.get("strate"));
        if st: c["strate"]=st
        rg=ss(r.get("region"));
        if rg: c["region"]=rg
        carac=[]
        if ss(r.get("rural")).lower() in("oui","1","true"): carac.append("Rurale")
        if ss(r.get("montagne")).lower() in("oui","1","true"): carac.append("Montagne")
        if ss(r.get("touristique")).lower() in("oui","1","true"): carac.append("Touristique")
        if carac: c["carac"]=carac
        for k,col in[("epargne","epargne_brute"),("dette","dette"),("dep_equip","depenses_equip"),("rec_fonct","recettes_fonct")]:
            v=sf(r.get(col),None)
            if v is not None: c[k]=round(v,0)
        zon=[]
        for p,l in[("pvd","PVD"),("acv","ACV"),("va","VA"),("ti","TI"),("fs","FS"),("cite_edu","Cité édu.")]:
            if si(r.get(p))==1: zon.append(l)
        if zon: c["zonages"]=zon
        rm=sf(r.get("revenu_median"),None)
        if rm and rm>0: c["rev_med"]=round(rm)
        nm=sf(r.get("nb_menages"),None)
        if nm and nm>0: c["menages"]=round(nm)
        gi=sf(r.get("gini"),None)
        if gi and gi>0: c["gini"]=round(gi,3)
        for k,col in[("pct_ret","pct_retraites"),("pct_prest","pct_prestations"),("pct_act","pct_activite")]:
            v=sf(r.get(col),None)
            if v is not None and v>0: c[k]=round(v,1)
        clist.append(c)

    # 3. Détail par dispositif×année
    det=con.execute("SELECT code_insee,dispositif,exercice,nb_projets,total_subvention FROM v_commune_dispositif_annee ORDER BY code_insee,exercice,dispositif").fetchdf()
    dmap={}
    for _,r in det.iterrows():
        code=r["code_insee"]
        if code not in dmap: dmap[code]=[]
        dmap[code].append({"d":r["dispositif"],"y":si(r["exercice"]),"n":si(r["nb_projets"]),"s":round(sf(r["total_subvention"]),0)})

    # 4. Projets (50/commune) avec bénéficiaire
    print("  📝 Export projets...")
    pmap={}
    for _,r in con.execute("""
        SELECT code_insee, exercice, dispositif, intitule,
               COALESCE(cout_ht,0) AS c, COALESCE(subvention,0) AS s, COALESCE(taux,0) AS t,
               COALESCE(beneficiaire_type,'commune') AS btype, beneficiaire_nom AS bnom, source
        FROM dgcl ORDER BY code_insee, exercice DESC, subvention DESC
    """).fetchdf().iterrows():
        code=r["code_insee"]
        if code not in pmap: pmap[code]=[]
        if len(pmap[code])<50:
            p={"y":si(r["exercice"]),"d":r["dispositif"],"t":r["intitule"] or "",
               "c":round(sf(r["c"]),0),"s":round(sf(r["s"]),0),"r":round(sf(r["t"]),4)}
            btype=ss(r.get("btype"))
            if btype and btype!="commune": p["b"]=btype
            bnom=ss(r.get("bnom")); source=ss(r.get("source"))
            if bnom and source in("FONDS_VERT","ADEME"): p["bn"]=bnom
            pmap[code].append(p)

    # 5. Benchmark
    bmap={}
    for _,r in con.execute("SELECT departement,dispositif,SUM(total_subventions) t,SUM(nb_communes_beneficiaires) n,AVG(mediane_subvention) m FROM v_benchmark_dep GROUP BY departement,dispositif").fetchdf().iterrows():
        d=r["departement"]
        if d not in bmap: bmap[d]={}
        bmap[d][r["dispositif"]]={"total":round(sf(r["t"]),0),"nb":si(r["n"]),"med":round(sf(r["m"]),0)}

    # 6. Disp stats
    dlist=[]
    for _,r in con.execute("SELECT dispositif,exercice,COUNT(*) n,SUM(subvention) t FROM dgcl GROUP BY dispositif,exercice ORDER BY exercice,dispositif").fetchdf().iterrows():
        dlist.append({"d":r["dispositif"],"y":si(r["exercice"]),"n":si(r["n"]),"s":round(sf(r["t"]),0)})

    # ═══ 7. INDEX PORTEURS — agrégation par bénéficiaire ═══
    print("  🏢 Construction index porteurs...")
    porteurs_df = con.execute("""
        SELECT
            beneficiaire_nom, beneficiaire_type, source,
            code_insee, exercice, dispositif,
            COALESCE(subvention, 0) AS subvention,
            intitule
        FROM dgcl
        WHERE beneficiaire_nom IS NOT NULL AND TRIM(beneficiaire_nom) != ''
        ORDER BY beneficiaire_nom, exercice DESC, subvention DESC
    """).fetchdf()

    # Nom commune lookup
    nom_commune_map = {c["code"]: c["nom"] for c in clist}

    porteurs_agg = defaultdict(lambda: {
        "type": "", "subv": 0, "n": 0,
        "communes": defaultdict(lambda: {"n": 0, "s": 0, "nom": ""}),
        "dispositifs": defaultdict(lambda: {"n": 0, "s": 0}),
        "annees": set(),
        "projets": [],  # top 30
    })

    for _, r in porteurs_df.iterrows():
        bnom = ss(r.get("beneficiaire_nom"))
        if not bnom: continue
        p = porteurs_agg[bnom]
        p["type"] = ss(r.get("beneficiaire_type")) or p["type"]
        subv = sf(r.get("subvention"))
        p["subv"] += subv
        p["n"] += 1
        code = r["code_insee"]
        p["communes"][code]["n"] += 1
        p["communes"][code]["s"] += subv
        p["communes"][code]["nom"] = nom_commune_map.get(code, "")
        disp = r["dispositif"]
        p["dispositifs"][disp]["n"] += 1
        p["dispositifs"][disp]["s"] += subv
        p["annees"].add(si(r.get("exercice")))
        if len(p["projets"]) < 30:
            p["projets"].append({
                "y": si(r.get("exercice")), "d": disp,
                "t": ss(r.get("intitule")),
                "s": round(subv, 0), "c": code,
            })

    # Convertir en liste JSON-friendly, triée par montant desc
    porteurs_list = []
    for nom, p in sorted(porteurs_agg.items(), key=lambda x: -x[1]["subv"]):
        if p["n"] < 1: continue
        entry = {
            "nom": nom, "type": p["type"],
            "subv": round(p["subv"], 0), "n": p["n"],
            "an_min": min(p["annees"]) if p["annees"] else 0,
            "an_max": max(p["annees"]) if p["annees"] else 0,
            "communes": sorted(
                [{"code": k, "nom": v["nom"], "n": v["n"], "s": round(v["s"], 0)}
                 for k, v in p["communes"].items()],
                key=lambda x: -x["s"]
            )[:20],
            "dispositifs": sorted(
                [{"d": k, "n": v["n"], "s": round(v["s"], 0)}
                 for k, v in p["dispositifs"].items()],
                key=lambda x: -x["s"]
            ),
            "projets": p["projets"][:30],
        }
        porteurs_list.append(entry)

    print(f"     {len(porteurs_list):,} porteurs distincts")

    con.close()

    # Écriture
    print(f"📦 Écriture {OUT_DATA}...")
    with open(OUT_DATA, "w", encoding="utf-8") as f:
        f.write("// Auto-généré\n")
        for name, data in [("GLOBAL_STATS",gstats),("COMMUNES",clist),("DETAIL",dmap),
                           ("PROJETS",pmap),("BENCHMARK",bmap),("DISP_STATS",dlist),
                           ("PORTEURS",porteurs_list)]:
            f.write(f"const {name} = {json.dumps(data, ensure_ascii=False)};\n")

    sz = OUT_DATA.stat().st_size / 1e6
    print(f"✅ data.js ({sz:.1f} MB) · {len(clist):,} communes · {len(porteurs_list):,} porteurs")

if __name__ == "__main__":
    build_data_js()
