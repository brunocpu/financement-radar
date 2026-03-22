"""
rebuild_all.py — Pipeline complet en UNE connexion DuckDB.
Plus de risque d'effacement entre étapes.

Usage : python rebuild_all.py [--force]
"""

import csv
import json
import sys
import requests
import duckdb
from pathlib import Path

BASE_DIR = Path(__file__).parent
RAW_DIR = BASE_DIR / "data" / "raw"
DB_PATH = BASE_DIR / "data" / "financement_radar.duckdb"
PACA_CARTO = Path(r"C:\MCP\fonds-vert-paca-carto")
RAW_DIR.mkdir(parents=True, exist_ok=True)

PACA_DEPTS = ('04', '05', '06', '13', '83', '84')
PACA_DEPTS_SET = set(PACA_DEPTS)
REGION_PACA = "Provence-Alpes-Côte d'Azur"

DGCL_URLS = {
    2024: "https://static.data.gouv.fr/resources/projets-finances-par-les-dotations-de-soutien-a-linvestissement-des-collectivites-territoriales/20250721-173636/dgcl-liste-projets-dotations-investissement-local-2024.csv",
    2023: "https://static.data.gouv.fr/resources/projets-finances-par-les-dotations-de-soutien-a-linvestissement-des-collectivites-territoriales/20240625-090908/dgcl-liste-projets-dotations-investissement-local-2023.csv",
    2022: "https://static.data.gouv.fr/resources/projets-finances-par-les-dotations-de-soutien-a-linvestissement-des-collectivites-territoriales/20240625-091205/dgcl-liste-projets-dotations-investissement-local-2022.csv",
    2021: "https://static.data.gouv.fr/resources/projets-finances-par-les-dotations-de-soutien-a-linvestissement-des-collectivites-territoriales/20240625-091143/dgcl-liste-projets-dotations-investissement-local-2021.csv",
    2020: "https://static.data.gouv.fr/resources/projets-finances-par-les-dotations-de-soutien-a-linvestissement-des-collectivites-territoriales/20240625-091129/dgcl-liste-projets-dotations-investissement-local-2020.csv",
    2019: "https://static.data.gouv.fr/resources/projets-finances-par-les-dotations-de-soutien-a-linvestissement-des-collectivites-territoriales/20240625-090948/dgcl-liste-projets-dotations-investissement-local-2019.csv",
    2018: "https://static.data.gouv.fr/resources/projets-finances-par-les-dotations-de-soutien-a-linvestissement-des-collectivites-territoriales/20240625-091104/dgcl-liste-projets-dotations-investissement-local-2018.csv",
}
FV_URLS = {
    2023: "https://static.data.gouv.fr/resources/fonds-vert-liste-des-projets-subventionnes-en-2023/20240731-110636/fonds-vert-2023-export.csv",
    2024: "https://static.data.gouv.fr/resources/fonds-vert-liste-des-projets-subventionnes/20250731-095516/fonds-vert-2024-export.csv",
}
FV_CONFIG = {
    2023: {"col_demarche": "nom_demarche_ds", "col_benef": "nom_beneficiaire_principal"},
    2024: {"col_demarche": "demarche", "col_benef": "raison_sociale_beneficiaire"},
}
CORRECTIONS_2024 = {
    '14719608':'13054','17574680':'06085','18113567':'13077','16726781':'84138',
    '16474025':'04112','17457171':'04143','14026700':'04049','17963893':'06088',
    '16954930':'06083','11338517':'83098','11816713':'83137',
    '15728266':'04245','17029299':'84031',
    '11833274':'83086','11833674':'83086','11833196':'83086','11833649':'83086',
    '11731205':'83086','11833121':'83086','11833617':'83086','11833705':'83086',
    '13014757':'06095','19905814':'83143','18214898':'13055',
}
ADEME_URL = "https://data.ademe.fr/data-fair/api/v1/datasets/les-aides-financieres-de-l'ademe/convert"
ANCT_URL = "https://static.data.gouv.fr/resources/croisement-des-dispositifs-de-politique-publique-de-lanct/20250924-164108/ngeo-anct-cog2025.csv"
FILOSOFI_URL = "https://static.data.gouv.fr/resources/revenu-des-francais-a-la-commune/20251210-134014/revenu-des-francais-a-la-commune-1765372688826.csv"
OFGL_EXPORT_URL = "https://data.ofgl.fr/api/explore/v2.1/catalog/datasets/ofgl-base-communes/exports/csv"


def dl(url, dest, force=False, timeout=120):
    if dest.exists() and not force:
        print(f"  ✓ {dest.name} ({dest.stat().st_size/1e6:.1f} MB)")
        return
    print(f"  ↓ {dest.name}...", end=" ", flush=True)
    r = requests.get(url, timeout=timeout, stream=True)
    r.raise_for_status()
    with open(dest, "wb") as f:
        for chunk in r.iter_content(65536):
            f.write(chunk)
    print(f"OK ({dest.stat().st_size/1e6:.1f} MB)")


def count(con, where=""):
    w = f" WHERE {where}" if where else ""
    return con.execute(f"SELECT COUNT(*) FROM dgcl{w}").fetchone()[0]


def main():
    force = "--force" in sys.argv
    print("=" * 60)
    print("REBUILD COMPLET — FINANCEMENT RADAR PACA")
    print("=" * 60)

    # ═══ DOWNLOADS ═══
    print("\n[1/6] Téléchargements")
    for year, url in sorted(DGCL_URLS.items()):
        dl(url, RAW_DIR / f"dgcl_{year}.csv", force)
    for year, url in sorted(FV_URLS.items()):
        dl(url, RAW_DIR / f"fonds_vert_{year}.csv", force)
    dl(ADEME_URL, RAW_DIR / "ademe_aides.csv", force)
    dl(ANCT_URL, RAW_DIR / "anct_zonages.csv", force)
    dl(FILOSOFI_URL, RAW_DIR / "filosofi_revenus.csv", force)
    # OFGL skip si déjà là (très lent)
    ofgl_file = RAW_DIR / "ofgl_communes.csv"
    if not ofgl_file.exists():
        print("  ↓ OFGL (peut prendre 2-3 min)...", end=" ", flush=True)
        r = requests.get("https://data.ofgl.fr/api/explore/v2.1/catalog/datasets/ofgl-base-communes/records",
                         params={"order_by":"exer DESC","limit":1,"select":"exer"}, timeout=30)
        last_year = r.json()["results"][0]["exer"]
        r2 = requests.get(OFGL_EXPORT_URL, params={
            "select":"exer,com_code,com_name,dep_code,reg_name,tranche_population,rural,montagne,touristique,agregat,montant,ptot",
            "where":f"exer=date'{last_year}'","delimiter":";","use_labels":"false"
        }, timeout=300, stream=True)
        r2.raise_for_status()
        with open(ofgl_file, "wb") as f:
            for chunk in r2.iter_content(65536): f.write(chunk)
        print(f"OK ({ofgl_file.stat().st_size/1e6:.1f} MB)")
    else:
        print(f"  ✓ ofgl_communes.csv ({ofgl_file.stat().st_size/1e6:.1f} MB)")

    # ═══ BASE DUCKDB ═══
    print("\n[2/6] Construction base DuckDB")
    if DB_PATH.exists():
        DB_PATH.unlink()
    con = duckdb.connect(str(DB_PATH))

    con.execute("""CREATE TABLE dgcl (
        exercice SMALLINT NOT NULL, dispositif VARCHAR NOT NULL, programme SMALLINT,
        beneficiaire_type VARCHAR, beneficiaire_siren VARCHAR,
        beneficiaire_dep VARCHAR NOT NULL, beneficiaire_nom VARCHAR,
        code_insee VARCHAR NOT NULL, intitule VARCHAR,
        cout_ht DOUBLE, subvention DOUBLE, taux DOUBLE,
        source VARCHAR DEFAULT 'DGCL'
    )""")

    # ── DGCL ──
    for year in sorted(DGCL_URLS.keys()):
        f = RAW_DIR / f"dgcl_{year}.csv"
        n0 = count(con)
        con.execute(f"""INSERT INTO dgcl SELECT
            CAST(exercice AS SMALLINT), UPPER(TRIM(dispositif)), TRY_CAST(programme AS SMALLINT),
            LOWER(TRIM(beneficiaire_type)), TRIM(CAST(beneficiaire_siren AS VARCHAR)),
            CASE WHEN REGEXP_MATCHES(TRIM(CAST(beneficiaire_dep AS VARCHAR)),'^[0-9]+$')
                 THEN LPAD(LTRIM(TRIM(CAST(beneficiaire_dep AS VARCHAR)),'0'),2,'0')
                 ELSE TRIM(CAST(beneficiaire_dep AS VARCHAR)) END,
            TRIM(beneficiaire_nom),
            LPAD(TRIM(CAST(beneficiaire_code_insee AS VARCHAR)),5,'0'),
            TRIM(intitule), cout_ht, subvention, taux, 'DGCL'
        FROM read_csv('{f.as_posix()}', header=true, delim=';', decimal_separator=',', auto_detect=true, ignore_errors=true)
        WHERE beneficiaire_code_insee IS NOT NULL
          AND TRIM(CAST(beneficiaire_code_insee AS VARCHAR)) != ''
          AND LEFT(LPAD(TRIM(CAST(beneficiaire_code_insee AS VARCHAR)),5,'0'),2) IN {PACA_DEPTS}
        """)
        print(f"  DGCL {year}: +{count(con)-n0:,}")

    n_dgcl = count(con)
    print(f"  → DGCL total : {n_dgcl:,}")

    # ── FONDS VERT PACA ──
    print("\n  Fonds Vert PACA...")
    corrections_2023 = {}
    corr_file = BASE_DIR / "corrections_2023.json"
    if not corr_file.exists():
        corr_file = PACA_CARTO / "data" / "corrections_2023.json"
    if corr_file.exists():
        raw = json.loads(corr_file.read_text(encoding="utf-8"))
        corrections_2023 = {k: v["code_commune"] for k, v in raw.items()}
    corrections = {**corrections_2023, **CORRECTIONS_2024}
    print(f"  Corrections : {len(corrections)}")

    for year in sorted(FV_URLS.keys()):
        src = RAW_DIR / f"fonds_vert_{year}.csv"
        cfg = FV_CONFIG[year]
        col_dem, col_benef = cfg["col_demarche"], cfg["col_benef"]
        rows = []
        with open(src, 'r', encoding='utf-8') as fh:
            for row in csv.DictReader(fh):
                region = (row.get("nom_region") or "").strip()
                dept = (row.get("code_departement") or "").strip()
                if region != REGION_PACA and dept not in PACA_DEPTS_SET:
                    continue
                code = (row.get("code_commune") or "").strip()
                dossier = (row.get("numero_dossier_ds") or "").strip()
                if dossier in corrections:
                    code = corrections[dossier]
                elif not code or code in ("NULL","nan","0"):
                    continue
                code = code.zfill(5)
                if code[:2] not in PACA_DEPTS_SET:
                    continue
                montant_raw = (row.get("montant_engage") or "0").replace("\xa0","").replace(" ","").replace(",",".")
                try: montant = float(montant_raw)
                except: montant = 0
                if montant <= 0: continue
                siren = (row.get("siren") or row.get("siret_beneficiaire") or "").strip()
                prefix = siren[:2] if len(siren) >= 2 else ""
                btype = {"21":"commune","20":"epci","24":"epci","25":"epci","26":"epci","22":"departement","23":"region"}.get(prefix, "autre")
                dem = (row.get(col_dem) or "").strip()
                proj = (row.get("nom_du_projet") or "").strip()
                bnom = (row.get(col_benef) or "").strip()
                intitule = " ".join(p for p in [f"[{dem}]" if dem else "", proj, f"— {bnom}" if bnom else ""] if p)
                rows.append((year, 'FONDS VERT', btype, siren[:9] if siren else '', code[:2], bnom, code, intitule, montant))

        n0 = count(con)
        for r in rows:
            con.execute("INSERT INTO dgcl (exercice,dispositif,programme,beneficiaire_type,beneficiaire_siren,beneficiaire_dep,beneficiaire_nom,code_insee,intitule,cout_ht,subvention,taux,source) VALUES (?,?,NULL,?,?,?,?,?,?,NULL,?,NULL,'FONDS_VERT')", r)
        print(f"  FV {year}: +{count(con)-n0:,}")

    n_fv = count(con, "source='FONDS_VERT'")
    print(f"  → Fonds Vert total : {n_fv:,}")

    # ── ADEME PACA ──
    print("\n  ADEME PACA...")
    con.execute("DROP TABLE IF EXISTS ademe_raw")
    ademe_file = RAW_DIR / "ademe_aides.csv"
    con.execute(f"CREATE TABLE ademe_raw AS SELECT * FROM read_csv('{ademe_file.as_posix()}', header=true, auto_detect=true, ignore_errors=true)")
    n0 = count(con)
    con.execute("""INSERT INTO dgcl (exercice,dispositif,programme,beneficiaire_type,beneficiaire_siren,beneficiaire_dep,beneficiaire_nom,code_insee,intitule,cout_ht,subvention,taux,source)
        SELECT COALESCE(EXTRACT(YEAR FROM TRY_CAST("dateConvention" AS DATE)),2023),
            'ADEME',NULL,'commune',
            LEFT(TRIM(CAST("idBeneficiaire" AS VARCHAR)),9),
            SUBSTRING(TRIM(CAST("idBeneficiaire" AS VARCHAR)),3,2),
            TRIM(CAST("nomBeneficiaire" AS VARCHAR)),
            SUBSTRING(TRIM(CAST("idBeneficiaire" AS VARCHAR)),3,5),
            TRIM(CAST("objet" AS VARCHAR)),
            NULL, TRY_CAST("montant" AS DOUBLE), NULL, 'ADEME'
        FROM ademe_raw
        WHERE TRY_CAST("montant" AS DOUBLE) > 0
          AND LEFT(TRIM(CAST("idBeneficiaire" AS VARCHAR)),2) = '21'
          AND LENGTH(TRIM(CAST("idBeneficiaire" AS VARCHAR))) >= 9
          AND SUBSTRING(TRIM(CAST("idBeneficiaire" AS VARCHAR)),3,2) IN ('04','05','06','13','83','84')
    """)
    n_ademe = count(con) - n0
    con.execute("DROP TABLE IF EXISTS ademe_raw")
    print(f"  → ADEME : +{n_ademe:,}")

    # ── Fix codes INSEE ──
    con.execute("UPDATE dgcl SET code_insee='84007' WHERE code_insee='84000'")
    # Arrondissements Marseille → commune
    n_arr = con.execute("SELECT COUNT(*) FROM dgcl WHERE code_insee BETWEEN '13201' AND '13216'").fetchone()[0]
    if n_arr > 0:
        con.execute("UPDATE dgcl SET code_insee='13055' WHERE code_insee BETWEEN '13201' AND '13216'")
        print(f"  Fix arrondissements Marseille : {n_arr} → 13055")
    # ADEME SIREN mal décodé (211300553 → 13005 au lieu de 13055)
    n_before = con.execute("SELECT COUNT(*) FROM dgcl WHERE source='ADEME' AND code_insee='13005' AND LOWER(beneficiaire_nom) LIKE '%marseille%'").fetchone()[0]
    if n_before > 0:
        con.execute("UPDATE dgcl SET code_insee='13055' WHERE source='ADEME' AND code_insee='13005' AND LOWER(beneficiaire_nom) LIKE '%marseille%'")
        print(f"  Fix ADEME Marseille : {n_before} projets 13005 → 13055")

    # ═══ VUES ═══
    print("\n[3/6] Vues")
    con.execute("""CREATE OR REPLACE VIEW v_commune_dispositif_annee AS
        SELECT code_insee, dispositif, exercice, COUNT(*) AS nb_projets,
               COALESCE(SUM(subvention),0) AS total_subvention,
               COALESCE(SUM(cout_ht),0) AS total_cout_ht, AVG(taux) AS taux_moyen
        FROM dgcl GROUP BY code_insee, dispositif, exercice""")
    con.execute("""CREATE OR REPLACE VIEW v_commune_resume AS
        SELECT code_insee,
            COALESCE(MODE(beneficiaire_nom) FILTER (WHERE source='DGCL'),MODE(beneficiaire_nom)) AS nom_commune,
            LEFT(code_insee,2) AS departement,
            COUNT(*) AS nb_projets_total, COALESCE(SUM(subvention),0) AS subventions_total,
            COALESCE(SUM(cout_ht),0) AS cout_total, MIN(exercice) AS premiere_annee,
            MAX(exercice) AS derniere_annee, COUNT(DISTINCT dispositif) AS nb_dispositifs_utilises,
            LIST(DISTINCT dispositif) AS dispositifs_utilises, LIST(DISTINCT source) AS sources
        FROM dgcl GROUP BY code_insee""")
    con.execute("""CREATE OR REPLACE VIEW v_benchmark_dep AS
        SELECT LEFT(code_insee,2) AS departement, dispositif, exercice,
            COUNT(DISTINCT code_insee) AS nb_communes_beneficiaires, COUNT(*) AS nb_projets,
            COALESCE(SUM(subvention),0) AS total_subventions,
            MEDIAN(subvention) AS mediane_subvention, AVG(subvention) AS moyenne_subvention
        FROM dgcl GROUP BY LEFT(code_insee,2), dispositif, exercice""")
    print("  ✓ 3 vues créées")

    # ═══ CONTEXTE ═══
    print("\n[4/6] Contexte (ANCT, OFGL, Filosofi)")

    # ANCT
    con.execute("DROP TABLE IF EXISTS anct_raw"); con.execute("DROP TABLE IF EXISTS zonages")
    anct_f = RAW_DIR / "anct_zonages.csv"
    con.execute(f"CREATE TABLE anct_raw AS SELECT * FROM read_csv('{anct_f.as_posix()}',header=true,auto_detect=true,ignore_errors=true)")
    con.execute("""CREATE TABLE zonages AS SELECT
        LPAD(TRIM(CAST(insee_com AS VARCHAR)),5,'0') AS code_insee,
        CASE WHEN TRIM(COALESCE(CAST(id_pvd AS VARCHAR),''))!='' THEN 1 ELSE 0 END AS pvd,
        CASE WHEN TRIM(COALESCE(CAST(id_acv AS VARCHAR),''))!='' THEN 1 ELSE 0 END AS acv,
        CASE WHEN TRIM(COALESCE(CAST(id_va AS VARCHAR),''))!='' THEN 1 ELSE 0 END AS va,
        CASE WHEN TRIM(COALESCE(CAST(id_ti AS VARCHAR),''))!='' THEN 1 ELSE 0 END AS ti,
        CASE WHEN TRIM(COALESCE(CAST(id_fs AS VARCHAR),''))!='' THEN 1 ELSE 0 END AS fs,
        CASE WHEN TRIM(COALESCE(CAST(id_cite AS VARCHAR),''))!='' THEN 1 ELSE 0 END AS cite_edu
    FROM anct_raw WHERE insee_com IS NOT NULL""")
    con.execute("DROP TABLE IF EXISTS anct_raw")
    print(f"  ANCT : {con.execute('SELECT COUNT(*) FROM zonages').fetchone()[0]:,} communes")

    # OFGL
    con.execute("DROP TABLE IF EXISTS ofgl_raw"); con.execute("DROP TABLE IF EXISTS ofgl")
    con.execute(f"CREATE TABLE ofgl_raw AS SELECT * FROM read_csv('{ofgl_file.as_posix()}',header=true,delim=';',auto_detect=true,ignore_errors=true)")
    con.execute("""CREATE TABLE ofgl AS SELECT
        LPAD(TRIM(CAST(com_code AS VARCHAR)),5,'0') AS code_insee,
        TRIM(CAST(reg_name AS VARCHAR)) AS region, TRY_CAST(ptot AS DOUBLE) AS population,
        TRIM(CAST(tranche_population AS VARCHAR)) AS strate,
        COALESCE(CAST(rural AS VARCHAR),'') AS rural, COALESCE(CAST(montagne AS VARCHAR),'') AS montagne,
        COALESCE(CAST(touristique AS VARCHAR),'') AS touristique,
        TRIM(CAST(agregat AS VARCHAR)) AS agregat, TRY_CAST(montant AS DOUBLE) AS montant
    FROM ofgl_raw WHERE com_code IS NOT NULL""")
    con.execute("DROP TABLE IF EXISTS ofgl_raw")
    print(f"  OFGL : {con.execute('SELECT COUNT(DISTINCT code_insee) FROM ofgl WHERE population>0').fetchone()[0]:,} communes")

    # Filosofi
    con.execute("DROP TABLE IF EXISTS filosofi_raw"); con.execute("DROP TABLE IF EXISTS filosofi")
    filo_f = RAW_DIR / "filosofi_revenus.csv"
    con.execute(f"CREATE TABLE filosofi_raw AS SELECT * FROM read_csv('{filo_f.as_posix()}',header=true,auto_detect=true,ignore_errors=true)")
    cols = [r[0] for r in con.execute("DESCRIBE filosofi_raw").fetchall()]
    col_code = next((c for c in cols if 'code' in c.lower() and 'géo' in c.lower()), next((c for c in cols if 'code' in c.lower() and 'geo' in c.lower()), None))
    col_med = next((c for c in cols if '[disp] médiane' in c.lower() or '[disp] mediane' in c.lower()), None)
    col_men = next((c for c in cols if '[disp] nbre de ménages' in c.lower() or '[disp] nbre de menages' in c.lower()), None)
    col_gini = next((c for c in cols if 'gini' in c.lower() and '[disp]' in c.lower()), None)
    col_ret = next((c for c in cols if 'pensions' in c.lower() and 'retraites' in c.lower() and '[disp]' in c.lower()), None)
    col_prest = next((c for c in cols if 'prestations sociales' in c.lower() and '[disp]' in c.lower()), None)
    col_act = next((c for c in cols if "revenus d'activit" in c.lower() and '[disp]' in c.lower() and 'dont' not in c.lower()), None)
    if not col_act:
        col_act = next((c for c in cols if "revenus d\x92activit" in c.lower() or "activité" in c.lower() and '[disp]' in c.lower() and 'dont' not in c.lower()), None)

    sel = [f'LPAD(TRIM(CAST("{col_code}" AS VARCHAR)),5,\'0\') AS code_insee']
    for key, col in [("revenu_median",col_med),("nb_menages",col_men),("gini",col_gini),("pct_retraites",col_ret),("pct_prestations",col_prest),("pct_activite",col_act)]:
        sel.append(f'TRY_CAST("{col}" AS DOUBLE) AS {key}' if col else f'NULL AS {key}')
    con.execute(f"CREATE TABLE filosofi AS SELECT {','.join(sel)} FROM filosofi_raw WHERE \"{col_code}\" IS NOT NULL")
    con.execute("DROP TABLE IF EXISTS filosofi_raw")
    n_filo = con.execute("SELECT COUNT(*) FROM filosofi WHERE revenu_median IS NOT NULL").fetchone()[0]
    print(f"  Filosofi : {n_filo:,} communes")

    # Vue contextuelle
    con.execute("""CREATE OR REPLACE VIEW v_ofgl_pop AS
        SELECT DISTINCT ON (code_insee) code_insee, population, strate, region, rural, montagne, touristique
        FROM ofgl WHERE population IS NOT NULL AND population > 0""")
    con.execute("""CREATE OR REPLACE VIEW v_ofgl_finance AS SELECT code_insee,
        MAX(CASE WHEN agregat LIKE '%pargne brute%' THEN montant END) AS epargne_brute,
        MAX(CASE WHEN agregat LIKE '%ncours de dette%' THEN montant END) AS dette,
        MAX(CASE WHEN agregat LIKE '%quipement brut%' THEN montant END) AS depenses_equip,
        MAX(CASE WHEN agregat LIKE '%ecettes de fonctionnement%' THEN montant END) AS recettes_fonct
        FROM ofgl GROUP BY code_insee""")
    con.execute("""CREATE OR REPLACE VIEW v_commune_context AS
        SELECT r.code_insee,r.nom_commune,r.departement,r.nb_projets_total,r.subventions_total,
            r.cout_total,r.premiere_annee,r.derniere_annee,r.nb_dispositifs_utilises,
            z.pvd,z.acv,z.va,z.ti,z.fs,z.cite_edu,
            p.population,p.strate,p.region,p.rural,p.montagne,p.touristique,
            f.epargne_brute,f.dette,f.depenses_equip,f.recettes_fonct,
            fi.revenu_median,fi.nb_menages,fi.gini,fi.pct_retraites,fi.pct_prestations,fi.pct_activite
        FROM v_commune_resume r
        LEFT JOIN zonages z ON r.code_insee=z.code_insee
        LEFT JOIN v_ofgl_pop p ON r.code_insee=p.code_insee
        LEFT JOIN v_ofgl_finance f ON r.code_insee=f.code_insee
        LEFT JOIN filosofi fi ON r.code_insee=fi.code_insee""")
    print("  ✓ Vue contextuelle créée")

    # ═══ VÉRIFICATION ═══
    print("\n[5/6] Vérification")
    for r in con.execute("SELECT source, COUNT(*), COALESCE(SUM(subvention),0)/1e6 FROM dgcl GROUP BY source ORDER BY 3 DESC").fetchall():
        print(f"  {r[0]:<15} {r[1]:>6,} projets · {r[2]:>8.1f} M€")
    total = con.execute("SELECT COUNT(*), SUM(subvention)/1e6, COUNT(DISTINCT code_insee), COUNT(DISTINCT LEFT(code_insee,2)) FROM dgcl").fetchone()
    print(f"\n  TOTAL : {total[0]:,} projets · {total[1]:,.1f} M€ · {total[2]:,} communes · {total[3]} départements")

    # Test Gemenos
    t = con.execute("SELECT * FROM v_commune_context WHERE code_insee='13042'").fetchdf()
    if not t.empty:
        r = t.iloc[0]
        print(f"\n  🔍 Gemenos: pop={r.get('population')}, TI={r.get('ti')}, rev={r.get('revenu_median')}")

    con.close()

    # ═══ DATA.JS ═══
    print("\n[6/6] Génération data.js")
    import importlib
    bh = importlib.import_module("build_html")
    bh.build_data_js()

    print("\n" + "=" * 60)
    print("✅ REBUILD TERMINÉ — ouvrez index.html")
    print("=" * 60)


if __name__ == "__main__":
    main()
