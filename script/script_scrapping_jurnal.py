import time
import requests
import pandas as pd
from bs4 import BeautifulSoup

HEADERS = {"User-Agent": "Mozilla/5.0"}
BASE = "https://sinta.kemdiktisaintek.go.id/authors/profile"

session = requests.Session()
session.headers.update(HEADERS)


# =====================================================
# 1. SCOPUS
# =====================================================
def scrape_scopus(id_sinta, max_page=1):
    results = []

    for page in range(1, max_page):
        url = f"{BASE}/{id_sinta}/?view=scopus&page={page}"
        print(f"SCOPUS | {id_sinta} | Page {page}")

        res = session.get(url, timeout=30)
        soup = BeautifulSoup(res.text, "html.parser")

        items = soup.select("div.ar-list-item")
        if not items:
            break

        for a in items:
            title = a.select_one(".ar-title a")
            judul = title.get_text(strip=True) if title else ""
            link_url_article = title["href"] if title and title.has_attr("href") else ""

            meta = a.select("div.ar-meta")

            quartile = ""
            journal = ""
            author_order = ""
            creator = ""
            tahun = ""
            cited = 0

            if len(meta) > 0:
                q = meta[0].select_one(".ar-quartile")
                if q:
                    quartile = q.get_text(strip=True)

                pub = meta[0].select_one(".ar-pub")
                if pub:
                    journal = pub.get_text(strip=True)

                for l in meta[0].select("a"):
                    t = l.get_text(strip=True)
                    if "Author Order" in t:
                        author_order = t.replace("Author Order :", "").strip()
                    if "Creator" in t:
                        creator = t.replace("Creator :", "").strip()

            if len(meta) > 1:
                year_tag = meta[1].select_one(".ar-year")
                cited_tag = meta[1].select_one(".ar-cited")

                if year_tag:
                    tahun = year_tag.get_text(strip=True)

                if cited_tag:
                    cited = int("".join(filter(str.isdigit, cited_tag.get_text())) or 0)

            results.append({
                "id_sinta": id_sinta,
                "id_article": None,  
                "title": judul,
                "url": link_url_article,  # ✅ Tambahan
                "tahun": tahun,
                "quartile_jurnal": quartile,
                "nama_jurnal": journal,
                "author_order": author_order,
                "creator/leader": creator,
                "jumlah_cited": cited
            })

        time.sleep(1)

    return results

# =====================================================
# 2. RESEARCHES & SERVICES (struktur mirip)
# =====================================================
def scrape_research_or_service(id_sinta, view_name):
    url = f"{BASE}/{id_sinta}/?view={view_name}"
    print(f"{view_name.upper()} | {id_sinta}")

    res = session.get(url, timeout=30)
    soup = BeautifulSoup(res.text, "html.parser")

    results = []
    items = soup.select("div.ar-list-item")

    for a in items:
        title = a.select_one(".ar-title a")
        artikel = title.get_text(strip=True) if title else ""

        leader = ""
        penerbit = ""
        personils = ""
        tahun = ""
        pendanaan = ""
        status = ""
        sumber_dana = ""

        meta = a.select("div.ar-meta")

        if len(meta) > 0:
            for l in meta[0].select("a"):
                txt = l.get_text(strip=True)
                if "Leader" in txt:
                    leader = txt.replace("Leader :", "").strip()

            pub = meta[0].select_one(".ar-pub")
            if pub:
                penerbit = pub.get_text(strip=True)

        if len(meta) > 1:
            personils = meta[1].get_text(strip=True).replace("Personils :", "")

        if len(meta) > 2:
            year_tag = meta[2].select_one(".ar-year")
            if year_tag:
                tahun = year_tag.get_text(strip=True)

            q = meta[2].select(".ar-quartile")
            if len(q) > 0:
                pendanaan = q[0].get_text(strip=True)
            if len(q) > 1:
                status = q[1].get_text(strip=True)
            if len(q) > 2:
                sumber_dana = q[2].get_text(strip=True)

        results.append({
            "id_sinta": id_sinta,
            "title": artikel,
            "creator/leader": leader,
            "penerbit/publisher": penerbit,
            "authors": personils,
            "tahun": tahun,
            "pendanaan": pendanaan,
            "status": status,
            "pendanaan_dari": sumber_dana
        })

    return results

# =====================================================
# 3. IPRS
# =====================================================
def scrape_iprs(id_sinta, max_page=1):
    results = []

    for page in range(1, max_page + 1):
        url = f"{BASE}/{id_sinta}/?view=iprs&page={page}"
        print(f"IPRS | {id_sinta} | Page {page}")

        res = session.get(url, timeout=30)
        soup = BeautifulSoup(res.text, "html.parser")

        items = soup.select("div.ar-list-item")
        if not items:
            break

        for a in items:

            # =============================
            # JUDUL
            # =============================
            title_tag = a.select_one(".ar-title a")
            artikel = title_tag.get_text(strip=True) if title_tag else ""

            # =============================
            # META
            # =============================
            meta = a.select("div.ar-meta")

            inventor = ""
            anggota = ""
            tahun = ""
            nomor_permohonan = ""
            status = ""
            hak = ""

            # ---- META ATAS (Inventor + Anggota)
            if len(meta) > 0:
                inventor_tag = meta[0].select_one("a")
                if inventor_tag:
                    inventor = inventor_tag.get_text(strip=True).replace("Inventor :", "").strip()

                anggota_tag = meta[0].select_one(".ar-pub")
                if anggota_tag:
                    anggota = anggota_tag.get_text(strip=True)

            # ---- META BAWAH (Tahun + Nomor + Status + Hak)
            if len(meta) > 1:

                # Tahun
                year_tag = meta[1].select_one(".ar-year")
                if year_tag:
                    tahun = year_tag.get_text(strip=True)

                # Nomor Permohonan & Status
                cited_tags = meta[1].select(".ar-cited")
                for c in cited_tags:
                    text = c.get_text(strip=True)

                    if "Nomor Permohonan" in text:
                        nomor_permohonan = text.replace("Nomor Permohonan :", "").strip()

                    if "Status" in text:
                        status = text.replace("Status :", "").strip()

                # Hak (Hak Cipta, Paten, dll)
                hak_tag = meta[1].select_one(".ar-quartile")
                if hak_tag:
                    hak = hak_tag.get_text(strip=True)

            results.append({
                "id_sinta": id_sinta,
                "title": artikel,
                "creator/leader": inventor,
                "authors": anggota,
                "tahun": tahun,
                "nomor_permohonan": nomor_permohonan,
                "status": status,
                "hak": hak
            })

        time.sleep(1)

    return results

# =====================================================
# 4. Books
# =====================================================
def scrape_books(id_sinta, max_page=1):
    results = []

    for page in range(1, max_page + 1):
        url = f"{BASE}/{id_sinta}/?view=books&page={page}"
        print(f"BOOKS | {id_sinta} | Page {page}")

        res = session.get(url, timeout=30)
        soup = BeautifulSoup(res.text, "html.parser")

        items = soup.select("div.ar-list-item")
        if not items:
            break

        for a in items:

            # =============================
            # JUDUL
            # =============================
            title_tag = a.select_one(".ar-title a")
            judul = title_tag.get_text(strip=True) if title_tag else ""

            meta = a.select("div.ar-meta")

            category = ""
            penulis = ""
            publisher = ""
            tahun = ""
            kota = ""
            isbn = ""

            # ---- META 1 : Category
            if len(meta) > 0:
                cat_tag = meta[0].select_one("a")
                if cat_tag:
                    category = cat_tag.get_text(strip=True).replace("Category :", "").strip()

            # ---- META 2 : Penulis + Publisher
            if len(meta) > 1:
                penulis_tag = meta[1].select_one("a:not(.ar-pub)")
                if penulis_tag:
                    penulis = penulis_tag.get_text(strip=True)

                pub_tag = meta[1].select_one(".ar-pub")
                if pub_tag:
                    publisher = pub_tag.get_text(strip=True)

            # ---- META 3 : Tahun + Kota + ISBN
            if len(meta) > 2:
                year_tag = meta[2].select_one(".ar-year")
                if year_tag:
                    tahun = year_tag.get_text(strip=True)

                cited_tag = meta[2].select_one(".ar-cited")
                if cited_tag:
                    kota = cited_tag.get_text(strip=True)

                quartile_tag = meta[2].select_one(".ar-quartile")
                if quartile_tag:
                    isbn = quartile_tag.get_text(strip=True).replace("ISBN :", "").strip()

            results.append({
                "id_sinta": id_sinta,
                "title": judul,
                "category": category,
                "authors": penulis,
                "penerbit/publisher": publisher,
                "tahun": tahun,
                "kota": kota,
                "isbn": isbn
            })

        time.sleep(1)

    return results

# =====================================================
# 5. Garuda
# =====================================================
def scrape_garuda(id_sinta, max_page=1):
    results = []

    for page in range(1, max_page + 1):
        url = f"{BASE}/{id_sinta}/?view=garuda&page={page}"
        print(f"GARUDA | {id_sinta} | Page {page}")

        res = session.get(url, timeout=30)
        soup = BeautifulSoup(res.text, "html.parser")

        items = soup.select("div.ar-list-item")
        if not items:
            break

        for a in items:

            # =============================
            # JUDUL + LINK ARTIKEL
            # =============================
            title_tag = a.select_one(".ar-title a")
            judul = title_tag.get_text(strip=True) if title_tag else ""
            link_artikel = title_tag["href"] if title_tag and title_tag.has_attr("href") else ""

            meta = a.select("div.ar-meta")

            institusi = ""
            nama_jurnal = ""
            link_jurnal = ""
            author_order = ""
            authors = ""
            tahun = ""
            doi = ""
            akreditasi = ""

            # =============================
            # META ATAS (Institusi + Jurnal)
            # =============================
            if len(meta) > 0:
                links = meta[0].select("a")

                if len(links) > 0:
                    institusi = links[0].get_text(strip=True)

                pub_tag = meta[0].select_one(".ar-pub")
                if pub_tag:
                    nama_jurnal = pub_tag.get_text(strip=True)
                    if pub_tag.has_attr("href"):
                        link_jurnal = pub_tag["href"]

            # =============================
            # META BAWAH (Author + Tahun + DOI + Akreditasi)
            # =============================
            if len(meta) > 1:

                # Author Order
                author_tags = meta[1].select("a")
                for tag in author_tags:
                    text = tag.get_text(strip=True)
                    if "Author Order" in text:
                        author_order = text.replace("Author Order :", "").strip()
                    elif not tag.has_attr("class"):
                        authors = text

                # Tahun
                year_tag = meta[1].select_one(".ar-year")
                if year_tag:
                    tahun = year_tag.get_text(strip=True)

                # DOI
                cited_tag = meta[1].select_one(".ar-cited")
                if cited_tag:
                    doi_text = cited_tag.get_text(strip=True)
                    if "DOI" in doi_text:
                        doi = doi_text.replace("DOI :", "").strip()

                # Akreditasi
                quartile_tag = meta[1].select_one(".ar-quartile")
                if quartile_tag:
                    akreditasi = quartile_tag.get_text(strip=True)

            results.append({
                "id_sinta": id_sinta,
                "title": judul,
                "url": link_artikel,
                "institusi": institusi,
                "nama_jurnal": nama_jurnal,
                "url": link_jurnal,
                "author_order": author_order,
                "authors": authors,
                "tahun": tahun,
                "doi": doi,
                "quartile_jurnal": akreditasi
            })

        time.sleep(1)

    return results

# =====================================================
# 6. Google Scholar
# =====================================================
def scrape_google_scholar(id_sinta, max_page=1):
    results = []

    for page in range(1, max_page + 1):
        url = f"{BASE}/{id_sinta}/?view=googlescholar&page={page}"
        print(f"GOOGLE SCHOLAR | {id_sinta} | Page {page}")

        res = session.get(url, timeout=30)
        soup = BeautifulSoup(res.text, "html.parser")

        items = soup.select("div.ar-list-item")
        if not items:
            break

        for item in items:
            # =========================
            # TITLE + LINK
            # =========================
            title_tag = item.select_one(".ar-title a")
            judul = title_tag.get_text(strip=True) if title_tag else ""
            link_url = title_tag["href"] if title_tag and title_tag.has_attr("href") else ""

            # =========================
            # META DATA
            # =========================
            meta = item.select("div.ar-meta")

            authors = ""
            journal = ""
            tahun = ""
            cited = 0

            # META PERTAMA → Authors & Journal
            if len(meta) > 0:
                author_tag = meta[0].select_one("a")
                if author_tag:
                    authors = author_tag.get_text(strip=True).replace("Authors :", "").strip()

                journal_tag = meta[0].select_one(".ar-pub")
                if journal_tag:
                    journal = journal_tag.get_text(strip=True)

            # META KEDUA → Year & Cited
            if len(meta) > 1:
                year_tag = meta[1].select_one(".ar-year")
                if year_tag:
                    tahun = year_tag.get_text(strip=True)

                cited_tag = meta[1].select_one(".ar-cited")
                if cited_tag:
                    cited_text = cited_tag.get_text(strip=True)
                    cited = int("".join(filter(str.isdigit, cited_text)) or 0)

            results.append({
                "id_sinta": id_sinta,
                "title": judul,
                "url": link_url,
                "authors": authors,
                "penerbit/publisher": journal,
                "tahun": tahun,
                "jumlah_cited": cited
            })

        time.sleep(1)

    return results

# =====================================================
# 6. Rama
# =====================================================
# Not Found / Tidak ada di SINTA

# =====================================================
# MAIN
# =====================================================
def main():
    # df_dosen = pd.read_csv("SINTA_DOSEN_LENGKAP.csv")
    # ids = df_dosen["id_sinta"].dropna().astype(str).tolist()
    ids = ["257962"]

    all_scopus = []
    all_research = []
    all_services = []
    all_iprs = []
    all_books = []
    all_garuda = []
    all_google_scholar = []

    for sid in ids:
        try:
            all_scopus += scrape_scopus(sid, max_page=2)
            all_research += scrape_research_or_service(sid, "researches")
            all_services += scrape_research_or_service(sid, "services")
            all_iprs += scrape_iprs(sid)
            all_books += scrape_books(sid)
            all_garuda += scrape_garuda(sid)
            all_google_scholar += scrape_google_scholar(sid)
            time.sleep(1)
        except Exception as e:
            print("ERROR:", sid, e)

    # pd.DataFrame(all_scopus).to_csv("SINTA_SCOPUS.csv", index=False, encoding="utf-8-sig")
    # pd.DataFrame(all_research).to_csv("SINTA_RESEARCHES.csv", index=False, encoding="utf-8-sig")
    # pd.DataFrame(all_services).to_csv("SINTA_SERVICES.csv", index=False, encoding="utf-8-sig")
    # pd.DataFrame(all_iprs).to_csv("SINTA_IPRS.csv", index=False, encoding="utf-8-sig")
    # pd.DataFrame(all_books).to_csv("SINTA_BOOKS.csv", index=False, encoding="utf-8-sig")
    # pd.DataFrame(all_garuda).to_csv("SINTA_GARUDA.csv", index=False, encoding="utf-8-sig")
    # pd.DataFrame(all_google_scholar).to_csv("SINTA_GOOGLE_SCHOLAR.csv", index=False, encoding="utf-8-sig")

        # =====================================================
    # FULL GABUNGAN SEMUA VIEW
    # =====================================================
    df_scopus = pd.DataFrame(all_scopus)
    df_scopus["source_view"] = "scopus"

    df_research = pd.DataFrame(all_research)
    df_research["source_view"] = "researches"

    df_services = pd.DataFrame(all_services)
    df_services["source_view"] = "services"

    df_iprs = pd.DataFrame(all_iprs)
    df_iprs["source_view"] = "iprs"

    df_books = pd.DataFrame(all_books)
    df_books["source_view"] = "books"

    df_garuda = pd.DataFrame(all_garuda)
    df_garuda["source_view"] = "garuda"

    df_google = pd.DataFrame(all_google_scholar)
    df_google["source_view"] = "google_scholar"

    # Gabungkan semua
    df_full = pd.concat([
        df_scopus,
        df_research,
        df_services,
        df_iprs,
        df_books,
        df_garuda,
        df_google
    ], ignore_index=True, sort=False)

    df_full.to_csv("SINTA_FULL_GABUNGAN.csv", index=False, encoding="utf-8-sig")

    print("✅ SELESAI SEMUA VIEW")


if __name__ == "__main__":
    main()
