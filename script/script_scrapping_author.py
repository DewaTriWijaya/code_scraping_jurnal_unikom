import time
import requests
import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.microsoft import EdgeChromiumDriverManager

# =====================================================
# CONFIG
# =====================================================
HEADERS = {"User-Agent": "Mozilla/5.0"}
AFFIL_URL = "https://sinta.kemdiktisaintek.go.id/affiliations/authors/528"
WAIT_TIME = 20


# =====================================================
# SELENIUM INIT
# =====================================================
def init_driver():
    options = Options()
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")

    try:
        service = Service(EdgeChromiumDriverManager().install())
        return webdriver.Edge(service=service, options=options)
    except:
        return webdriver.Edge(options=options)


# =====================================================
# 1. SCRAPE LIST DOSEN
# =====================================================
def scrape_affiliation(page):
    url = f"{AFFIL_URL}?page={page}"
    res = requests.get(url, headers=HEADERS, timeout=30)
    if res.status_code != 200:
        return []

    soup = BeautifulSoup(res.text, "html.parser")
    cards = soup.select("div.col-lg")
    results = []

    for c in cards:
        name_div = c.select_one(".profile-name")
        if not name_div:
            continue   # skip card aneh
        
        a = name_div.find("a")

        nama = a.get_text(strip=True) if a else name_div.get_text(strip=True)
        profile_url = a["href"] if a and a.has_attr("href") else ""

        dept = c.select_one(".profile-dept")
        sid = c.select_one(".profile-id")

        hindex = {"scopus": "", "gs": ""}
        h = c.select_one(".profile-hindex")
        if h:
            for s in h.find_all("span"):
                t = s.get_text(strip=True)
                if "Scopus" in t:
                    hindex["scopus"] = t.split(":")[-1].strip()
                if "GS" in t:
                    hindex["gs"] = t.split(":")[-1].strip()

        score = {}
        for col in c.select("div.col"):
            label = col.select_one(".stat-text")
            val = col.select_one(".stat-num")
            if label and val:
                score[label.text.strip()] = val.text.strip()

        results.append({
            "id_sinta": sid.text.replace("ID :", "").strip(),
            "nama": nama,
            "jurusan": dept.text.strip() if dept else "",
            "profile_url": profile_url,
            "scopus_hindex": hindex["scopus"],
            "gs_hindex": hindex["gs"],
            "sinta_score_3yr": score.get("SINTA Score 3Yr", ""),
            "sinta_score": score.get("SINTA Score", ""),
            "affil_score_3yr": score.get("Affil Score 3Yr", ""),
            "affil_score": score.get("Affil Score", "")
        })

    return results


# =====================================================
# 2. SCRAPE DETAIL STATIC (METRICS)
# =====================================================
def scrape_profile_metrics(url):
    res = requests.get(url, headers=HEADERS, timeout=30)
    soup = BeautifulSoup(res.text, "html.parser")

    data = {}
    rows = soup.select("tbody tr")

    for r in rows:
        td = r.find_all("td")
        if len(td) >= 3:
            key = td[0].text.lower().replace(" ", "_")
            data[f"{key}_scopus"] = td[1].text.strip()
            data[f"{key}_gscholar"] = td[2].text.strip()

    return data


# =====================================================
# 3. SCRAPE QUARTILE (ECHARTS)
# =====================================================
def scrape_quartile(driver, url):
    driver.get(url)

    result = {"Q1": 0, "Q2": 0, "Q3": 0, "Q4": 0, "No-Q": 0}

    try:
        WebDriverWait(driver, WAIT_TIME).until(
            lambda d: d.execute_script(
                "return echarts?.getInstanceByDom(document.getElementById('quartile-pie')) !== null"
            )
        )

        data = driver.execute_script("""
            return echarts.getInstanceByDom(
                document.getElementById('quartile-pie')
            ).getOption().series[0].data
        """)

        for d in data:
            if d["name"] in result:
                result[d["name"]] = d["value"]
            else:
                result["No-Q"] += d["value"]

    except:
        pass

    return result


# =====================================================
# MAIN
# =====================================================
def main():
    all_authors = []
    page = 1

    while True:
        data = scrape_affiliation(page)
        if not data:
            break
        all_authors.extend(data)
        page += 1
        time.sleep(1)

    driver = init_driver()
    final = []

    for a in all_authors:
        print("Scraping:", a["nama"])
        metrics = scrape_profile_metrics(a["profile_url"])
        quartile = scrape_quartile(driver, a["profile_url"])

        final.append({**a, **quartile, **metrics})
        time.sleep(2)

    driver.quit()

    df = pd.DataFrame(final)
    df.to_csv("SINTA_DOSEN_LENGKAP.csv", index=False, encoding="utf-8-sig")
    print("✅ SELESAI")


if __name__ == "__main__":
    main()
