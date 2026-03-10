import requests
import json
import time
import base64
import psutil
import os
import random
import threading
import logging
from datetime import datetime, timezone, timedelta

# =============================================
# KONFIGURASI — dibaca dari environment variable
# Kalau jalan lokal, isi langsung di sini.
# Kalau jalan di GitHub Actions, isi di Secrets.
# =============================================
GITHUB_TOKEN        = os.environ.get("GITHUB_TOKEN_SCRAPER", "ghp_xxxxxxxxxxxxxxxxxxxxxxxx")
GITHUB_USER         = os.environ.get("GITHUB_USER",          "username_github_kamu")
GITHUB_REPO         = os.environ.get("GITHUB_REPO",          "nama_repo_kamu")
GITHUB_BRANCH       = "main"
GITHUB_FOLDER       = "data/cuaca"
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL",  "https://discord.com/api/webhooks/xxxxx/yyyyy")
# =============================================

# Pengaturan scraping
SLEEP_API_MIN       = 2      # random sleep min antar request BMKG (detik)
SLEEP_API_MAX       = 5      # random sleep max antar request BMKG (detik)
BATCH_MIN           = 1000   # batch push min ke GitHub
BATCH_MAX           = 2000   # batch push max ke GitHub
MAX_RAM_MB          = 150    # batas RAM — scraper pause kalau nyentuh ini
SLEEP_SCRAPER_PAUSE = 10     # scraper nunggu kalau RAM penuh (detik)
SLEEP_GITHUB_UPLOAD = 1      # jeda antar upload file ke GitHub (detik)
SLEEP_ANTAR_SIKLUS  = 3600   # istirahat antar siklus (1 jam) — hanya untuk mode lokal

# Filter 1 ibukota per provinsi (34 provinsi dari CSV permendagri)
# Cocok untuk GitHub Actions (limit 6 jam per job)
IBUKOTA_FILTER = {
    "11.71": "Banda Aceh",
    "12.71": "Medan",
    "13.71": "Padang",
    "14.71": "Pekanbaru",
    "15.71": "Jambi",
    "16.71": "Palembang",
    "17.71": "Bengkulu",
    "18.71": "Bandar Lampung",
    "19.71": "Pangkal Pinang",
    "21.71": "Tanjung Pinang",
    "31.71": "Jakarta",
    "32.73": "Bandung",
    "33.74": "Semarang",
    "34.71": "Yogyakarta",
    "35.78": "Surabaya",
    "36.73": "Serang",
    "51.71": "Denpasar",
    "52.71": "Mataram",
    "53.71": "Kupang",
    "61.71": "Pontianak",
    "62.71": "Palangka Raya",
    "63.71": "Banjarmasin",
    "64.72": "Samarinda",
    "65.03": "Tanjung Selor",
    "71.71": "Manado",
    "72.71": "Palu",
    "73.71": "Makassar",
    "74.71": "Kendari",
    "75.71": "Gorontalo",
    "76.01": "Mamuju",
    "81.71": "Ambon",
    "82.71": "Sofifi",
    "91.71": "Jayapura",
    "92.03": "Manokwari",
}

ADM4_CSV_URL = (
    "https://raw.githubusercontent.com/"
    "kodewilayah/permendagri-72-2019/main/dist/base.csv"
)

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# UTILITIES
# ─────────────────────────────────────────────

def get_ram_mb() -> float:
    return psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024


def clean_filename(nama: str) -> str:
    nama = nama.split(" - ")[0].strip().lower().replace(" ", "_")
    return f"{nama}.json"


def now_wib() -> str:
    wib = timezone(timedelta(hours=7))
    return (
        datetime.now(wib)
        .isoformat(timespec="seconds")
        .replace("+07:00", " WIB")
    )


def random_batch_size() -> int:
    """Pilih ukuran batch secara random antara BATCH_MIN dan BATCH_MAX."""
    return random.randint(BATCH_MIN, BATCH_MAX)


def random_sleep():
    """Sleep random antara SLEEP_API_MIN dan SLEEP_API_MAX detik."""
    delay = random.uniform(SLEEP_API_MIN, SLEEP_API_MAX)
    time.sleep(delay)


# ─────────────────────────────────────────────
# LOAD SEMUA ADM4 SE-INDONESIA
# ─────────────────────────────────────────────

def load_adm4_list() -> list:
    """
    Unduh CSV permendagri, filter hanya adm4 dari kota ibukota provinsi.
    Cocok untuk GitHub Actions (selesai jauh di bawah limit 6 jam).
    """
    log.info("📥 Mengunduh daftar kode adm4 dari permendagri...")
    resp = requests.get(ADM4_CSV_URL, timeout=30)
    resp.raise_for_status()

    result = []
    for line in resp.text.splitlines():
        parts = line.strip().split(",", 1)
        if len(parts) != 2:
            continue
        code, name = parts[0].strip(), parts[1].strip()
        segments   = code.split(".")
        if len(segments) == 4:
            adm2 = f"{segments[0]}.{segments[1]}"
            if adm2 in IBUKOTA_FILTER:
                result.append({
                    "adm4": code,
                    "nama": name,
                    "kota": IBUKOTA_FILTER[adm2],
                })

    from collections import Counter
    rekap = Counter(i["kota"] for i in result)
    log.info(f"   ✅ Total {len(result):,} kelurahan dari {len(rekap)} ibukota provinsi:")
    for kota, jml in sorted(rekap.items()):
        log.info(f"      {kota:<20} : {jml} kelurahan")
    log.info("")
    return result


# ─────────────────────────────────────────────
# GITHUB UPLOAD
# ─────────────────────────────────────────────

def _gh_headers() -> dict:
    return {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }


def get_file_sha(path_in_repo: str):
    url  = (
        f"https://api.github.com/repos/{GITHUB_USER}/{GITHUB_REPO}"
        f"/contents/{path_in_repo}"
    )
    resp = requests.get(url, headers=_gh_headers(), timeout=15)
    return resp.json().get("sha") if resp.status_code == 200 else None


def upload_one(filename: str, content_str: str) -> bool:
    folder = GITHUB_FOLDER.strip("/")
    path   = f"{folder}/{filename}" if folder else filename
    sha    = get_file_sha(path)

    payload = {
        "message": f"🌤️ Update cuaca: {filename}",
        "content": base64.b64encode(content_str.encode()).decode(),
        "branch":  GITHUB_BRANCH,
    }
    if sha:
        payload["sha"] = sha

    url  = (
        f"https://api.github.com/repos/{GITHUB_USER}/{GITHUB_REPO}"
        f"/contents/{path}"
    )
    resp = requests.put(url, headers=_gh_headers(), json=payload, timeout=20)
    return resp.status_code in (200, 201)


# ─────────────────────────────────────────────
# DISCORD NOTIF
# ─────────────────────────────────────────────

def kirim_discord(ok: int, total: int, batch_ke: int, siklus: int):
    try:
        payload = {
            "embeds": [{
                "title": "🌤️ BMKG Scraper — Batch Selesai!",
                "color": 3066993,
                "fields": [
                    {"name": "🔄 Siklus",          "value": str(siklus),            "inline": True},
                    {"name": "📦 Batch ke",         "value": str(batch_ke),          "inline": True},
                    {"name": "✅ Upload",           "value": f"{ok}/{total} file",   "inline": True},
                    {"name": "🧠 RAM",              "value": f"{get_ram_mb():.1f}MB","inline": True},
                    {"name": "🕐 Waktu",            "value": now_wib(),              "inline": True},
                    {"name": "📁 Repo",
                     "value": f"[{GITHUB_USER}/{GITHUB_REPO}](https://github.com/{GITHUB_USER}/{GITHUB_REPO})",
                     "inline": False},
                ],
                "footer": {"text": "Sumber: BMKG api.bmkg.go.id"}
            }]
        }
        resp = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
        if resp.status_code in (200, 204):
            log.info("  🔔 Notif Discord terkirim!")
        else:
            log.warning(f"  ⚠️  Discord gagal [{resp.status_code}]")
    except Exception as e:
        log.error(f"  ❌ Discord error: {e}")


# ─────────────────────────────────────────────
# PUSH BATCH KE GITHUB
# ─────────────────────────────────────────────

def push_batch(batch: list, stats: dict, batch_ke: int, siklus: int):
    """
    Upload semua file dalam batch ke GitHub satu per satu,
    kosongkan batch dari RAM, kirim notif Discord.
    """
    total = len(batch)
    log.info(f"\n  🚀 Push batch #{batch_ke} — {total:,} file ke GitHub...")
    ok = 0

    for item in batch:
        try:
            success = upload_one(item["filename"], item["content"])
            if success:
                stats["uploaded"] += 1
                ok += 1
            else:
                log.warning(f"    ❌ Gagal: {item['filename']}")
                stats["failed"] += 1
        except Exception as e:
            log.error(f"    ❌ Error upload {item['filename']}: {e}")
            stats["failed"] += 1

        time.sleep(SLEEP_GITHUB_UPLOAD)

    # Hapus dari RAM
    batch.clear()
    log.info(f"  📭 Batch #{batch_ke} selesai. OK: {ok}/{total} | RAM: {get_ram_mb():.1f}MB\n")

    # Kirim notif Discord
    kirim_discord(ok, total, batch_ke, siklus)


# ─────────────────────────────────────────────
# BMKG FETCHER
# ─────────────────────────────────────────────

def fetch_bmkg(adm4: str, tanggal_ambil: str):
    url  = f"https://api.bmkg.go.id/publik/prakiraan-cuaca?adm4={adm4}"
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    js   = resp.json()

    prak_blok = js.get("data", [])
    if not prak_blok:
        return None

    cuaca_item = None
    for blok in prak_blok:
        if "cuaca" in blok and blok["cuaca"]:
            for group in blok["cuaca"]:
                if group:
                    cuaca_item = group[0]
                    break
        if cuaca_item:
            break

    if not cuaca_item:
        return None

    lokasi = js.get("lokasi", {})
    return {
        "provinsi":        lokasi.get("provinsi", ""),
        "kota":            lokasi.get("kotkab", ""),
        "kecamatan":       lokasi.get("kecamatan", ""),
        "desa":            lokasi.get("desa", ""),
        "adm4":            adm4,
        "cuaca":           cuaca_item.get("weather_desc", "N/A"),
        "cuaca_en":        cuaca_item.get("weather_desc_en", "N/A"),
        "curah_hujan":     cuaca_item.get("tp", "0"),
        "kecepatan_angin": cuaca_item.get("ws", "N/A"),
        "arah_angin":      cuaca_item.get("wd", "N/A"),
        "arah_angin_deg":  cuaca_item.get("wd_deg", "N/A"),
        "suhu":            cuaca_item.get("t", "N/A"),
        "kelembaban":      cuaca_item.get("hu", "N/A"),
        "tutupan_awan":    cuaca_item.get("tcc", "N/A"),
        "jarak_pandang":   cuaca_item.get("vs_text", "N/A"),
        "waktu_data":      cuaca_item.get("local_datetime", "N/A"),
        "tanggal_ambil":   tanggal_ambil,
    }


# ─────────────────────────────────────────────
# SCRAPER THREAD
# ─────────────────────────────────────────────

def scraper_thread(adm4_list: list, stats: dict, siklus: int, done_event: threading.Event):
    total         = len(adm4_list)
    tanggal_ambil = now_wib()
    batch         = []
    batch_ke      = 0
    batch_size    = random_batch_size()   # random 1000–2000 per batch

    log.info(f"🌐 Scraper mulai — {total:,} adm4 | batch size: {batch_size:,}")

    for idx, item in enumerate(adm4_list, 1):
        adm4 = item["adm4"]
        nama = item["nama"]

        # ── Guard RAM ─────────────────────────────────────────
        while get_ram_mb() >= MAX_RAM_MB:
            log.warning(
                f"  ⚠️  RAM {get_ram_mb():.1f}MB >= {MAX_RAM_MB}MB! "
                f"Pause {SLEEP_SCRAPER_PAUSE}s..."
            )
            time.sleep(SLEEP_SCRAPER_PAUSE)

        if idx % 100 == 0 or idx == 1:
            log.info(
                f"[{idx:>6,}/{total:,}] batch:{len(batch):>4}/{batch_size}  "
                f"RAM:{get_ram_mb():.1f}MB  — {nama} ({adm4})"
            )

        # ── Fetch BMKG ────────────────────────────────────────
        try:
            entry = fetch_bmkg(adm4, tanggal_ambil)
            if entry is None:
                stats["no_data"] += 1
            else:
                filename    = clean_filename(nama)
                content_str = json.dumps(entry, ensure_ascii=False, indent=2)
                batch.append({"filename": filename, "content": content_str})
                stats["fetched"] += 1

                # ── Batch penuh → push, lalu ambil ukuran batch baru ──
                if len(batch) >= batch_size:
                    batch_ke   += 1
                    push_batch(batch, stats, batch_ke, siklus)
                    batch_size  = random_batch_size()   # random lagi untuk batch berikutnya
                    log.info(f"  📏 Batch size berikutnya: {batch_size:,}")

        except Exception as e:
            log.error(f"  ❌ [{adm4}]: {e}")
            stats["errors"] += 1

        # Random sleep biar nggak kelihatan kayak bot 😤
        random_sleep()

    # ── Final push: sisa batch yang belum penuh ───────────────
    if batch:
        batch_ke += 1
        log.info(f"\n  🏁 Final push — sisa {len(batch):,} file...")
        push_batch(batch, stats, batch_ke, siklus)

    log.info(f"✅ Scraper siklus #{siklus} selesai")
    done_event.set()


# ─────────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────────

def run():
    adm4_list = load_adm4_list()
    total     = len(adm4_list)
    siklus    = 0

    while True:
        siklus += 1
        log.info(f"\n{'='*65}")
        log.info(f"🔄 SIKLUS KE-{siklus} | {now_wib()}")
        log.info(f"   Total target   : {total:,} adm4 se-Indonesia")
        log.info(f"   Batch size     : random {BATCH_MIN:,}–{BATCH_MAX:,} file/push")
        log.info(f"   Sleep API      : random {SLEEP_API_MIN}–{SLEEP_API_MAX} detik")
        log.info(f"   RAM limit      : {MAX_RAM_MB} MB")
        log.info(f"{'='*65}")

        stats = {
            "fetched":  0,
            "uploaded": 0,
            "failed":   0,
            "no_data":  0,
            "errors":   0,
        }

        done_event = threading.Event()
        t = threading.Thread(
            target=scraper_thread,
            args=(adm4_list, stats, siklus, done_event),
            name="Scraper",
            daemon=True,
        )
        t.start()
        t.join()

        log.info(f"\n{'='*65}")
        log.info(f"📊 SIKLUS KE-{siklus} SELESAI — {now_wib()}")
        log.info(f"   ✅ Fetch berhasil  : {stats['fetched']:,}/{total:,}")
        log.info(f"   ☁️  Upload berhasil : {stats['uploaded']:,} file")
        log.info(f"   ❌ Upload gagal    : {stats['failed']:,} file")
        log.info(f"   ⚠️  No data        : {stats['no_data']:,}")
        log.info(f"   💥 Error fetch     : {stats['errors']:,}")
        log.info(f"   🧠 RAM saat ini    : {get_ram_mb():.1f}MB")
        log.info(f"{'='*65}")
        log.info(f"😴 Istirahat {SLEEP_ANTAR_SIKLUS // 60} menit sebelum siklus berikutnya...\n")

        time.sleep(SLEEP_ANTAR_SIKLUS)


if __name__ == "__main__":
    run()
