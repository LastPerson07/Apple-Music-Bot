import asyncio
import re
import httpx
from bs4 import BeautifulSoup

APL_BASE = "https://aplmate.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": APL_BASE + "/",
}

TIMEOUT = httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=10.0)
MAX_RETRIES = 3
RETRY_DELAY = 2.0


async def init_session(client: httpx.AsyncClient):
    await client.get(APL_BASE + "/", headers={"User-Agent": HEADERS["User-Agent"]})


async def get_token(client: httpx.AsyncClient, music_url: str) -> str:
    r = await client.post(
        f"{APL_BASE}/action/userverify",
        data={"url": music_url},
        headers={**HEADERS, "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"},
    )
    if not r.text.strip():
        raise RuntimeError("userverify returned empty response")
    j = r.json()
    if j.get("success") and j.get("token"):
        return j["token"]
    raise RuntimeError(f"userverify failed: {j}")


async def get_all_track_forms(client: httpx.AsyncClient, music_url: str, token: str):
    r = await client.post(
        f"{APL_BASE}/action",
        data={"url": music_url, "cf-turnstile-response": token},
        headers={**HEADERS, "Content-Type": "application/x-www-form-urlencoded"},
    )
    j = r.json()
    if not j.get("success"):
        raise RuntimeError(f"/action failed: {j.get('message')}")

    soup = BeautifulSoup(j["html"], "html.parser")
    forms = soup.find_all("form", {"name": "submitapurl"})
    if not forms:
        raise RuntimeError("No track forms found in response HTML")

    # ── Thumbnail: prefer Apple CDN (mzstatic / mzcdn) ───────────────────────
    thumb = None

    # 1. img tags with Apple CDN src
    for img in soup.find_all("img"):
        src = img.get("src", "")
        if "mzstatic.com" in src or "mzcdn.com" in src:
            thumb = src
            break

    # 2. Bare text that looks like an Apple CDN URL
    if not thumb:
        for text in soup.stripped_strings:
            m = re.search(r"https?://\S*mzstatic\.com\S+", text)
            if m:
                thumb = m.group(0).rstrip(".,)")
                break

    # 3. Any img — last resort
    if not thumb:
        first_img = soup.find("img")
        if first_img:
            thumb = first_img.get("src")

    # ── Track forms ───────────────────────────────────────────────────────────
    track_forms = []
    for form in forms:
        fields = {
            inp["name"]: inp["value"]
            for inp in form.find_all("input", {"type": "hidden"})
            if inp.get("name")
        }
        track_forms.append(fields)

    return track_forms, thumb


async def get_links_for_track(
    client: httpx.AsyncClient,
    fields: dict,
    semaphore: asyncio.Semaphore,
    track_index: int,
) -> tuple[int, list[dict]]:
    async with semaphore:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = await client.post(
                    f"{APL_BASE}/action/track",
                    data=fields,
                    headers={**HEADERS, "Content-Type": "application/x-www-form-urlencoded"},
                )
                j = r.json()
                if j.get("error"):
                    return track_index, []

                soup = BeautifulSoup(j["data"], "html.parser")
                results = []
                seen = set()
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if "/dl?token=" not in href:
                        continue
                    full = APL_BASE + href
                    if full in seen:
                        continue
                    seen.add(full)
                    label = a.get_text(strip=True)
                    if label and "Another" not in label:
                        results.append({"quality": label, "link": full})

                # Terminal output
                print(f"\n── Track {track_index} ──")
                for item in results:
                    print(f"  {item['quality']}: {item['link']}")

                return track_index, results

            except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.TimeoutException) as e:
                if attempt < MAX_RETRIES:
                    delay = RETRY_DELAY * (2 ** (attempt - 1))
                    await asyncio.sleep(delay)
                else:
                    print(f"[Track {track_index}] Failed after {MAX_RETRIES} attempts: {e}")
                    return track_index, []

            except Exception as e:
                print(f"[Track {track_index}] Unexpected error: {e}")
                return track_index, []


async def full_flow(music_url: str):
    async with httpx.AsyncClient(follow_redirects=True, timeout=TIMEOUT) as client:
        await init_session(client)
        token = await get_token(client, music_url)
        track_forms, thumb = await get_all_track_forms(client, music_url, token)

        print(f"\n{'='*50}")
        print(f"[Flow] URL        : {music_url}")
        print(f"[Flow] Tracks     : {len(track_forms)}")
        print(f"[Flow] Thumbnail  : {thumb}")
        print(f"{'='*50}")

        semaphore = asyncio.Semaphore(3)
        tasks = [
            asyncio.ensure_future(get_links_for_track(client, f, semaphore, i + 1))
            for i, f in enumerate(track_forms)
        ]

        results_all = {}
        for coro in asyncio.as_completed(tasks):
            track_index, links = await coro
            results_all[track_index] = links

        return results_all, thumb
