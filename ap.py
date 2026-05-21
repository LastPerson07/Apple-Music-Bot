import asyncio
import re
import httpx
from bs4 import BeautifulSoup

APL_BASE = "https://aplmate.com"

# Cloudflare
HEADERS = {
    "User-Agent":        "Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
    "Accept":            "*/*",
    "Accept-Language":   "en-US,en;q=0.9",
    "Accept-Encoding":   "gzip, deflate, br",
    "Origin":            APL_BASE,
    "Referer":           APL_BASE + "/",
    "X-Requested-With":  "XMLHttpRequest",
    "Sec-Fetch-Dest":    "empty",
    "Sec-Fetch-Mode":    "cors",
    "Sec-Fetch-Site":    "same-origin",
    "sec-ch-ua":         '"Chromium";v="124", "Google Chrome";v="124"',
    "sec-ch-ua-mobile":  "?1",
    "sec-ch-ua-platform": '"Android"',
}

TIMEOUT     = httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=10.0)
MAX_RETRIES = 3
RETRY_DELAY = 2.0


async def init_session(client: httpx.AsyncClient):
    await client.get(APL_BASE + "/", headers=HEADERS)


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

    # Thumbnail
    thumb = None
    for img in soup.find_all("img"):
        src = img.get("src", "")
        if "mzstatic.com" in src or "mzcdn.com" in src:
            thumb = src
            break
    if not thumb:
        for text in soup.stripped_strings:
            m = re.search(r"https?://\S*mzstatic\.com\S+", text)
            if m:
                thumb = m.group(0).rstrip(".,)")
                break
    if not thumb:
        first_img = soup.find("img")
        if first_img:
            thumb = first_img.get("src")

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
                    print(f"[Track {track_index}] Error: {j.get('message')}")
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

                print(f"\n── Track {track_index} ──")
                for item in results:
                    print(f"  {item['quality']}: {item['link']}")

                return track_index, results

            except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.TimeoutException) as e:
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(RETRY_DELAY * (2 ** (attempt - 1)))
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

        print(f"\n[Flow] URL: {music_url} | Tracks: {len(track_forms)} | Thumb: {thumb}")

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
        
