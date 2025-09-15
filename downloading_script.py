import asyncio
import os
import csv
import json
import random
import time
from datetime import datetime, timezone
from TikTokApi import TikTokApi

# ================== Configuration ==================
ms_token      = os.environ.get("ms_token", "YOUR_MS_TOKEN_HERE")
DOWNLOAD_DIR  = os.environ.get("DOWNLOAD_DIR", "downloads")
DATA_CSV_PATH = os.environ.get("DATA_CSV_PATH", "tiktok_trending_dataset.csv")
DATA_JSONL    = os.environ.get("DATA_JSONL", "tiktok_trending_dataset.jsonl")

# How many FINAL successfully downloaded videos you want:
COUNT         = int(os.environ.get("COUNT", "1000"))

# Safer paging: request small pages repeatedly
PAGE_SIZE     = int(os.environ.get("PAGE_SIZE", "20"))  # 30–80 is usually safe

# Backoff / stability
MAX_LOOPS              = int(os.environ.get("MAX_LOOPS", "999999"))  # outer guard
MAX_CONSECUTIVE_ERRORS = int(os.environ.get("MAX_CONSECUTIVE_ERRORS", "6"))
BACKOFF_BASE_SEC       = float(os.environ.get("BACKOFF_BASE_SEC", "2.0"))
BACKOFF_MAX_SEC        = float(os.environ.get("BACKOFF_MAX_SEC", "30.0"))
JITTER_SEC             = float(os.environ.get("JITTER_SEC", "0.8"))
RESET_SESSION_AFTER_ERRORS = int(os.environ.get("RESET_SESSION_AFTER_ERRORS", "3"))

# Popular sound heuristic threshold (if available)
POPULAR_SOUND_MIN_USES = int(os.environ.get("POPULAR_SOUND_MIN_USES", "1000"))

# Playwright/browser settings
TIKTOK_BROWSER = os.environ.get("TIKTOK_BROWSER", "chromium")  # chromium | firefox | webkit
HEADLESS       = os.environ.get("HEADLESS", "true").lower() in ("1", "true", "yes")

# ================== Helpers ==================
def _to_iso(ts):
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
    except Exception:
        return None

def _extract_hashtags(data):
    tags = []
    for item in (data.get("textExtra") or []):
        name = item.get("hashtagName")
        if name:
            tags.append(f"#{name}")
    if not tags:
        desc = data.get("desc") or ""
        tags = [w for w in desc.split() if w.startswith("#")]
    seen, uniq = set(), []
    for t in tags:
        tl = t.lower()
        if tl not in seen:
            seen.add(tl)
            uniq.append(t)
    return uniq

def _popular_sound_heuristic(music_obj, music_uses_count):
    try:
        original = (music_obj or {}).get("original")
        popular = (original is False) or (isinstance(music_uses_count, int) and music_uses_count >= POPULAR_SOUND_MIN_USES)
        reasons = []
        if original is False:
            reasons.append("non_original_sound")
        if isinstance(music_uses_count, int):
            reasons.append(f"videoCount={music_uses_count}")
        return bool(popular), "|".join(reasons) if reasons else "no_reason"
    except Exception as e:
        return False, f"error:{e}"

async def _fetch_music_usage_count(api, music_obj):
    if not music_obj:
        return None
    music_id = music_obj.get("id") or music_obj.get("musicId") or music_obj.get("idStr")
    if not music_id:
        return None

    # Try api.music(id=...)
    try:
        m = api.music(id=music_id)
        await m.info()
        md = m.as_dict
        stats = md.get("stats") or {}
        vc = stats.get("videoCount") if isinstance(stats, dict) else None
        if vc is None:
            vc = md.get("videoCount")
        if isinstance(vc, int):
            return vc
    except Exception:
        pass

    # Try api.sound(id=...) as fallback
    try:
        s = api.sound(id=music_id)
        await s.info()
        sd = s.as_dict
        stats = sd.get("stats") or {}
        vc = stats.get("videoCount") if isinstance(stats, dict) else None
        if vc is None:
            vc = sd.get("videoCount")
        if isinstance(vc, int):
            return vc
    except Exception:
        pass

    return None

async def _new_session(api):
    await api.create_sessions(
        ms_tokens=[ms_token],
        num_sessions=1,
        sleep_after=2,                # short human-ish pause between calls
        browser=TIKTOK_BROWSER,
        headless=HEADLESS
    )

async def trending_videos():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    is_new_csv = not os.path.exists(DATA_CSV_PATH)
    csv_file = open(DATA_CSV_PATH, "a", newline="", encoding="utf-8")
    csv_writer = csv.writer(csv_file)
    if is_new_csv:
        csv_writer.writerow([
            "video_id","watch_url","username",
            "creator_followers","creator_video_count","creator_total_likes",
            "avg_likes_per_video",
            "create_time_iso","video_duration_sec",
            "hashtags","uses_popular_sound","music_uses_count","popular_sound_reason",
            "caption","play_count","like_count","comment_count","share_count","download_path"
        ])
    jsonl_file = open(DATA_JSONL, "a", encoding="utf-8")

    downloaded_count = 0
    seen_ids = set()
    consecutive_errors = 0
    loops = 0
    music_usage_cache = {}

    async with TikTokApi() as api:
        await _new_session(api)

        while downloaded_count < COUNT and loops < MAX_LOOPS:
            loops += 1
            page_target = min(PAGE_SIZE, COUNT - downloaded_count)
            print(f"\n=== Page {loops} (need {COUNT - downloaded_count} more; requesting {page_target}) ===")

            try:
                # Request a SMALL page; big counts often trigger 10201
                got_any = False
                async for video in api.trending.videos(count=page_target):
                    got_any = True
                    # Guard: stop if we already reached target
                    if downloaded_count >= COUNT:
                        break

                    try:
                        data = video.as_dict
                        video_id = data.get("id")
                        if not video_id or video_id in seen_ids:
                            continue
                        seen_ids.add(video_id)

                        username = (data.get("author") or {}).get("uniqueId")
                        watch_url = f"https://www.tiktok.com/@{username}/video/{video_id}" if username else None
                        t = api.video(url=watch_url) if watch_url else video

                        # load full info
                        await t.info()
                        data = t.as_dict

                        # ---- author ----
                        author_stats = data.get("authorStats") or {}
                        creator_followers   = author_stats.get("followerCount")
                        creator_video_count = author_stats.get("videoCount")
                        creator_total_likes = author_stats.get("heartCount")
                        avg_likes = None
                        if isinstance(creator_video_count, int) and creator_video_count > 0 and isinstance(creator_total_likes, (int,float)):
                            avg_likes = creator_total_likes / creator_video_count

                        # ---- video ----
                        create_time_iso = _to_iso(data.get("createTime"))
                        video_obj = data.get("video") or {}
                        video_duration_sec = video_obj.get("duration")
                        caption = data.get("desc") or ""
                        hashtags = _extract_hashtags(data)

                        # ---- music ----
                        music_obj = data.get("music") or {}
                        music_id  = music_obj.get("id") or music_obj.get("musicId") or music_obj.get("idStr")
                        if music_id in music_usage_cache:
                            music_uses_count = music_usage_cache[music_id]
                        else:
                            music_uses_count = await _fetch_music_usage_count(api, music_obj)
                            music_usage_cache[music_id] = music_uses_count
                        uses_popular_sound, reason = _popular_sound_heuristic(music_obj, music_uses_count)

                        # ---- stats ----
                        stats = data.get("stats") or {}
                        play_count    = stats.get("playCount")
                        like_count    = stats.get("diggCount") or stats.get("likeCount")
                        comment_count = stats.get("commentCount")
                        share_count   = stats.get("shareCount")

                        # ---- download ----
                        out_path = None
                        try:
                            video_bytes = await t.bytes()
                            out_path = os.path.join(DOWNLOAD_DIR, f"{video_id}.mp4")
                            with open(out_path, "wb") as f:
                                f.write(video_bytes)
                        except Exception as e:
                            print(f"   ✗ download failed for {video_id}: {e}")
                            continue

                        # ---- row ----
                        row = {
                            "video_id": video_id,"watch_url": watch_url,"username": username,
                            "creator_followers": creator_followers,"creator_video_count": creator_video_count,
                            "creator_total_likes": creator_total_likes,"avg_likes_per_video": avg_likes,
                            "create_time_iso": create_time_iso,"video_duration_sec": video_duration_sec,
                            "hashtags": hashtags,"uses_popular_sound": uses_popular_sound,
                            "music_uses_count": music_uses_count,"popular_sound_reason": reason,
                            "caption": caption,"play_count": play_count,"like_count": like_count,
                            "comment_count": comment_count,"share_count": share_count,"download_path": out_path,
                        }

                        csv_writer.writerow([
                            row["video_id"],row["watch_url"],row["username"],
                            row["creator_followers"],row["creator_video_count"],row["creator_total_likes"],
                            row["avg_likes_per_video"],
                            row["create_time_iso"],row["video_duration_sec"],
                            " ".join(row["hashtags"]) if row["hashtags"] else "",
                            row["uses_popular_sound"],row["music_uses_count"],row["popular_sound_reason"],
                            row["caption"],row["play_count"],row["like_count"],row["comment_count"],row["share_count"],row["download_path"],
                        ])
                        csv_file.flush()
                        jsonl_file.write(json.dumps(row, ensure_ascii=False) + "\n")
                        jsonl_file.flush()

                        downloaded_count += 1
                        consecutive_errors = 0  # success resets error counter
                        print(f"   ✓ saved {downloaded_count}/{COUNT}")

                        # polite jitter between items
                        await asyncio.sleep(random.uniform(0.3, 0.9))

                    except Exception as e:
                        print(f"   ✗ item error: {e}")
                        consecutive_errors += 1
                        if consecutive_errors >= RESET_SESSION_AFTER_ERRORS:
                            print("   ↻ restarting session due to consecutive item errors…")
                            await _new_session(api)
                            consecutive_errors = 0
                        continue

                # If the generator yielded nothing, treat as API block (often 10201)
                if not got_any:
                    raise RuntimeError("Empty page (likely 10201/throttle)")

                # small pause between pages
                await asyncio.sleep(random.uniform(1.2, 2.5))

            except Exception as e:
                # Backoff on API errors (10201/throttle/captcha/etc.)
                consecutive_errors += 1
                expo = min(BACKOFF_MAX_SEC, BACKOFF_BASE_SEC * (2 ** min(consecutive_errors, 6)))
                sleep_for = expo + random.uniform(0, JITTER_SEC)
                print(f"⚠️ Page error: {e} — backing off {sleep_for:.1f}s")
                await asyncio.sleep(sleep_for)

                if consecutive_errors >= RESET_SESSION_AFTER_ERRORS:
                    print("↻ Recreating session to clear potential verification/throttle…")
                    await _new_session(api)
                    consecutive_errors = 0

        # End while

    csv_file.close()
    jsonl_file.close()

    print(f"\n✅ Done. {downloaded_count}/{COUNT} videos saved.\n  • CSV: {DATA_CSV_PATH}\n  • JSONL: {DATA_JSONL}\n  • Videos: {DOWNLOAD_DIR}/")

if __name__ == "__main__":
    asyncio.run(trending_videos())
