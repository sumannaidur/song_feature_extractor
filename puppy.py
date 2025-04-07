import asyncio
from pyppeteer import launch
from pyppeteer.chromium_downloader import chromium_executable

async def fetch_youtube_url(query):
    browser = await launch(
        headless=True,
        executablePath=chromium_executable(),
        args=['--no-sandbox', '--disable-dev-shm-usage']
    )
    page = await browser.newPage()
    search_query = query.replace(" ", "+")
    search_url = f"https://www.youtube.com/results?search_query={search_query}"

    await page.goto(search_url)
    await page.waitForSelector("ytd-video-renderer", timeout=10000)

    video_url = await page.evaluate('''() => {
        const video = document.querySelector("ytd-video-renderer a#thumbnail");
        return video ? video.href : null;
    }''')

    await browser.close()
    return video_url

def get_youtube_url(query):
    try:
        return asyncio.get_event_loop().run_until_complete(fetch_youtube_url(query))
    except Exception as e:
        print(f"❌ Error fetching YouTube URL: {e}")
        return None

if __name__ == "__main__":
    query = "Kesariya Arijit Singh official audio"
    youtube_link = get_youtube_url(query)
    print(f"🎬 YouTube URL for '{query}':\n{youtube_link}")
