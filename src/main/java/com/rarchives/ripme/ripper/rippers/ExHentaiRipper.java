package com.rarchives.ripme.ripper.rippers;

import com.rarchives.ripme.ripper.AbstractHTMLRipper;
import com.rarchives.ripme.ripper.DownloadThreadPool;
import com.rarchives.ripme.ui.RipStatusMessage.STATUS;
import com.rarchives.ripme.utils.Http;
import com.rarchives.ripme.utils.Utils;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExHentaiRipper extends AbstractHTMLRipper {
    private static final String EXHENTAI_IGNEOUS = "exhentai.igneous";
    private static final String EXHENTAI_IPB_MEMBER_ID = "exhentai.ipb_member_id";
    private static final String EXHENTAI_IPB_PASS_HASH = "exhentai.ipb_pass_hash";

    // All sleep times are in milliseconds
    private static final int PAGE_SLEEP_TIME = 3000;
    private static final int IMAGE_PAGE_SLEEP_TIME = 3000;
    private static final int IMAGE_DOWNLOAD_SLEEP_TIME = 4000;
    private static final int DOWNLOADER_QUIESCENCE_TIME = 10000;
    private static final int IP_BLOCK_SLEEP_TIME = 70000;

    private String lastURL = null;

    // When downloading a full-res image, other requests to the server will fail. This is a problem because AbstractHtmlRipper
    // is loading successive pages of thumbnails while ExHentaiRipper's thread pool is loading the image pages themselves
    // and queueing up the image URLs for the image downloader thread pool to consume and download. We need to somehow serialize
    // all of these requests so that they don't fail.
    //
    // We can get part way there by hijacking the image downloader pool for ExHentaiRipper's own use. However, the
    // image downloader pool's concurrency level is a global setting and it's unreasonable to expect the user to remember
    // to adjust this every time they rip an exhentai album. Since we have to use reflection anyway to hijack the downloader
    // pool, we can just replace it with a single-threaded pool.
    //
    // Unfortunately, we still have the main AbstractHtmlRipper thread loading pages of thumbnails. So we @Override its
    // rip() method to only trigger ExHentaiRipper's work when the thumbnail pages are done downloading.
    //
    // This whole scheme is unbelievably fucked and it's ridiculous that ripme has no facility for this.

    private DownloadThreadPool downloaderPool;
    private ThreadPoolExecutor downloaderThreadPoolExecutor;
    private Queue<Thread> workToDo = new ArrayDeque<>();

    @Override
    public void setup() throws IOException {
        super.setup();

        downloaderThreadPoolExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        try {
            Field threadPoolField = getClass().getSuperclass().getSuperclass().getDeclaredField("threadPool");
            threadPoolField.setAccessible(true);
            Field threadPoolSubField = threadPoolField.getType().getDeclaredField("threadPool");
            threadPoolSubField.setAccessible(true);
            DownloadThreadPool dtp = (DownloadThreadPool)threadPoolField.get(this);
            ((ThreadPoolExecutor)threadPoolSubField.get(dtp)).shutdown();
            threadPoolSubField.set(dtp, downloaderThreadPoolExecutor);

            downloaderPool = dtp;
        } catch (NoSuchFieldException | IllegalAccessException ex) {
            throw new IOException("Someone changed the code", ex);
        }
    }

    @Override
    public void rip() throws IOException {
        // Fetch all of the thumbnail pages and queue up the individual image pages. This will try to shut down the
        // image downloader pool at the end, but fortunately that's a protected method (waitForThreads) so we can
        // no-op it out in ExHentaiRipper and call it in the superclass when we're ready.
        super.rip();

        // With that over with, we control the execution of requests. Since everything is going into a single-threaded
        // pool we can intermingle the image-page-loading and image-downloading tasks.
        for (Thread thread : workToDo) {
            downloaderPool.addThread(thread);
        }

        // Here we busy-wait until it's actually drained before letting AbstractRipper proceed with the shutdown.
        // Unfortunately because the image downloader pool HAS to be a ThreadPoolExecutor it's difficult to tell whether
        // it's currently working on something that may add another task later...
        try {
            for (;;) {
                long lastTaskCount = downloaderThreadPoolExecutor.getTaskCount();
                Thread.sleep(DOWNLOADER_QUIESCENCE_TIME);
                if (downloaderThreadPoolExecutor.getTaskCount() == lastTaskCount) {
                    break;
                }
            }
        } catch (InterruptedException ex) {
            LOGGER.warn("Interrupted while waiting for work queue to complete.", ex);
        }

        // Finally we can let the thread pool be shut down normally.
        super.waitForThreads();
    }

    @Override
    protected void waitForThreads() {
        // No-op (see comment in rip())
    }

    @Override
    protected void checkIfComplete() {
        // After every image download DownloadFileThread is going to ask AbstractHtmlRipper to possibly notify the user
        // that the rip is complete. However, even though there may be nothing in the image download queue, we may still
        // be loading an image page that leads to another image download. Once again ThreadPoolExecutor limits our
        // visibility into whether there's more work to do, so we perform the check a few times to mitigate race conditions.
        for (int i=0; i<3; i++) {
            if (downloaderThreadPoolExecutor.getTaskCount() != downloaderThreadPoolExecutor.getCompletedTaskCount()) {
                return;
            }

            try {
                Thread.sleep(10);
            } catch (InterruptedException ex) {
                LOGGER.warn("Interrupted while checking for rip completion.", ex);
            }
        }

        super.checkIfComplete();
    }

    // Unbelievably, ripme has no rate limiting on downloads, relying on rippers to spoon feed it URLs slowly. But we
    // need to add dozens of downloads to the queue at once before starting any of them, because we only have one thread.
    // So the only way to introduce a sleep is to override these methods.

    @Override
    public void downloadCompleted(URL url, File saveAs) {
        try {
            Thread.sleep(IMAGE_DOWNLOAD_SLEEP_TIME);
        } catch (InterruptedException ex) {
            LOGGER.warn("Interrupted while sleeping after image download success.", ex);
        }

        super.downloadCompleted(url, saveAs);
    }

    @Override
    public void downloadErrored(URL url, String reason) {
        try {
            Thread.sleep(IMAGE_DOWNLOAD_SLEEP_TIME);
        } catch (InterruptedException ex) {
            LOGGER.warn("Interrupted while sleeping after image download failure.", ex);
        }

        super.downloadErrored(url, reason);
    }

    @Override
    public void downloadExists(URL url, File file) {
        try {
            Thread.sleep(IMAGE_DOWNLOAD_SLEEP_TIME);
        } catch (InterruptedException ex) {
            LOGGER.warn("Interrupted while sleeping after image download skip due to existence.", ex);
        }

        super.downloadExists(url, file);
    }

    // Current HTML document
    private Document albumDoc = null;

    private static Map<String, String> cookies = null;
    private static Map<String, String> getCookies() throws IOException {
        if (cookies == null) {
            cookies = new HashMap<>();
            cookies.put("tip", "1");

            String igneous = Utils.getConfigString(EXHENTAI_IGNEOUS, "");
            if (igneous.isEmpty()) {
                throw new IOException("Missing sadpanda login cookie '" + EXHENTAI_IGNEOUS + "' from config.");
            }
            cookies.put("igneous", igneous);

            String ipb_member_id = Utils.getConfigString(EXHENTAI_IPB_MEMBER_ID, "");
            if (ipb_member_id.isEmpty()) {
                throw new IOException("Missing sadpanda login cookie '" + EXHENTAI_IPB_MEMBER_ID + "' from config.");
            }
            cookies.put("ipb_member_id", ipb_member_id);

            String ipb_pass_hash = Utils.getConfigString(EXHENTAI_IPB_PASS_HASH, "");
            if (ipb_pass_hash.isEmpty()) {
                throw new IOException("Missing sadpanda login cookie '" + EXHENTAI_IPB_PASS_HASH + "' from config.");
            }
            cookies.put("ipb_pass_hash", ipb_pass_hash);
        }
        return cookies;
    }

    public ExHentaiRipper(URL url) throws IOException {
        super(url);
    }

    @Override
    public String getHost() {
        return "exhentai";
    }

    @Override
    public String getDomain() {
        return "exhentai.org";
    }

    public String getAlbumTitle(URL url) throws MalformedURLException {
        try {
            // Attempt to use album title as GID
            if (albumDoc == null) {
                albumDoc = getPageWithRetries(url);
            }
            Elements elems = albumDoc.select("#gn");
            return getHost() + "_" + elems.first().text();
        } catch (Exception e) {
            // Fall back to default album naming convention
            LOGGER.warn("Failed to get album title from " + url, e);
        }
        return super.getAlbumTitle(url);
    }

    @Override
    public String getGID(URL url) throws MalformedURLException {
        Pattern p;
        Matcher m;

        p = Pattern.compile("^https://exhentai\\.org/g/([0-9]+)/([a-fA-F0-9]+)/?");
        m = p.matcher(url.toExternalForm());
        if (m.matches()) {
            return m.group(1) + "-" + m.group(2);
        }

        throw new MalformedURLException(
                "Expected exhentai.org gallery format: "
                        + "https://exhentai.org/g/####/####/"
                        + " Got: " + url);
    }

    /**
     * Attempts to get page, checks for IP ban, waits.
     * @param url
     * @return Page document
     * @throws IOException If page loading errors, or if retries are exhausted
     */
    private Document getPageWithRetries(URL url) throws IOException {
        Document doc;
        int retries = 3;
        while (true) {
            sendUpdate(STATUS.LOADING_RESOURCE, url.toExternalForm());
            LOGGER.info("Retrieving " + url);
            doc = Http.url(url)
                      .referrer(this.url)
                      .cookies(getCookies())
                      .get();
            if (doc.toString().contains("IP address will be automatically banned")) {
                if (retries == 0) {
                    throw new IOException("Hit rate limit and maximum number of retries, giving up");
                }
                LOGGER.warn("Hit rate limit while loading " + url + ", sleeping for " + IP_BLOCK_SLEEP_TIME + "ms, " + retries + " retries remaining");
                retries--;
                try {
                    Thread.sleep(IP_BLOCK_SLEEP_TIME);
                } catch (InterruptedException e) {
                    throw new IOException("Interrupted while waiting for rate limit to subside");
                }
            }
            else {
                return doc;
            }
        }
    }

    @Override
    public Document getFirstPage() throws IOException {
        if (albumDoc == null) {
            albumDoc = getPageWithRetries(this.url);
        }
        this.lastURL = this.url.toExternalForm();
        return albumDoc;
    }

    @Override
    public Document getNextPage(Document doc) throws IOException {
        // Check if we've stopped
        if (isStopped()) {
            throw new IOException("Ripping interrupted");
        }
        // Find next page
        Elements hrefs = doc.select(".ptt a");
        if (hrefs.isEmpty()) {
            LOGGER.info("doc: " + doc.html());
            throw new IOException("No navigation links found");
        }
        // Ensure next page is different from the current page
        String nextURL = hrefs.last().attr("href");
        if (nextURL.equals(this.lastURL)) {
            LOGGER.info("lastURL = nextURL : " + nextURL);
            throw new IOException("Reached last page of results");
        }
        // Sleep before loading next page
        sleep(PAGE_SLEEP_TIME);
        // Load next page
        Document nextPage = getPageWithRetries(new URL(nextURL));
        this.lastURL = nextURL;
        return nextPage;
    }

    @Override
    public List<String> getURLsFromPage(Document page) {
        List<String> imageURLs = new ArrayList<>();
        Elements thumbs = page.select("#gdt > .gdtm a");
        // Iterate over images on page
        for (Element thumb : thumbs) {
            imageURLs.add(thumb.attr("href"));
        }
        return imageURLs;
    }

    @Override
    public void downloadURL(URL url, int index) {
        workToDo.add(new ExHentaiImageThread(url, index, this.workingDir));
    }

    /**
     * Helper class to find and download images found on "image" pages
     *
     * Handles case when site has IP-banned the user.
     */
    private class ExHentaiImageThread extends Thread {
        private URL url;
        private int index;
        private File workingDir;

        ExHentaiImageThread(URL url, int index, File workingDir) {
            super();
            this.url = url;
            this.index = index;
            this.workingDir = workingDir;
        }

        @Override
        public void run() {
            fetchImage();

            try {
                sleep(IMAGE_PAGE_SLEEP_TIME);
            } catch (InterruptedException e) {
                LOGGER.warn("Interrupted while sleeping after loading an image page", e);
            }
        }

        private void fetchImage() {
            try {
                Document doc = getPageWithRetries(this.url);

                // Find image
                Elements images = doc.select(".sni > a > img");
                if (images.isEmpty()) {
                    // Attempt to find image elsewise (Issue #41)
                    images = doc.select("img#img");
                    if (images.isEmpty()) {
                        LOGGER.warn("Image not found at " + this.url);
                        return;
                    }
                }
                Element image = images.first();
                String imgsrc = image.attr("src");

                // Override download URL if there's an "original resolution" link
                Optional<String> fullimgUrl = doc.select(".sni a").stream()
                        .map(link -> link.attr("href"))
                        .filter(href -> href.contains("fullimg.php"))
                        .findFirst();
                String downloadUrl = fullimgUrl.orElse(imgsrc);

                LOGGER.info("Found URL " + downloadUrl + " via " + images.get(0));
                Pattern p = Pattern.compile("xres=[^/]+\\/([^&]+)$");
                Matcher m = p.matcher(imgsrc);
                if (m.find()) {
                    // Manually discover filename from URL
                    String savePath = this.workingDir + File.separator;
                    if (Utils.getConfigBoolean("download.save_order", true)) {
                        savePath += String.format("%03d_", index);
                    }
                    savePath += m.group(1);

                    // Don't send your auth cookies to some random guy hosting a hath node.
                    if (downloadUrl.startsWith("https://exhentai.org")) {
                        addURLToDownload(new URL(downloadUrl), new File(savePath), null, getCookies(), false);
                    } else {
                        addURLToDownload(new URL(downloadUrl), new File(savePath));
                    }
                }
                else {
                    // Provide prefix and let the AbstractRipper "guess" the filename
                    String prefix = "";
                    if (Utils.getConfigBoolean("download.save_order", true)) {
                        prefix = String.format("%03d_", index);
                    }

                    // Don't send your auth cookies to some random guy hosting a hath node.
                    if (downloadUrl.startsWith("https://exhentai.org")) {
                        addURLToDownload(new URL(downloadUrl), prefix, "", null, getCookies(), null);
                    } else {
                        addURLToDownload(new URL(downloadUrl), prefix);
                    }
                }
            } catch (IOException e) {
                LOGGER.error("[!] Exception while loading/parsing " + this.url, e);
            }
        }
    }
}
