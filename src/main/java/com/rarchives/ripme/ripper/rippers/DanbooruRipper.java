package com.rarchives.ripme.ripper.rippers;

import com.rarchives.ripme.ripper.AbstractJSONRipper;
import com.rarchives.ripme.utils.Http;
import com.rarchives.ripme.utils.Utils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class DanbooruRipper extends AbstractJSONRipper {
    private static final String DANBOORU_SESSION_KEY = "danbooru.auth";
    private static final String NO_MORE_IMAGES_MESSAGE = "No more images. Missing some? They may be restricted. Update your " +
            DANBOORU_SESSION_KEY + " configuration in rip.properties.";

    private static final Map<URL, String> prefixes = new HashMap<>();
    private String current_id;
    private String current_tag;
    private int page_num = 2;

    public DanbooruRipper(URL url) throws IOException {
        super(url);
    }

    private static Map<String, String> cookies = null;
    private static Map<String, String> getCookies() {
        if (cookies == null) {
            cookies = new HashMap<>(1);

            String session_key = Utils.getConfigString(DANBOORU_SESSION_KEY, "");
            if (!session_key.isEmpty()) {
                cookies.put("_danbooru2_session", session_key);
            }
        }
        return cookies;
    }

    @Override
    protected String getDomain() {
        return "danbooru_donmai";
    }

    @Override
    public boolean canRip(URL url) {
        String[] urls_regex = {
                "^https?://danbooru.donmai.us/posts/.*$",
                "^https?://danbooru.donmai.us/post/show/.*$",
                "^https?://danbooru.donmai.us/posts\\?tags=.*$"
        };

        for (String s : urls_regex) {
            if (url.toExternalForm().matches(s)) {
                return true;
            }
        }
        return false;

    }

    @Override
    public String getHost() {
        return "danbooru.donmai.us";
    }

    @Override
    public String getGID(URL url) throws MalformedURLException {
        if (url.toExternalForm().matches("^https?://danbooru.donmai.us/posts\\?tags=.*$")) {
            Pattern p = Pattern.compile("^https?://danbooru.donmai.us/posts\\?tags=(.*)");
            Matcher m = p.matcher(url.toExternalForm());
            if (m.matches()) {
                current_tag = m.group(1);
                return current_tag;
            }
        } else {
            final List<Pattern> url_patterns = new ArrayList<>();
            url_patterns.add(Pattern.compile("^https?://danbooru.donmai.us/posts/([0-9]+).*$"));
            url_patterns.add(Pattern.compile("^https?://danbooru.donmai.us/post/show/([0-9]+).*$"));
            for (Pattern url_pattern: url_patterns) {
                Matcher m = url_pattern.matcher(url.toExternalForm());
                if (m.matches()) {
                    current_id = m.group(1);
                    return current_id;
                }
            }
        }

        throw new MalformedURLException("Expected danbooru.donmai.us/posts/123456 URL format.");
    }

    @Override
    protected JSONObject getFirstPage() throws IOException {
        if (url.toExternalForm().matches("^https?://danbooru.donmai.us/posts\\?tags=.*$")) {
            Http httpClient = new Http("https://danbooru.donmai.us/posts.json?tags=" + current_tag).cookies(getCookies());
            httpClient.ignoreContentType();
            String r_body = httpClient.get().body().text();
            String json_new = "{ posts:" + r_body + "}";
            return new JSONObject(json_new);
        } else {
            if (url.toExternalForm().matches("^https?://danbooru.donmai.us/post/show/([0-9]+).*$")) {
                url = new URL("https://danbooru.donmai.us/posts/" + current_id);
            }
            Http httpClient = new Http(url.toExternalForm() + ".json");
            return httpClient.getJSON();
        }
    }

    @Override
    protected JSONObject getNextPage(JSONObject doc) throws IOException {
        if (url.toExternalForm().matches("^https?://danbooru.donmai.us/posts\\?tags=.*$")) {
            JSONArray jsonArray = doc.getJSONArray("posts");
            Http httpClient = new Http("https://danbooru.donmai.us/posts.json?page=" + Integer.toString(page_num) + "&tags=" + current_tag).cookies(getCookies());
            httpClient.ignoreContentType();
            String r_body = httpClient.get().body().text();
            String json_new = "{ posts:" + r_body + "}";
            JSONObject json_new_obj = new JSONObject(json_new);
            if (json_new_obj.getJSONArray("posts").length() == 0) {
                throw new IOException(NO_MORE_IMAGES_MESSAGE);
            }
            page_num++;
            return json_new_obj;
        } else {
            throw new IOException(NO_MORE_IMAGES_MESSAGE);
        }
    }

    @Override
    protected List<String> getURLsFromJSON(JSONObject json) {
        List<String> urls = new ArrayList<>();
        if (url.toExternalForm().matches("^https?://danbooru.donmai.us/posts\\?tags=.*$")) {
            JSONArray jsonArray = json.getJSONArray("posts");
            for (int i = 0; i < jsonArray.length(); i++) {
                if (!jsonArray.getJSONObject(i).getBoolean("is_banned")) {
                    try{
                        JSONObject object = jsonArray.getJSONObject(i);
                        String url = object.getString("file_url");
                        urls.add(url);

                        String artist = object.optString("tag_string_artist", "");
                        if (artist.isEmpty()) artist = "unknown";
                        String character = object.optString("tag_string_character", "");
                        if (character.isEmpty()) character = "unknown";

                        String prefix = artist + " - " + character + " - ";
                        prefixes.put(new URL(url), prefix);
                    } catch (JSONException | MalformedURLException ignored) {
                    }

                }
            }
            return urls;
        } else {
            if (!json.getBoolean("is_banned")) {
                urls.add(json.getString("file_url"));
            }

            return urls;
        }

    }

    @Override
    protected void downloadURL(URL url, int index) {
        addURLToDownload(url, prefixes.getOrDefault(url, ""));
    }
}
