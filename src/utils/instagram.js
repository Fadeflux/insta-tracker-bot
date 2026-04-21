function extractPostId(url) {
  const patterns = [
    /instagram\.com\/(?:p|reels?|tv)\/([A-Za-z0-9_-]+)/,
    /instagr\.am\/(?:p|reels?|tv)\/([A-Za-z0-9_-]+)/,
  ];
  for (const pattern of patterns) {
    const match = url.match(pattern);
    if (match) return match[1];
  }
  return null;
}

function isInstagramPostUrl(text) {
  return /https?:\/\/(www\.)?(instagram\.com|instagr\.am)\/(p|reels?|tv)\/[A-Za-z0-9_-]+/i.test(text);
}

function extractInstagramUrls(text) {
  const regex = /https?:\/\/(www\.)?(instagram\.com|instagr\.am)\/(p|reels?|tv)\/[A-Za-z0-9_-]+\/?(\?[^\s]*)*/gi;
  return (text.match(regex) || []).map((url) => url.replace(/\/$/, ''));
}

function normalizeUrl(url) {
  const id = extractPostId(url);
  if (!id) return null;
  const typeMatch = url.match(/\/(p|reels?|tv)\//);
  var type = typeMatch ? typeMatch[1] : 'p';
  // Normalize reels -> reel
  if (type === 'reels') type = 'reel';
  return 'https://www.instagram.com/' + type + '/' + id + '/';
}

// Extract the Instagram username that posted a given post from HTML.
// Tries multiple signals (JSON owner.username, meta tags, title) because IG
// varies the structure between embed/main/graphql responses.
function extractUsernameFromHtml(html) {
  if (!html || html.length === 0) return null;
  // Unescape JSON-in-HTML (same trick the scraper already uses)
  var unescaped = html.replace(/\\\\"/g, '"').replace(/\\"/g, '"');

  var patterns = [
    // JSON fields (order matters: most specific first)
    /"owner"\s*:\s*\{[^}]*"username"\s*:\s*"([A-Za-z0-9_.]{1,30})"/,
    /"user"\s*:\s*\{[^}]*"username"\s*:\s*"([A-Za-z0-9_.]{1,30})"/,
    /"username"\s*:\s*"([A-Za-z0-9_.]{1,30})"/,
    // OG + meta tags
    /<meta[^>]*property="instapp:owner_user_id"[^>]*content="([A-Za-z0-9_.]{1,30})"/,
    /<meta[^>]*property="og:title"[^>]*content="[^"]*\(@([A-Za-z0-9_.]{1,30})\)/,
    // Embed captions prefix like "username on Instagram:"
    /class="Username"[^>]*>\s*<[^>]+>\s*([A-Za-z0-9_.]{1,30})\s*</,
    // <title>Username (@handle) on Instagram</title>
    /<title>[^<]*\(@([A-Za-z0-9_.]{1,30})\)/,
  ];

  for (var i = 0; i < patterns.length; i++) {
    var m = unescaped.match(patterns[i]) || html.match(patterns[i]);
    if (m && m[1]) {
      var u = m[1].toLowerCase();
      // Filter out obvious false positives
      if (u === 'instagram' || u === 'meta' || u.length < 2) continue;
      return u;
    }
  }
  return null;
}

module.exports = { extractPostId, isInstagramPostUrl, extractInstagramUrls, normalizeUrl, extractUsernameFromHtml };
