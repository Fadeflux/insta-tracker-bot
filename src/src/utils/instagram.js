function extractPostId(url) {
  const patterns = [
    /instagram\.com\/(?:p|reel|tv)\/([A-Za-z0-9_-]+)/,
    /instagr\.am\/(?:p|reel|tv)\/([A-Za-z0-9_-]+)/,
  ];
  for (const pattern of patterns) {
    const match = url.match(pattern);
    if (match) return match[1];
  }
  return null;
}

function isInstagramPostUrl(text) {
  return /https?:\/\/(www\.)?(instagram\.com|instagr\.am)\/(p|reel|tv)\/[A-Za-z0-9_-]+/i.test(text);
}

function extractInstagramUrls(text) {
  const regex = /https?:\/\/(www\.)?(instagram\.com|instagr\.am)\/(p|reel|tv)\/[A-Za-z0-9_-]+\/?(\?[^\s]*)*/gi;
  return (text.match(regex) || []).map((url) => url.replace(/\/$/, ''));
}

function normalizeUrl(url) {
  const id = extractPostId(url);
  if (!id) return null;
  const typeMatch = url.match(/\/(p|reel|tv)\//);
  const type = typeMatch ? typeMatch[1] : 'p';
  return 'https://www.instagram.com/' + type + '/' + id + '/';
}

module.exports = { extractPostId, isInstagramPostUrl, extractInstagramUrls, normalizeUrl };
