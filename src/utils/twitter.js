// Twitter/X URL utilities
// Supports: twitter.com, x.com, mobile.twitter.com, mobile.x.com

function extractTweetId(url) {
  var patterns = [
    /(?:twitter\.com|x\.com)\/\w+\/status\/(\d+)/,
    /(?:twitter\.com|x\.com)\/i\/web\/status\/(\d+)/,
  ];
  for (var i = 0; i < patterns.length; i++) {
    var match = url.match(patterns[i]);
    if (match) return match[1];
  }
  return null;
}

function isTwitterUrl(text) {
  return /https?:\/\/(www\.)?(twitter\.com|x\.com|mobile\.twitter\.com|mobile\.x\.com)\/\w+\/status\/\d+/i.test(text);
}

function extractTwitterUrls(text) {
  var regex = /https?:\/\/(www\.)?(twitter\.com|x\.com|mobile\.twitter\.com|mobile\.x\.com)\/\w+\/status\/\d+\/?(\?[^\s]*)*/gi;
  return (text.match(regex) || []).map(function(url) { return url.replace(/\/$/, ''); });
}

function normalizeTwitterUrl(url) {
  var id = extractTweetId(url);
  if (!id) return null;
  // Extract username
  var userMatch = url.match(/(?:twitter\.com|x\.com)\/(\w+)\/status\//);
  var username = userMatch ? userMatch[1] : 'i';
  return 'https://x.com/' + username + '/status/' + id;
}

function extractTwitterUsername(url) {
  if (!url) return null;
  var m = url.match(/(?:twitter\.com|x\.com|mobile\.twitter\.com|mobile\.x\.com)\/(\w+)\/status\//i);
  if (!m) return null;
  var u = m[1].toLowerCase();
  if (u === 'i' || u === 'web' || u.length < 2) return null;
  return u;
}

module.exports = { extractTweetId: extractTweetId, isTwitterUrl: isTwitterUrl, extractTwitterUrls: extractTwitterUrls, normalizeTwitterUrl: normalizeTwitterUrl, extractTwitterUsername: extractTwitterUsername };
