// Threads URL utilities
// Supports: threads.net, threads.com, www.threads.net, www.threads.com
// URL formats:
//   - https://www.threads.net/@username/post/CODE
//   - https://www.threads.com/@username/post/CODE
//   - https://threads.net/t/CODE (short form)

function extractThreadsPostId(url) {
  var patterns = [
    /threads\.(?:net|com)\/@[^\/]+\/post\/([A-Za-z0-9_-]+)/,
    /threads\.(?:net|com)\/t\/([A-Za-z0-9_-]+)/,
  ];
  for (var i = 0; i < patterns.length; i++) {
    var match = url.match(patterns[i]);
    if (match) return match[1];
  }
  return null;
}

function isThreadsUrl(text) {
  return /https?:\/\/(www\.)?(threads\.net|threads\.com)\/(@[^\/]+\/post|t)\/[A-Za-z0-9_-]+/i.test(text);
}

function extractThreadsUrls(text) {
  var regex = /https?:\/\/(www\.)?(threads\.net|threads\.com)\/(@[^\/]+\/post|t)\/[A-Za-z0-9_-]+\/?(\?[^\s]*)*/gi;
  return (text.match(regex) || []).map(function(url) { return url.replace(/\/$/, ''); });
}

function normalizeThreadsUrl(url) {
  var id = extractThreadsPostId(url);
  if (!id) return null;
  var userMatch = url.match(/threads\.(?:net|com)\/@([A-Za-z0-9_.]+)\/post\//);
  var username = userMatch ? userMatch[1] : null;
  if (username) return 'https://www.threads.net/@' + username + '/post/' + id;
  return 'https://www.threads.net/t/' + id;
}

function extractThreadsUsername(url) {
  if (!url) return null;
  var m = url.match(/threads\.(?:net|com)\/@([A-Za-z0-9_.]+)/i);
  return m ? m[1] : null;
}

module.exports = {
  extractThreadsPostId: extractThreadsPostId,
  isThreadsUrl: isThreadsUrl,
  extractThreadsUrls: extractThreadsUrls,
  normalizeThreadsUrl: normalizeThreadsUrl,
  extractThreadsUsername: extractThreadsUsername,
};
