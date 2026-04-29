// crashAlerts.js — Tracks consecutive scrape failures per platform and sends
// Discord notifications when scraping breaks (and again when it recovers).
//
// Why this exists:
//   When Instagram (or any platform) changes its page format, our scraper starts
//   returning errors on every post. Without alerts, we sometimes don't notice
//   for hours. This module sends a single message to a dedicated #notif-crash
//   channel so we react fast.
//
// Algorithm:
//   - In-memory state per platform: { consecutiveFailures, alerted, lastError }
//   - On each scrape attempt, scrapeQueue calls reportFailure() or reportSuccess()
//   - If consecutiveFailures reaches THRESHOLD (3) and not yet alerted → send alert
//     and mark alerted=true.
//   - On the first success after alerted=true → send "recovery" alert and reset.
//   - State is in-memory only: a bot restart resets everything (acceptable —
//     fresh start = clean slate, and we can always observe again over 3 attempts).
//
// Safety:
//   - All Discord sends are wrapped in try/catch. If the Discord API fails
//     (network, missing channel, missing permission), we LOG the error but do
//     NOT throw, so the scraping pipeline keeps running.
//   - If the channel ID env var is missing for a platform, alerts for that
//     platform are silently skipped (no spam in logs).

var THRESHOLD = 3;

// Per-platform state. Initialized lazily on first call so we don't error out
// if a new platform appears later (e.g. someone adds a fifth platform).
var state = {};

function ensureState(platform) {
  if (!state[platform]) {
    state[platform] = {
      consecutiveFailures: 0,
      alerted: false,
      lastError: null,
      lastFailureAt: null,
      alertSentAt: null,
    };
  }
  return state[platform];
}

// Map platform name → env var name for the alert channel ID
function getCrashChannelId(platform) {
  var key = 'CHANNEL_NOTIF_CRASH_' + String(platform).toUpperCase();
  return process.env[key] || null;
}

// Discord client is set by index.js at startup so we don't need to require()
// circular things.
var discordClient = null;
function setDiscordClient(client) { discordClient = client; }

// Send a Discord embed to a specific channel ID. Wrapped in try/catch so the
// scraper never crashes because of a Discord hiccup.
async function sendToChannel(channelId, embed) {
  if (!discordClient) {
    console.log('[CrashAlert] Discord client not ready, skipping alert');
    return false;
  }
  try {
    var channel = await discordClient.channels.fetch(channelId);
    if (!channel) {
      console.log('[CrashAlert] Channel ' + channelId + ' not found');
      return false;
    }
    await channel.send({ embeds: [embed] });
    return true;
  } catch (e) {
    console.log('[CrashAlert] Send failed for channel ' + channelId + ': ' + e.message);
    return false;
  }
}

// Called by scrapeQueue when a scrape attempt FAILS. Increments the counter and
// triggers an alert if we cross the threshold for the first time.
async function reportFailure(platform, errorMessage) {
  var s = ensureState(platform);
  s.consecutiveFailures++;
  s.lastError = errorMessage || 'Unknown error';
  s.lastFailureAt = new Date();

  // Only alert ONCE per outage (until we recover)
  if (s.consecutiveFailures >= THRESHOLD && !s.alerted) {
    var channelId = getCrashChannelId(platform);
    if (!channelId) {
      console.log('[CrashAlert] No channel configured for ' + platform + ' (CHANNEL_NOTIF_CRASH_' + platform.toUpperCase() + '), skipping');
      s.alerted = true; // mark anyway so we don't keep checking
      return;
    }

    var embed = {
      title: '🚨 SCRAPING ' + platform.toUpperCase() + ' EN PANNE',
      description: 'Le bot rencontre des erreurs répétées lors du scraping de **' + platform + '**.',
      color: 0xef4444, // red
      fields: [
        {
          name: '📊 Détails',
          value:
            '• **Échecs consécutifs** : ' + s.consecutiveFailures + '\n' +
            '• **Dernière erreur** : `' + truncate(s.lastError, 200) + '`\n' +
            '• **Détecté à** : ' + formatTime(s.lastFailureAt),
          inline: false,
        },
        {
          name: '🔍 Actions à vérifier',
          value:
            (platform === 'instagram' || platform === 'threads')
              ? '• État du proxy SOCKS5\n• ' + capitalize(platform) + ' a-t-il changé son format ?\n• Cookies session encore valides ? (Threads)\n• Logs Railway pour plus de détails'
              : '• État de l\'API ' + capitalize(platform) + '\n• Token API encore valide ?\n• Logs Railway pour plus de détails',
          inline: false,
        },
      ],
      footer: { text: 'Tu recevras une notif quand ça repartira.' },
      timestamp: new Date().toISOString(),
    };

    var sent = await sendToChannel(channelId, embed);
    if (sent) {
      s.alerted = true;
      s.alertSentAt = new Date();
      console.log('[CrashAlert] 🚨 Crash alert sent for ' + platform + ' (after ' + s.consecutiveFailures + ' failures)');
    }
  }
}

// Called by scrapeQueue when a scrape attempt SUCCEEDS. Resets the counter and,
// if we were in "alerted" state, sends a recovery message.
async function reportSuccess(platform) {
  var s = ensureState(platform);
  var wasAlerted = s.alerted;
  var failureCount = s.consecutiveFailures;
  s.consecutiveFailures = 0;
  s.alerted = false;

  if (wasAlerted) {
    var channelId = getCrashChannelId(platform);
    if (!channelId) return;

    var downDurationMs = s.alertSentAt ? (Date.now() - s.alertSentAt.getTime()) : 0;
    var downDurationStr = formatDuration(downDurationMs);

    var embed = {
      title: '✅ SCRAPING ' + platform.toUpperCase() + ' EST DE RETOUR',
      description: 'Le scraping de **' + platform + '** fonctionne à nouveau normalement.',
      color: 0x10b981, // green
      fields: [
        {
          name: '⏱️ Durée de la panne',
          value: downDurationStr || 'Inconnue',
          inline: true,
        },
        {
          name: '📊 Échecs avant retour',
          value: String(failureCount),
          inline: true,
        },
      ],
      timestamp: new Date().toISOString(),
    };

    await sendToChannel(channelId, embed);
    console.log('[CrashAlert] ✅ Recovery alert sent for ' + platform + ' (was down for ' + downDurationStr + ')');
  }
}

// Diagnostic helper — current state, useful for debugging via API.
function getStatus() {
  var out = {};
  Object.keys(state).forEach(function(p) {
    out[p] = {
      consecutiveFailures: state[p].consecutiveFailures,
      alerted: state[p].alerted,
      lastError: state[p].lastError,
      lastFailureAt: state[p].lastFailureAt,
      channelConfigured: !!getCrashChannelId(p),
    };
  });
  return out;
}

// === helpers ===
function truncate(s, max) {
  if (!s) return '';
  s = String(s);
  return s.length > max ? s.substring(0, max - 3) + '...' : s;
}
function formatTime(d) {
  if (!d) return '?';
  return d.toLocaleString('fr-FR', { timeZone: 'Africa/Porto-Novo', hour: '2-digit', minute: '2-digit', day: '2-digit', month: '2-digit' });
}
function formatDuration(ms) {
  if (!ms || ms < 0) return '?';
  var s = Math.round(ms / 1000);
  if (s < 60) return s + ' sec';
  var m = Math.floor(s / 60);
  if (m < 60) return m + ' min';
  var h = Math.floor(m / 60);
  var rm = m % 60;
  return h + 'h' + (rm < 10 ? '0' : '') + rm;
}
function capitalize(s) { return s ? s.charAt(0).toUpperCase() + s.slice(1) : s; }

module.exports = {
  reportFailure: reportFailure,
  reportSuccess: reportSuccess,
  setDiscordClient: setDiscordClient,
  getStatus: getStatus,
  THRESHOLD: THRESHOLD,
};
