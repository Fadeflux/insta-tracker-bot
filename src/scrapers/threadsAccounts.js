// Threads accounts pool — manages multiple Threads sessions for scraping rotation.
//
// Configuration via environment variables:
//   THREADS_ACCOUNT_1_SESSIONID, THREADS_ACCOUNT_1_CSRFTOKEN, THREADS_ACCOUNT_1_USERID
//   THREADS_ACCOUNT_2_SESSIONID, ...
//   ... up to whatever number is configured
//
// On boot, we scan for THREADS_ACCOUNT_<N>_SESSIONID env vars and build the pool.
// During scraping, we round-robin through accounts. If one returns auth errors,
// we mark it as banned and skip it.

var accounts = [];
var nextIndex = 0;

function loadAccounts() {
  accounts = [];
  // Scan up to 20 accounts
  for (var i = 1; i <= 20; i++) {
    var sid = process.env['THREADS_ACCOUNT_' + i + '_SESSIONID'];
    var csrf = process.env['THREADS_ACCOUNT_' + i + '_CSRFTOKEN'];
    var uid = process.env['THREADS_ACCOUNT_' + i + '_USERID'];
    if (sid && csrf && uid) {
      accounts.push({
        index: i,
        sessionid: sid,
        csrftoken: csrf,
        userid: uid,
        status: 'active', // active | banned | rate_limited
        bannedAt: null,
        lastError: null,
        successCount: 0,
        errorCount: 0,
        lastUsedAt: null,
      });
    }
  }
  console.log('[ThreadsAccounts] Loaded ' + accounts.length + ' account(s) from env');
  return accounts.length;
}

// Return the next active account (round-robin), or null if all are banned.
function getNextAccount() {
  if (accounts.length === 0) return null;
  var attempts = 0;
  while (attempts < accounts.length) {
    var acc = accounts[nextIndex % accounts.length];
    nextIndex++;
    attempts++;
    if (acc.status === 'active') {
      acc.lastUsedAt = new Date();
      return acc;
    }
    // Re-activate rate_limited accounts after 1h
    if (acc.status === 'rate_limited' && acc.bannedAt && (Date.now() - acc.bannedAt.getTime()) > 3600000) {
      acc.status = 'active';
      acc.lastUsedAt = new Date();
      console.log('[ThreadsAccounts] Account #' + acc.index + ' reactivated after rate-limit cooldown');
      return acc;
    }
  }
  return null;
}

function markAccountSuccess(account) {
  if (!account) return;
  account.successCount++;
  account.lastError = null;
}

function markAccountError(account, errorType, message) {
  if (!account) return;
  account.errorCount++;
  account.lastError = message;
  if (errorType === 'banned' || errorType === 'auth_failed') {
    account.status = 'banned';
    account.bannedAt = new Date();
    console.log('[ThreadsAccounts] Account #' + account.index + ' marked as BANNED: ' + message);
  } else if (errorType === 'rate_limited') {
    account.status = 'rate_limited';
    account.bannedAt = new Date();
    console.log('[ThreadsAccounts] Account #' + account.index + ' marked as RATE_LIMITED: ' + message);
  }
}

function getAccountsStatus() {
  return accounts.map(function(a) {
    return {
      index: a.index,
      status: a.status,
      successCount: a.successCount,
      errorCount: a.errorCount,
      lastError: a.lastError,
      bannedAt: a.bannedAt,
    };
  });
}

function getActiveCount() {
  return accounts.filter(function(a) { return a.status === 'active'; }).length;
}

module.exports = {
  loadAccounts: loadAccounts,
  getNextAccount: getNextAccount,
  markAccountSuccess: markAccountSuccess,
  markAccountError: markAccountError,
  getAccountsStatus: getAccountsStatus,
  getActiveCount: getActiveCount,
};
