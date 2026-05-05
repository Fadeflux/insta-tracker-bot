// Daily motivational quotes
// ────────────────────────
// Returns a "quote of the day" in French, deterministically chosen so all
// VAs receive the SAME quote on the same day (creates a sense of unity in
// the agency).
//
// We use day-of-year modulo the quote count to rotate through the list. On
// Jan 1 = quote 0, Jan 2 = quote 1, etc. With ~30 quotes in the pool, each
// quote is shown roughly once per month.

// ─────────────────────────────────────────────────────────────────────────────
// Quote pool. Each entry: text + author. Mix of philosophical, business,
// sports, and pop-culture sources. All in French. Kept short so the morning
// notification doesn't get bloated.
// ─────────────────────────────────────────────────────────────────────────────
var QUOTES = [
  { text: 'Le succes c\'est tomber 7 fois et se relever 8.', author: 'Proverbe japonais' },
  { text: 'Le seul endroit ou le succes vient avant le travail, c\'est dans le dictionnaire.', author: 'Vidal Sassoon' },
  { text: 'Soit tu trouves un moyen, soit tu trouves une excuse.', author: 'Jim Rohn' },
  { text: 'La discipline, c\'est choisir entre ce que tu veux maintenant et ce que tu veux le plus.', author: 'Abraham Lincoln' },
  { text: 'Tu n\'es pas en retard, ni en avance. Tu es exactement la ou tu dois etre.', author: 'Inconnu' },
  { text: 'L\'echec n\'est pas l\'oppose du succes, c\'est une etape vers le succes.', author: 'Arianna Huffington' },
  { text: 'Crois en toi et tu seras a moitie arrive.', author: 'Theodore Roosevelt' },
  { text: 'Les reves ne fonctionnent que si tu les fais fonctionner.', author: 'John C. Maxwell' },
  { text: 'Une seule personne motivee vaut mieux que cent personnes interessees.', author: 'Inconnu' },
  { text: 'Si ca ne te challenge pas, ca ne te change pas.', author: 'Fred DeVito' },
  { text: 'La meilleure facon de predire l\'avenir, c\'est de le creer.', author: 'Peter Drucker' },
  { text: 'Travaille dur en silence, laisse ton succes faire le bruit.', author: 'Frank Ocean' },
  { text: 'Tu rates 100% des tirs que tu ne tentes pas.', author: 'Wayne Gretzky' },
  { text: 'Le talent fait gagner des matchs, mais le travail d\'equipe fait gagner des championnats.', author: 'Michael Jordan' },
  { text: 'Ne compte pas les jours, fais que les jours comptent.', author: 'Mohamed Ali' },
  { text: 'Sois plus fort que ton excuse la plus forte.', author: 'Inconnu' },
  { text: 'La constance bat le talent quand le talent ne reste pas constant.', author: 'Inconnu' },
  { text: 'Chaque pro etait un debutant. Chaque expert etait un apprenti.', author: 'Robin Sharma' },
  { text: 'Si tu attends d\'etre pret, tu attendras toute ta vie.', author: 'Lemony Snicket' },
  { text: 'Les opportunites ne se presentent pas, elles se creent.', author: 'Chris Grosser' },
  { text: 'Ce qui compte, ce n\'est pas combien de fois tu tombes, c\'est combien de fois tu te releves.', author: 'Vince Lombardi' },
  { text: 'Aujourd\'hui est le premier jour du reste de ta vie.', author: 'Charles Dederich' },
  { text: 'Si le plan A ne marche pas, l\'alphabet en a 25 autres.', author: 'Claire Cook' },
  { text: 'Fais aujourd\'hui ce que les autres ne veulent pas, vis demain ce que les autres ne pourront pas.', author: 'Jerry Rice' },
  { text: 'L\'energie et la persistance conquierent toutes choses.', author: 'Benjamin Franklin' },
  { text: 'Concentre-toi sur le progres, pas sur la perfection.', author: 'Inconnu' },
  { text: 'Tu ne perds jamais. Soit tu gagnes, soit tu apprends.', author: 'Nelson Mandela' },
  { text: 'Le seul moyen de faire du bon travail, c\'est d\'aimer ce que tu fais.', author: 'Steve Jobs' },
  { text: 'La motivation te lance, mais l\'habitude te fait avancer.', author: 'Jim Ryun' },
  { text: 'Reve grand et ose echouer.', author: 'Norman Vaughan' },
];

// Get the quote of the day for a given Date (defaults to now in Bénin TZ).
// Deterministic: same day across servers/restarts gives the same result.
function getQuoteOfTheDay(date) {
  var d = date || new Date();
  // Use the day-of-year (1..366) for the rotation seed. We compute it in UTC
  // to avoid TZ ambiguity — what matters is consistency across instances.
  var start = new Date(Date.UTC(d.getUTCFullYear(), 0, 0));
  var diff = d.getTime() - start.getTime();
  var dayOfYear = Math.floor(diff / 86400000);
  // Combine with year so the same day in different years gets a different quote
  // (otherwise the same quote falls on the same calendar day every year)
  var seed = (dayOfYear + d.getUTCFullYear() * 13) % QUOTES.length;
  if (seed < 0) seed += QUOTES.length;
  return QUOTES[seed];
}

// Format the quote for inclusion in a Discord message
function formatQuote(quote) {
  if (!quote) return '';
  return '💬 *« ' + quote.text + ' »* — ' + quote.author;
}

module.exports = {
  getQuoteOfTheDay: getQuoteOfTheDay,
  formatQuote: formatQuote,
  // Exposed for testing / display
  QUOTES: QUOTES,
};
