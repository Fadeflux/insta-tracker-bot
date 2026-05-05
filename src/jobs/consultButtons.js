// VA consultation buttons
// ──────────────────────
// Posts a single "menu" message in each VA's ticket with two buttons:
//   - 📩 "Demander conseil sur un post"
//   - 📞 "Demander un appel prive"
//
// When the VA clicks, the bot reacts:
//   - For "conseil": prompts the VA to paste the post URL in the next message;
//     ties their next response to the manager + team leader pings
//   - For "appel": creates an immediate ping in the ticket asking the leadership
//     to schedule a call
//
// We don't expose a complex modal (Discord modals require slash commands or
// "show modal" which is more involved). The button → reply pattern is simpler
// and works fine for our use case.

var { ButtonBuilder, ActionRowBuilder, ButtonStyle } = require('discord.js');
var config = require('../../config');
var logger = require('../utils/logger');

// Custom IDs for the buttons (must be unique app-wide so we don't collide
// with future buttons).
var CUSTOM_ID_CONSEIL = 'consult:conseil';
var CUSTOM_ID_APPEL = 'consult:appel';

var discordClient = null;
function setDiscordClient(client) {
  discordClient = client;
  // Wire the interaction listener — extends the existing one in index.js
  // by adding button handling. The existing one only handles slash commands.
  client.on('interactionCreate', function(interaction) {
    handleInteraction(interaction).catch(function(e) {
      logger.warn('[Consult] handler failed: ' + e.message);
    });
  });
}

// Build the button row for a ticket message
function buildButtons() {
  var conseil = new ButtonBuilder()
    .setCustomId(CUSTOM_ID_CONSEIL)
    .setLabel('Demander conseil sur un post')
    .setEmoji('📩')
    .setStyle(ButtonStyle.Primary);
  var appel = new ButtonBuilder()
    .setCustomId(CUSTOM_ID_APPEL)
    .setLabel('Demander un appel prive')
    .setEmoji('📞')
    .setStyle(ButtonStyle.Secondary);
  return new ActionRowBuilder().addComponents(conseil, appel);
}

// Resolve the platform config for a given guild
function getPlatformForGuild(guildId) {
  var platforms = config.getActivePlatforms();
  for (var i = 0; i < platforms.length; i++) {
    if (platforms[i].guildId === guildId) return platforms[i];
  }
  return null;
}

// === Handle a button click ===
async function handleInteraction(interaction) {
  if (!interaction.isButton()) return; // ignore everything else (slash commands etc.)
  var customId = interaction.customId;
  if (customId !== CUSTOM_ID_CONSEIL && customId !== CUSTOM_ID_APPEL) return;

  var pc = getPlatformForGuild(interaction.guildId);
  if (!pc) {
    return interaction.reply({ content: 'Plateforme non reconnue, contacte un admin.', ephemeral: true });
  }

  var managerMention = pc.managerRoleId ? ('<@&' + pc.managerRoleId + '>') : '@manager';
  var teamLeaderMention = pc.teamLeaderRoleId ? (' <@&' + pc.teamLeaderRoleId + '>') : '';
  var leadership = managerMention + teamLeaderMention;
  var vaMention = '<@' + interaction.user.id + '>';

  if (customId === CUSTOM_ID_CONSEIL) {
    // Send a public message in the ticket pinging leadership + asking VA
    // to provide the post URL. We don't try to capture the URL — the VA
    // will just reply normally in the channel and the manager will see it.
    try {
      await interaction.reply({
        content:
          '📩 ' + leadership + ' — ' + vaMention + ' aimerait avoir un conseil sur un post.\n\n' +
          vaMention + ' partage le lien du post dans le canal et explique brievement ce qui te pose probleme.',
        allowedMentions: { parse: ['users', 'roles'] },
      });
      logger.info('[Consult] conseil request from ' + interaction.user.tag + ' in #' + (interaction.channel && interaction.channel.name));
    } catch (e) {
      logger.warn('[Consult] reply failed: ' + e.message);
    }
  } else if (customId === CUSTOM_ID_APPEL) {
    try {
      await interaction.reply({
        content:
          '📞 ' + leadership + ' — ' + vaMention + ' demande un **appel prive**.\n\n' +
          'Merci de proposer un creneau au plus vite.',
        allowedMentions: { parse: ['users', 'roles'] },
      });
      logger.info('[Consult] appel request from ' + interaction.user.tag + ' in #' + (interaction.channel && interaction.channel.name));
    } catch (e) {
      logger.warn('[Consult] reply failed: ' + e.message);
    }
  }
}

// === Helper: post the consultation menu message in a VA's ticket ===
// Called once per VA, ideally pinned for easy access. The message is
// idempotent — if there's already one in the channel, we don't re-post.
async function postConsultMenuInChannel(channel) {
  try {
    // Check if we already have a consultation menu message in this channel
    // (look at the most recent 50 messages from the bot for a CUSTOM_ID match)
    var existing = await channel.messages.fetch({ limit: 50 });
    var alreadyPosted = existing.some(function(m) {
      return m.author && m.author.id === discordClient.user.id &&
             m.components && m.components.length > 0 &&
             m.components[0].components.some(function(c) {
               return c.customId === CUSTOM_ID_CONSEIL || c.customId === CUSTOM_ID_APPEL;
             });
    });
    if (alreadyPosted) return false;

    var msg = await channel.send({
      content:
        '💬 **Besoin d\'aide ?**\n' +
        'Clique sur un bouton ci-dessous pour solliciter ton manager ou ton team leader. ' +
        'Ils recevront une notif et pourront te repondre dans ce ticket.',
      components: [buildButtons()],
    });
    // Try to pin so it's always accessible
    try { await msg.pin(); } catch (e) { /* permission missing, ok */ }
    return true;
  } catch (e) {
    logger.warn('[Consult] postConsultMenuInChannel failed for #' + (channel && channel.name) + ': ' + e.message);
    return false;
  }
}

// === Sweep all VA tickets and post the menu where missing ===
// Run this periodically (or via an admin button) to make sure every VA
// has the menu in their ticket. We identify "VA tickets" by their channel
// name matching a Discord username we know about.
async function postConsultMenuToAllVas(db) {
  if (!discordClient) return { posted: 0, skipped: 0 };
  // Get the list of VAs we know about (have at least one post tracked)
  var vas;
  try {
    vas = (await db.pool.query(
      "SELECT DISTINCT va_discord_id, MAX(va_name) AS va_name FROM posts " +
      "WHERE va_discord_id IS NOT NULL AND deleted_at IS NULL " +
      "GROUP BY va_discord_id"
    )).rows;
  } catch (e) {
    logger.warn('[Consult] could not list VAs: ' + e.message);
    return { posted: 0, skipped: 0 };
  }

  var platforms = config.getActivePlatforms();
  var guildOrder = [];
  ['instagram', 'geelark', 'twitter', 'threads'].forEach(function(plat) {
    var pc = platforms.find(function(p) { return p.name === plat; });
    if (pc && pc.guildId) guildOrder.push(pc);
  });

  var posted = 0, skipped = 0;
  for (var i = 0; i < vas.length; i++) {
    var va = vas[i];
    var foundChannel = null;
    for (var g = 0; g < guildOrder.length; g++) {
      try {
        var guild = await discordClient.guilds.fetch(guildOrder[g].guildId);
        // Try by va_name first
        var target = String(va.va_name || '').toLowerCase().trim();
        if (target.startsWith('@')) target = target.slice(1);
        var channel = null;
        if (target) {
          var all = await guild.channels.fetch();
          channel = all.find(function(ch) {
            return ch && ch.type === 0 && ch.name && ch.name.toLowerCase() === target;
          });
        }
        // Fallback to discord username
        if (!channel && va.va_discord_id) {
          try {
            var member = await guild.members.fetch(va.va_discord_id);
            if (member) {
              var uname = member.user.username.toLowerCase();
              channel = (await guild.channels.fetch()).find(function(ch) {
                return ch && ch.type === 0 && ch.name && ch.name.toLowerCase() === uname;
              });
            }
          } catch (e) {}
        }
        if (channel) { foundChannel = channel; break; }
      } catch (e) {}
    }
    if (!foundChannel) { skipped++; continue; }
    var didPost = await postConsultMenuInChannel(foundChannel);
    if (didPost) { posted++; } else { skipped++; }
    await new Promise(function(r) { setTimeout(r, 300); });
  }
  logger.info('[Consult] menu posted to ' + posted + ' tickets, ' + skipped + ' skipped');
  return { posted: posted, skipped: skipped };
}

module.exports = {
  setDiscordClient: setDiscordClient,
  postConsultMenuInChannel: postConsultMenuInChannel,
  postConsultMenuToAllVas: postConsultMenuToAllVas,
  CUSTOM_ID_CONSEIL: CUSTOM_ID_CONSEIL,
  CUSTOM_ID_APPEL: CUSTOM_ID_APPEL,
};
