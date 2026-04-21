const { REST, Routes } = require('discord.js');
require('dotenv').config();
var config = require('../../config');
var commands = require('./commands').commands;

var rest = new REST({ version: '10' }).setToken(process.env.DISCORD_TOKEN);

(async function() {
  try {
    var body = commands.map(function(cmd) { return cmd.toJSON(); });
    var guildIds = config.getAllGuildIds();

    if (guildIds.length === 0) {
      console.error('No guild IDs configured. Set GUILD_ID_INSTAGRAM and/or GUILD_ID_TWITTER.');
      process.exit(1);
    }

    for (var i = 0; i < guildIds.length; i++) {
      var guildId = guildIds[i];
      var platformInfo = config.getPlatformByGuild(guildId);
      var platformName = platformInfo ? platformInfo.name : 'unknown';
      console.log('Registering commands for ' + platformName + ' (guild ' + guildId + ')...');
      await rest.put(Routes.applicationGuildCommands(process.env.DISCORD_CLIENT_ID, guildId), { body: body });
      console.log('Commands registered for ' + platformName + '!');
    }

    console.log('All slash commands registered successfully!');
  } catch (err) {
    console.error('Failed to register commands:', err);
  }
})();
