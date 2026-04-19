const { REST, Routes } = require('discord.js');
require('dotenv').config();
var commands = require('./commands').commands;

var rest = new REST({ version: '10' }).setToken(process.env.DISCORD_TOKEN);

(async function() {
  try {
    console.log('Registering slash commands...');
    var body = commands.map(function(cmd) { return cmd.toJSON(); });
    await rest.put(Routes.applicationGuildCommands(process.env.DISCORD_CLIENT_ID, process.env.GUILD_ID), { body: body });
    console.log('Slash commands registered successfully!');
  } catch (err) {
    console.error('Failed to register commands:', err);
  }
})();
