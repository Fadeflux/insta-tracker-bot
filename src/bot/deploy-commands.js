const { REST, Routes } = require('discord.js');
require('dotenv').config();
const { commands } = require('./commands');

const rest = new REST({ version: '10' }).setToken(process.env.DISCORD_TOKEN);

(async () => {
  try {
    console.log('Registering slash commands...');
    const body = commands.map((cmd) => cmd.toJSON());
    await rest.put(Routes.applicationGuildCommands(process.env.DISCORD_CLIENT_ID, process.env.GUILD_ID), { body });
    console.log('Slash commands registered successfully!');
  } catch (err) {
    console.error('Failed to register commands:', err);
  }
})();
