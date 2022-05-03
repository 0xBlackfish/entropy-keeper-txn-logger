import os
import discord
from discord.ext import commands
import pandas as pd


TOKEN = os.getenv('DISCORD_TOKEN')

bot = commands.Bot(command_prefix="$")


@bot.event
async def on_ready():
    print(f'{bot.user.name} has connected to Discord!')

@bot.command(name='rewards')
async def show_keeper_rewards(ctx, entropy_keeper_address):
	
    # get current cumulative rewards for the entropy keepers program
    df = pd.read_parquet('gs://entropy-rewards/cumulative/current-total-rewards.parquet')

    # extract variables to return in the bot message
    user_rewards = df[df['entropy_keeper_address'] == entropy_keeper_address].iloc[0]['total_keeper_reward']
    as_of_date = df[df['entropy_keeper_address'] == entropy_keeper_address].iloc[0]['as_of_date']

    await ctx.channel.send("Your wallet has earned {:,.2f} entropy token as of {}".format(user_rewards, as_of_date))

bot.run(TOKEN)