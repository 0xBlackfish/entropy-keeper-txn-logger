#!/usr/bin/python3

from audioop import add
import os
import discord
from discord.ext import commands
import pandas as pd


TOKEN = os.getenv('DISCORD_TOKEN')
GUILD = os.getenv('DISCORD_GUILD')

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

    await ctx.channel.send("Your wallet has earned {:,.2f} entropy tokens as of {} 23:59:59 UTC".format(user_rewards, as_of_date))

@bot.command(name='stats')
async def show_keeper_stats(ctx, entropy_keeper_address):

    # get current keeper stats for a given keeper address
    df = pd.read_parquet('gs://entropy-rewards/cumulative/current-keeper-stats.parquet')
    address_df = df[df['entropy_keeper_address'] == entropy_keeper_address]


    # write message
    response = """

    **Stats for {}**\n
    **GENERAL**\n
    First Keeper Day: {}\n
    Number of Days Keeping: {}

    """.format(
        address_df['entropy_keeper_address'].iloc[0],
        address_df['first_keeper_day'].iloc[0],
        address_df['number_of_days_keeping'].iloc[0]
    )

    await ctx.channel.send(response)

bot.run(TOKEN)