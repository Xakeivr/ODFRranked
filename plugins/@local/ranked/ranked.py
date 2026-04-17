import discord
from discord.ext import commands, tasks
from discord.ui import Button, View, Modal, TextInput
import os
import hashlib
from motor.motor_asyncio import AsyncIOMotorClient
import uuid
from collections import Counter
from datetime import timedelta, timezone, datetime
import math
import re
import io
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import random

client = AsyncIOMotorClient(os.getenv("CONNECTION_URI"))
db = client["ranked_system"]
players_col = db["players"]
matches_col = db["matches"]
config_col = db["config"]
ranked_reports_col = db["ranked_reports"]
ranked_bans_col = db["ranked_bans"]
parties_col = db["ranked_parties"]
ranked_warns_col = db["ranked_warns"]
ranked_seasons_col = db["ranked_seasons"]
season_profiles_col = db["season_profiles"]
season_archives_col = db["season_archives"]
teams_col = db["ranked_teams"]

DRIFTERS_BOT_ID = 1482567264732577792

RANKS = [
    ("Bronze", 0, 199),
    ("Silver", 200, 399),
    ("Gold", 400, 699),
    ("Diamond", 700, 999),
    ("Advanced", 1000, 1399),
    ("Champion", 1400, 1799),
    ("Legend", 1800, 2199),
    ("Master", 2200, None),
]

RANK_EMOJIS = {
    "Bronze": "<:bronze:1488647064295440514>",
    "Silver": "<:silver:1488647187037425826>",
    "Gold": "<:gold:1488647357791862824>",
    "Diamond": "<:diamond:1488647555234660572>",
    "Advanced": "<:advanced:1488647908848042034>",
    "Champion": "<:champion:1488648494741979236>",
    "Legend": "<:legend:1488648821721530620>",
    "Master": "<:master:1488649548888014864>",
}

SEASON_DEFAULT_NUMBER = 0

SEASON_QUEST_TEMPLATES = {
    "daily": [
        {"id": "daily_play_1", "label": "Play 1 ranked match", "target": 1, "metric": "matches_played", "reward_xp": 25},
        {"id": "daily_win_1", "label": "Win 1 ranked match", "target": 1, "metric": "wins", "reward_xp": 35},
    ],
    "weekly": [
        {"id": "weekly_play_5", "label": "Play 5 ranked matches", "target": 5, "metric": "matches_played", "reward_xp": 100},
        {"id": "weekly_win_3", "label": "Win 3 ranked matches", "target": 3, "metric": "wins", "reward_xp": 125},
    ],
    "monthly": [
        {"id": "monthly_play_20", "label": "Play 20 ranked matches", "target": 20, "metric": "matches_played", "reward_xp": 300},
        {"id": "monthly_win_10", "label": "Win 10 ranked matches", "target": 10, "metric": "wins", "reward_xp": 400},
    ],
    "seasonal": [
        {"id": "season_play_50", "label": "Play 50 ranked matches", "target": 50, "metric": "matches_played", "reward_xp": 800},
        {"id": "season_win_25", "label": "Win 25 ranked matches", "target": 25, "metric": "wins", "reward_xp": 1000},
        {"id": "season_elo_500", "label": "Reach 500 season MMR", "target": 500, "metric": "peak_elo", "reward_xp": 1200},
    ],
}

SEASON_REWARD_TRACK = {
    1: {"xp_bonus": 0, "label": "Season Start"},
    5: {"xp_bonus": 50, "label": "Small XP Milestone"},
    10: {"xp_bonus": 100, "label": "Season Progress Milestone"},
    15: {"xp_bonus": 150, "label": "Mid-Season Milestone"},
    20: {"xp_bonus": 250, "label": "High Progress Milestone"},
    25: {"xp_bonus": 400, "label": "Top Progress Milestone"},
}

TEAM_QUEST_TEMPLATES = {
    "weekly": [
        {"id": "team_weekly_play_5", "label": "Team plays 5 ranked matches", "target": 5, "metric": "matches", "reward_xp": 150},
        {"id": "team_weekly_win_3", "label": "Team wins 3 ranked matches", "target": 3, "metric": "wins", "reward_xp": 200},
    ],
    "seasonal": [
        {"id": "team_season_play_25", "label": "Team plays 25 ranked matches", "target": 25, "metric": "matches", "reward_xp": 600},
        {"id": "team_season_win_15", "label": "Team wins 15 ranked matches", "target": 15, "metric": "wins", "reward_xp": 900},
        {"id": "team_season_points_250", "label": "Team reaches 250 points", "target": 250, "metric": "points", "reward_xp": 1200},
    ]
}

def get_rank_info(elo: int):
    for index, (rank_name, min_elo, max_elo) in enumerate(RANKS):
        if max_elo is None:
            if elo >= min_elo:
                return {
                    "name": rank_name,
                    "min_elo": min_elo,
                    "max_elo": max_elo,
                    "next_rank": None,
                    "next_rank_elo": None,
                    "index": index,
                }
        elif min_elo <= elo <= max_elo:
            next_rank_name = RANKS[index + 1][0] if index + 1 < len(RANKS) else None
            next_rank_elo = RANKS[index + 1][1] if index + 1 < len(RANKS) else None
            return {
                "name": rank_name,
                "min_elo": min_elo,
                "max_elo": max_elo,
                "next_rank": next_rank_name,
                "next_rank_elo": next_rank_elo,
                "index": index,
            }

    return {
        "name": "Bronze",
        "min_elo": 0,
        "max_elo": 199,
        "next_rank": "Silver",
        "next_rank_elo": 200,
        "index": 0,
    }

def get_rank_emoji(rank_name: str) -> str:
    return RANK_EMOJIS.get(rank_name, "🏅")

def format_rank_display(rank_name: str) -> str:
    return f"{get_rank_emoji(rank_name)} **{rank_name}**"

def format_rank_progress(elo: int) -> str:
    rank_info = get_rank_info(elo)

    if rank_info["next_rank_elo"] is None:
        return f"`{elo}` MMR • {get_rank_emoji(rank_info['name'])} max rank"

    return (
        f"`{elo}/{rank_info['next_rank_elo']}` MMR • "
        f"next: {get_rank_emoji(rank_info['next_rank'])} **{rank_info['next_rank']}**"
    )

def smoothstep(t: float) -> float:
    t = max(0.0, min(1.0, t))
    return t * t * (3.0 - 2.0 * t)

def blend(x: float, x0: float, y0: float, x1: float, y1: float) -> float:
    if x1 == x0:
        return y1
    t = (x - x0) / (x1 - x0)
    s = smoothstep(t)
    return y0 + (y1 - y0) * s

def expected_score(player_elo: int, enemy_avg_elo: int) -> float:
    diff = enemy_avg_elo - player_elo

    if diff <= -1800:
        return 0.9
    elif diff <= -1200:
        return blend(diff, -1800, 0.9, -1200, 0.75)
    elif diff <= -550:
        return blend(diff, -1200, 0.75, -550, 0.6)
    elif diff <= 550:
        return blend(diff, -550, 0.6, 550, 0.5)
    elif diff <= 1000:
        return blend(diff, 550, 0.5, 1000, 0.12)
    elif diff <= 1600:
        return blend(diff, 1000, 0.12, 1600, 0.01)
    else:
        return 0.0


def get_score_margin_multiplier(score_diff: int) -> float:
    """
    Score difference impact:
    1 -> 1.00
    2 -> 1.20
    3 -> 1.40
    4 -> 1.60
    5 -> 1.80
    6+ -> 2.00
    """
    if score_diff <= 1:
        return 1.00
    if score_diff == 2:
        return 1.2
    if score_diff == 3:
        return 1.4
    if score_diff == 4:
        return 1.6
    if score_diff == 5:
        return 1.8
    return 2


def get_upset_bonus_multiplier(player_elo: int, enemy_avg_elo: int, won: bool) -> float:
    """
    Lower-Elo wins against much higher opponents get a mild bonus.
    Higher-Elo losses to much lower opponents get punished gradually,
    starting only after 300 MMR gap.
    """
    gap = enemy_avg_elo - player_elo

    # LOWER-ELO PLAYER WON
    if won:
        if gap >= 1400:
            return 1.22
        if gap >= 1000:
            return 1.14
        if gap >= 800:
            return 1.10
        if gap >= 500:
            return 1.05
        return 1.00

    # HIGHER-ELO PLAYER LOST
    favorite_gap = player_elo - enemy_avg_elo

    if favorite_gap < 500:
        return 1.00
    if favorite_gap < 800:
        return 1.02
    if favorite_gap < 1000:
        return 1.05
    if favorite_gap < 1400:
        return 1.10
    return 1.12


def get_favorite_win_reduction_multiplier(player_elo: int, enemy_avg_elo: int, won: bool) -> float:
    """
    Higher-Elo players should still gain less when beating much lower-Elo opponents,
    but not so little that it feels dead.
    """
    gap = player_elo - enemy_avg_elo

    if not won:
        return 1.00

    if gap >= 1950:
        return 0.75
    if gap >= 1350:
        return 0.85
    if gap >= 950:
        return 0.90
    if gap >= 400:
        return 0.95
    return 1.00


def calculate_individual_elo_change(
    player_elo: int,
    enemy_avg_elo: int,
    won: bool,
    score_diff: int,
    k: int = 40
) -> int:
    expected = expected_score(player_elo, enemy_avg_elo)
    actual = 1 if won else 0

    base_change = k * (actual - expected)

    score_multiplier = get_score_margin_multiplier(score_diff)
    upset_multiplier = get_upset_bonus_multiplier(player_elo, enemy_avg_elo, won)
    favorite_multiplier = get_favorite_win_reduction_multiplier(player_elo, enemy_avg_elo, won)

    change = base_change * score_multiplier * upset_multiplier * favorite_multiplier
    change = round(change)

    # Winners must always gain at least 1
    if won and change < 1:
        change = 1

    return change

def calculate_placement_seed(player: dict, placement_games: int = 5) -> int:
    placements = player.get("placements", {})
    wins = placements.get("wins", 0)

    placement_games = max(1, placement_games)
    progress_ratio = max(0.0, min(1.0, wins / placement_games))

    # Linear seed from 0 to 500 based on placement win ratio
    return round(progress_ratio * 400)

def get_match_status_data(match: dict):
    if match.get("status") == "finished":
        return {
            "emoji": "🔴",
            "label": "Ended"
        }

    if match.get("status") == "cancelled":
        return {
            "emoji": "⚫",
            "label": "Cancelled"
        }

    if match.get("score_submission") is not None:
        return {
            "emoji": "🟡",
            "label": "Score Submitted"
        }

    return {
        "emoji": "🟢",
        "label": "Active"
    }

def parse_ranked_duration(duration: str):
    units = {
        "s": 1,
        "m": 60,
        "h": 3600,
        "d": 86400,
        "w": 604800,
        "mo": 2592000
    }

    pattern = r"(\d+)(mo|[smhdw])"
    matches = re.findall(pattern, duration.lower())

    if not matches:
        return None

    total_seconds = 0

    for amount, unit in matches:
        if unit not in units:
            return None
        total_seconds += int(amount) * units[unit]

    return total_seconds


def format_ranked_duration(seconds: int):
    if seconds <= 0:
        return "0 seconds"

    periods = [
        ("month", 2592000),
        ("week", 604800),
        ("day", 86400),
        ("hour", 3600),
        ("minute", 60),
        ("second", 1)
    ]

    result = []

    for name, sec in periods:
        amount = seconds // sec
        if amount > 0:
            suffix = "" if amount == 1 else "s"
            result.append(f"{amount} {name}{suffix}")
            seconds -= amount * sec

    return " ".join(result)

class SeasonDangerConfirmView(View):
    def __init__(self, cog, action: str, guild_id: int, actor_id: int, payload: dict):
        super().__init__(timeout=180)
        self.cog = cog
        self.action = action
        self.guild_id = guild_id
        self.actor_id = actor_id
        self.payload = payload

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.green)
    async def confirm_action(self, interaction: discord.Interaction, button: Button):
        if interaction.user.id != self.actor_id:
            return await interaction.response.send_message(
                "Only the admin who triggered this confirmation can use it.",
                ephemeral=True
            )

        guild = interaction.guild
        ok, message = await self.cog.execute_season_danger_action(
            guild,
            self.action,
            self.actor_id,
            self.payload
        )

        await interaction.response.edit_message(
            content=message,
            view=None,
            embed=None
        )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.red)
    async def cancel_action(self, interaction: discord.Interaction, button: Button):
        if interaction.user.id != self.actor_id:
            return await interaction.response.send_message(
                "Only the admin who triggered this confirmation can use it.",
                ephemeral=True
            )

        await interaction.response.edit_message(
            content="Season action cancelled.",
            view=None,
            embed=None
        )

class QueueView(View):
    def __init__(self, cog, mode):
        super().__init__(timeout=None)
        self.cog = cog
        self.mode = mode

    @discord.ui.button(
        label="Join Queue",
        style=discord.ButtonStyle.green,
        custom_id="ranked:queue:join"
    )
    async def join(self, interaction: discord.Interaction, button: Button):
        await self.cog.join_queue(interaction, self.mode)
    
    @discord.ui.button(
        label="Leave Queue",
        style=discord.ButtonStyle.red,
        custom_id="ranked:queue:leave"
    )
    async def leave(self, interaction: discord.Interaction, button: Button):
        await self.cog.leave_queue(interaction, self.mode)


class ResultView(View):
    def __init__(self, cog, players, mode, match_id):
        super().__init__(timeout=None)
        self.mode = mode
        self.match_id = match_id
        self.cog = cog
        self.players = players
        self.cancel_match_button.label = "Cancel Match 0/0"

    @discord.ui.button(
        label="Submit Score",
        style=discord.ButtonStyle.blurple,
        custom_id="ranked:match:submit"
    )
    async def submit_score_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user.id not in self.players:
            return await interaction.response.send_message(
                "You are not part of this match.",
                ephemeral=True
            )

        await interaction.response.send_modal(ScoreSubmissionModal(self))

    @discord.ui.button(
        label="Report Match",
        style=discord.ButtonStyle.red,
        custom_id="ranked:match:report"
    )
    async def report_match_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user.id not in self.players:
            return await interaction.response.send_message(
                "You are not part of this match.",
                ephemeral=True
            )

        await interaction.response.send_modal(MatchReportModal(self.cog, self.match_id))

    @discord.ui.button(
        label="Cancel Match",
        style=discord.ButtonStyle.secondary,
        custom_id="ranked:match:cancelmatch"
    )
    async def cancel_match_button(self, interaction: discord.Interaction, button: Button):
        match = await matches_col.find_one({"_id": self.match_id})
        if not match:
            return await interaction.response.send_message("Match not found.", ephemeral=True)

        ok, message, data = await self.cog.process_cancel_match_vote(
            interaction.guild,
            match,
            interaction.user.id
        )

        if not ok:
            return await interaction.response.send_message(message, ephemeral=True)

        if data:
            # update button label
            self.update_cancel_label(data["votes"], data["required"])

        if data and data.get("cancelled"):
            try:
                await interaction.message.edit(view=None)
            except discord.HTTPException:
                pass
        else:
            try:
                await interaction.message.edit(view=self)
            except discord.HTTPException:
                pass

        await interaction.response.send_message(message, ephemeral=True)

    def update_cancel_label(self, votes: int, required: int):
        self.cancel_match_button.label = f"Cancel Match {votes}/{required}"

    async def submit_score(self, interaction, your_score, enemy_score):
        match = await matches_col.find_one({"_id": self.match_id})
        if not match:
            return await interaction.followup.send("Match not found in database.", ephemeral=True)

        if match.get("score_submission") is not None:
            return await interaction.followup.send(
                "A score is already waiting for confirmation.",
                ephemeral=True
            )

        team1 = match["teams"]["team1"]
        team2 = match["teams"]["team2"]

        if interaction.user.id in team1:
            submitting_team = "team1"
            target_team = "team2"
        elif interaction.user.id in team2:
            submitting_team = "team2"
            target_team = "team1"
        else:
            return await interaction.followup.send("You are not part of this match.", ephemeral=True)

        submission = {
            "submitted_by": interaction.user.id,
            "submitting_team": submitting_team,
            "target_team": target_team,
            "team1_score": your_score if submitting_team == "team1" else enemy_score,
            "team2_score": enemy_score if submitting_team == "team1" else your_score
        }

        await matches_col.update_one(
            {"_id": self.match_id},
            {
                "$set": {
                    "score_submission": submission,
                    "score_confirmation": {
                        "target_team": target_team,
                        "confirmations": [],
                        "declines": []
                    }
                }
            }
        )

        result_message_id = match.get("result_message_id")
        if result_message_id:
            try:
                result_message = await interaction.channel.fetch_message(result_message_id)
                await result_message.edit(view=None)
            except discord.NotFound:
                pass
            except discord.HTTPException:
                pass

        target_members = team2 if target_team == "team2" else team1
        target_mentions = " ".join(f"<@{user_id}>" for user_id in target_members)
        target_team_label = "Team B" if target_team == "team2" else "Team A"

        await interaction.followup.send(
            "Score submitted and sent for confirmation.",
            ephemeral=True
        )

        confirmation_embed = discord.Embed(
            title="📨 Score Submitted",
            description=f"Submitted by <@{interaction.user.id}>",
            color=discord.Color.blurple()
        )
        confirmation_embed.add_field(name="Team A Score", value=f"`{submission['team1_score']}`", inline=True)
        confirmation_embed.add_field(name="Team B Score", value=f"`{submission['team2_score']}`", inline=True)
        confirmation_embed.add_field(name="Confirmation Team", value=target_team_label, inline=False)
        confirmation_embed.add_field(
            name="Players to Confirm",
            value=target_mentions if target_mentions else "No players found.",
            inline=False
        )
        confirmation_embed.set_footer(text="Use the buttons below to confirm or decline this score.")

        confirmation_message = await interaction.channel.send(
            embed=confirmation_embed,
            view=ScoreConfirmationView(self.cog, self.match_id, required_votes=len(target_members))
        )

        await matches_col.update_one(
            {"_id": self.match_id},
            {
                "$set": {
                    "confirmation_message_id": confirmation_message.id
                }
            }
        )
        await self.cog.update_match_visuals(interaction.guild, self.match_id)

class ScoreSubmissionModal(Modal, title="Submit Match Score"):
    def __init__(self, result_view):
        super().__init__()
        self.result_view = result_view

        self.your_score = TextInput(
            label="Your team's score",
            placeholder="Example: 5",
            required=True,
            max_length=3
        )
        self.enemy_score = TextInput(
            label="Opponent team's score",
            placeholder="Example: 3",
            required=True,
            max_length=3
        )

        self.add_item(self.your_score)
        self.add_item(self.enemy_score)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            your_score = int(str(self.your_score.value).strip())
            enemy_score = int(str(self.enemy_score.value).strip())
        except ValueError:
            return await interaction.response.send_message(
                "Scores must be numbers.",
                ephemeral=True
            )

        if abs(your_score - enemy_score) > 6:
            return await interaction.response.send_message(
                "The maximum allowed score gap is 6.",
                ephemeral=True
            )

        if your_score == enemy_score:
            return await interaction.response.send_message(
                "Scores cannot be tied.",
                ephemeral=True
            )
        
        await interaction.response.defer(ephemeral=True)
        await self.result_view.submit_score(interaction, your_score, enemy_score)

class MatchReportModal(Modal, title="Report Match"):
    def __init__(self, cog, match_id: str):
        super().__init__()
        self.cog = cog
        self.match_id = match_id

        self.reason = TextInput(
            label="Reason",
            placeholder="Explain the issue clearly...",
            required=True,
            max_length=500,
            style=discord.TextStyle.paragraph
        )

        self.add_item(self.reason)

    async def on_submit(self, interaction: discord.Interaction):
        match = await matches_col.find_one({"_id": self.match_id})
        if not match:
            return await interaction.response.send_message("Match not found.", ephemeral=True)

        if interaction.user.id not in match.get("players", []):
            return await interaction.response.send_message("You are not part of this match.", ephemeral=True)

        await self.cog.submit_match_report(
            interaction.guild,
            match,
            interaction.user.id,
            str(self.reason.value).strip(),
            interaction.channel if isinstance(interaction.channel, discord.Thread) else None
        )

        await interaction.response.send_message("Your report has been sent to staff.", ephemeral=True)

class ScoreConfirmationView(View):
    def __init__(self, cog, match_id, required_votes=0):
        super().__init__(timeout=None)
        self.cog = cog
        self.match_id = match_id

        self.confirm_score.label = f"Confirm 0/{required_votes}"
        self.decline_score.label = f"Decline 0/{required_votes}"

    @discord.ui.button(
        label="Confirm 0/0",
        style=discord.ButtonStyle.green,
        custom_id="ranked:confirm"
    )
    async def confirm_score(self, interaction: discord.Interaction, button: Button):
        await self.handle_vote(interaction, "confirm", button)

    @discord.ui.button(
        label="Decline 0/0",
        style=discord.ButtonStyle.red,
        custom_id="ranked:decline"
    )
    async def decline_score(self, interaction: discord.Interaction, button: Button):
        await self.handle_vote(interaction, "decline", button)

    async def handle_vote(self, interaction: discord.Interaction, vote_type: str, button: Button):
        match = await matches_col.find_one({"_id": self.match_id})
        if not match:
            return await interaction.response.send_message("Match not found.", ephemeral=True)

        if match.get("status") != "ongoing":
            return await interaction.response.send_message("This match is already finished.", ephemeral=True)

        submission = match.get("score_submission")
        if not submission:
            return await interaction.response.send_message("No score is waiting for confirmation.", ephemeral=True)

        confirmation = match.get("score_confirmation", {})
        target_team = confirmation.get("target_team")

        team1 = match["teams"]["team1"]
        team2 = match["teams"]["team2"]

        if target_team == "team1":
            allowed_team = team1
        elif target_team == "team2":
            allowed_team = team2
        else:
            return await interaction.response.send_message("Invalid confirmation state.", ephemeral=True)

        if interaction.user.id not in allowed_team:
            return await interaction.response.send_message(
                "Only the opposing team can vote on this score.",
                ephemeral=True
            )

        updated_match = await self.cog.apply_score_confirmation_vote(self.match_id, interaction.user.id, vote_type)
        if not updated_match:
            return await interaction.response.send_message(
                "The match state changed before your vote could be recorded.",
                ephemeral=True
            )

        if updated_match.get("status") != "ongoing":
            return await interaction.response.send_message("This match is already finished.", ephemeral=True)

        updated_submission = updated_match.get("score_submission")
        if not updated_submission:
            return await interaction.response.send_message("No score is waiting for confirmation.", ephemeral=True)

        updated_confirmation = updated_match.get("score_confirmation", {})
        confirmations = list(updated_confirmation.get("confirmations", []))
        declines = list(updated_confirmation.get("declines", []))

        required_votes = len(allowed_team)

        self.confirm_score.label = f"Confirm {len(confirmations)}/{required_votes}"
        self.decline_score.label = f"Decline {len(declines)}/{required_votes}"

        total_votes = len(confirmations) + len(declines)

        if len(confirmations) >= required_votes:
            await interaction.response.edit_message(view=None)
            await self.cog.finalize_confirmed_score(interaction.guild, self.match_id, interaction.channel)
            self.stop()
            return

        if len(declines) >= required_votes:
            await interaction.response.edit_message(view=None)

            await self.cog.reset_match_score_state(interaction.guild, self.match_id)

            declined_embed = discord.Embed(
                title="❌ Score Declined",
                description="The submitted score was declined by the opposing team.",
                color=discord.Color.red()
            )
            declined_embed.add_field(name="Next Step", value="Please submit a new score.", inline=False)

            await interaction.channel.send(embed=declined_embed)
            self.stop()
            return
            

        if total_votes >= required_votes:
            await matches_col.update_one(
                {"_id": self.match_id},
                {
                    "$set": {
                        "score_confirmation.confirmations": [],
                        "score_confirmation.declines": []
                    }
                }
            )

            self.confirm_score.label = f"Confirm 0/{required_votes}"
            self.decline_score.label = f"Decline 0/{required_votes}"

            await interaction.response.edit_message(view=self)
            split_embed = discord.Embed(
                title="⚠️ Votes Split",
                description="The team vote was not unanimous.",
                color=discord.Color.orange()
            )
            split_embed.add_field(
                name="Result",
                value="The confirmation vote has been reset. Please vote again.",
                inline=False
            )

            await interaction.channel.send(embed=split_embed)
            return

        await interaction.response.edit_message(view=self)

class PartyInviteView(View):
    def __init__(self, cog, invite_id: str):
        super().__init__(timeout=None)
        self.cog = cog
        self.invite_id = invite_id

    @discord.ui.button(
        label="Accept Party Invite",
        style=discord.ButtonStyle.green,
        custom_id="ranked:party:accept"
    )
    async def accept_invite(self, interaction: discord.Interaction, button: Button):
        ok, message = await self.cog.accept_party_invite(interaction.guild, interaction.user.id, self.invite_id)
        await interaction.response.send_message(message, ephemeral=True)

        if ok:
            try:
                await interaction.message.edit(view=None)
            except discord.HTTPException:
                pass

    @discord.ui.button(
        label="Decline Party Invite",
        style=discord.ButtonStyle.red,
        custom_id="ranked:party:decline"
    )
    async def decline_invite(self, interaction: discord.Interaction, button: Button):
        ok, message = await self.cog.decline_party_invite(interaction.guild, interaction.user.id, self.invite_id)
        await interaction.response.send_message(message, ephemeral=True)

        if ok:
            try:
                await interaction.message.edit(view=None)
            except discord.HTTPException:
                pass

class TeamInviteView(View):
    def __init__(self, cog, invite_id: str):
        super().__init__(timeout=None)
        self.cog = cog
        self.invite_id = invite_id

    @discord.ui.button(
        label="Accept Team Invite",
        style=discord.ButtonStyle.green,
        custom_id="ranked:team:accept"
    )
    async def accept_invite(self, interaction: discord.Interaction, button: Button):
        ok, message = await self.cog.accept_team_invite(interaction.guild, interaction.user.id, self.invite_id)
        await interaction.response.send_message(message, ephemeral=True)

        if ok:
            try:
                await interaction.message.edit(view=None)
            except discord.HTTPException:
                pass

    @discord.ui.button(
        label="Decline Team Invite",
        style=discord.ButtonStyle.red,
        custom_id="ranked:team:decline"
    )
    async def decline_invite(self, interaction: discord.Interaction, button: Button):
        ok, message = await self.cog.decline_team_invite(interaction.guild, interaction.user.id, self.invite_id)
        await interaction.response.send_message(message, ephemeral=True)

        if ok:
            try:
                await interaction.message.edit(view=None)
            except discord.HTTPException:
                pass

class SimplePaginationView(View):
    def __init__(self, cog, builder, current_page: int, total_pages: int, *builder_args):
        super().__init__(timeout=180)
        self.cog = cog
        self.builder = builder
        self.builder_args = builder_args
        self.current_page = current_page
        self.total_pages = max(total_pages, 1)
        self.update_buttons()

    def update_buttons(self):
        is_first = self.current_page <= 1
        is_last = self.current_page >= self.total_pages

        self.first_page.disabled = is_first
        self.previous_page.disabled = is_first
        self.next_page.disabled = is_last
        self.last_page.disabled = is_last

    async def update_message(self, interaction: discord.Interaction, new_page: int):
        self.current_page = max(1, min(new_page, self.total_pages))
        self.update_buttons()

        embed = await self.builder(*self.builder_args, page=self.current_page)
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="⏮️ First", style=discord.ButtonStyle.secondary)
    async def first_page(self, interaction: discord.Interaction, button: Button):
        await self.update_message(interaction, 1)

    @discord.ui.button(label="◀️ Previous", style=discord.ButtonStyle.secondary)
    async def previous_page(self, interaction: discord.Interaction, button: Button):
        await self.update_message(interaction, self.current_page - 1)

    @discord.ui.button(label="Next ▶️", style=discord.ButtonStyle.secondary)
    async def next_page(self, interaction: discord.Interaction, button: Button):
        await self.update_message(interaction, self.current_page + 1)

    @discord.ui.button(label="Last ⏭️", style=discord.ButtonStyle.secondary)
    async def last_page(self, interaction: discord.Interaction, button: Button):
        await self.update_message(interaction, self.total_pages)

class LeaderboardPaginationView(View):
    def __init__(self, cog, mode: str, metric: str, current_page: int, total_pages: int):
        super().__init__(timeout=180)
        self.cog = cog
        self.mode = mode
        self.metric = metric
        self.current_page = current_page
        self.total_pages = max(total_pages, 1)

        self.update_buttons()

    def update_buttons(self):
        is_first = self.current_page <= 1
        is_last = self.current_page >= self.total_pages

        self.first_page.disabled = is_first
        self.previous_page.disabled = is_first
        self.next_page.disabled = is_last
        self.last_page.disabled = is_last

    async def update_message(self, interaction: discord.Interaction, new_page: int):
        self.current_page = max(1, min(new_page, self.total_pages))
        self.update_buttons()

        embed = await self.cog.build_mode_leaderboard_embed(
            self.mode,
            metric=self.metric,
            page=self.current_page,
            per_page=20
        )
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="⏮️ First", style=discord.ButtonStyle.secondary)
    async def first_page(self, interaction: discord.Interaction, button: Button):
        await self.update_message(interaction, 1)

    @discord.ui.button(label="◀️ Previous", style=discord.ButtonStyle.secondary)
    async def previous_page(self, interaction: discord.Interaction, button: Button):
        await self.update_message(interaction, self.current_page - 1)

    @discord.ui.button(label="Next ▶️", style=discord.ButtonStyle.secondary)
    async def next_page(self, interaction: discord.Interaction, button: Button):
        await self.update_message(interaction, self.current_page + 1)

    @discord.ui.button(label="Last ⏭️", style=discord.ButtonStyle.secondary)
    async def last_page(self, interaction: discord.Interaction, button: Button):
        await self.update_message(interaction, self.total_pages)

class Ranked(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.queues = {}
        self.parties = {}
        self.lb_cache = {}  # guild_id -> hash
        self._views_restored = False
        self.leaderboard_loop.start()
        self.party_cleanup_loop.start()

    def cog_unload(self):
        self.leaderboard_loop.cancel()
        self.party_cleanup_loop.cancel()

    def build_default_queue_state(self) -> dict:
        return {"1v1": [], "2v2": [], "3v3": [], "4v4": []}

    def get_guild_queue_state(self, guild_id: int) -> dict:
        guild_id = int(guild_id)
        state = self.queues.get(guild_id)
        if not isinstance(state, dict):
            state = self.build_default_queue_state()
            self.queues[guild_id] = state
        return state

    def normalize_queue_state(self, raw_state: dict | None) -> dict:
        state = self.build_default_queue_state()

        if not isinstance(raw_state, dict):
            return state

        for mode in state.keys():
            value = raw_state.get(mode, [])
            cleaned_entries = []

            if not isinstance(value, list):
                continue

            for entry in value:
                if isinstance(entry, int):
                    cleaned_entries.append({
                        "party_id": None,
                        "members": [entry]
                    })
                    continue

                if not isinstance(entry, dict):
                    continue

                members = entry.get("members", [])
                if not isinstance(members, list):
                    continue

                cleaned_members = []
                for user_id in members:
                    try:
                        cleaned_members.append(int(user_id))
                    except (TypeError, ValueError):
                        continue

                if not cleaned_members:
                    continue

                cleaned_entries.append({
                    "party_id": entry.get("party_id"),
                    "members": cleaned_members
                })

            state[mode] = cleaned_entries

        return state

    async def load_persistent_queue_state(self):
        self.queues = {}
        async for config in config_col.find({}):
            guild_id = config.get("_id")
            try:
                guild_id = int(guild_id)
            except (TypeError, ValueError):
                continue

            self.queues[guild_id] = self.normalize_queue_state(config.get("queue_state"))

    async def save_queue_state(self, guild_id: int):
        config = await self.get_config(guild_id)
        config["queue_state"] = self.normalize_queue_state(self.get_guild_queue_state(guild_id))
        await config_col.update_one({"_id": guild_id}, {"$set": {"queue_state": config["queue_state"]}})

    def get_party_member_set(self, party: dict) -> set[int]:
        return set(int(x) for x in party.get("members", []))

    def build_party_cache_key(self, guild_id: int, owner_id: int) -> str:
        return f"{guild_id}:{owner_id}"

    def normalize_party_doc(self, doc: dict | None) -> dict | None:
        if not isinstance(doc, dict):
            return None

        members = doc.get("members", [])
        if not isinstance(members, list):
            return None

        cleaned_members = []
        for user_id in members:
            try:
                cleaned_members.append(int(user_id))
            except (TypeError, ValueError):
                continue

        if not cleaned_members:
            return None

        invites = doc.get("invites", [])
        if not isinstance(invites, list):
            invites = []

        return {
            "_id": doc["_id"],
            "guild_id": int(doc["guild_id"]),
            "owner_id": int(doc["owner_id"]),
            "members": cleaned_members[:4],
            "created_at": doc.get("created_at"),
            "last_activity_at": doc.get("last_activity_at"),
            "invites": invites
        }

    async def load_persistent_parties(self):
        self.parties = {}

        async for doc in parties_col.find({}):
            normalized = self.normalize_party_doc(doc)
            if not normalized:
                continue

            key = self.build_party_cache_key(normalized["guild_id"], normalized["owner_id"])
            self.parties[key] = normalized

    def normalize_party_invites(self, party: dict) -> list[dict]:
        invites = party.get("invites")
        if not isinstance(invites, list):
            invites = []
            party["invites"] = invites
        return invites

    def create_party_invite_doc(self, party: dict, invited_user_id: int) -> dict:
        now = discord.utils.utcnow()
        return {
            "_id": uuid.uuid4().hex[:12],
            "party_id": party["_id"],
            "guild_id": party["guild_id"],
            "owner_id": party["owner_id"],
            "invited_user_id": invited_user_id,
            "created_at": now,
            "expires_at": now + timedelta(minutes=15),
            "status": "pending"
        }

    def normalize_invite_time(self, dt):
        if dt is None:
            return None
        if hasattr(dt, "tzinfo") and dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt

    def is_party_invite_expired(self, invite: dict) -> bool:
        expires_at = self.normalize_invite_time(invite.get("expires_at"))
        if expires_at is None:
            return True
        return expires_at <= discord.utils.utcnow()

    async def cleanup_expired_party_invites(self, party: dict):
        invites = self.normalize_party_invites(party)
        changed = False

        for invite in invites:
            if invite.get("status") == "pending" and self.is_party_invite_expired(invite):
                invite["status"] = "expired"
                changed = True

        if changed:
            await self.save_party_doc(party)

    async def get_pending_party_invite_for_user(self, guild_id: int, user_id: int):
        for party in self.parties.values():
            if party["guild_id"] != guild_id:
                continue

            await self.cleanup_expired_party_invites(party)

            for invite in self.normalize_party_invites(party):
                if invite.get("invited_user_id") != user_id:
                    continue
                if invite.get("status") != "pending":
                    continue
                if self.is_party_invite_expired(invite):
                    continue
                return party, invite

        return None, None

    async def get_party_invite_by_id(self, guild_id: int, invite_id: str):
        for party in self.parties.values():
            if party["guild_id"] != guild_id:
                continue

            await self.cleanup_expired_party_invites(party)

            for invite in self.normalize_party_invites(party):
                if invite.get("_id") == invite_id:
                    return party, invite

        return None, None

    async def accept_party_invite(self, guild: discord.Guild, user_id: int, invite_id: str):
        party, invite = await self.get_party_invite_by_id(guild.id, invite_id)
        if not party or not invite:
            return False, "Party invite not found."

        if invite.get("invited_user_id") != user_id:
            return False, "This invite is not for you."

        if invite.get("status") != "pending":
            return False, "This invite is no longer pending."

        if self.is_party_invite_expired(invite):
            invite["status"] = "expired"
            await self.save_party_doc(party)
            return False, "This party invite has expired."

        active_ban = await self.get_active_ranked_ban(guild.id, user_id)
        if active_ban:
            expires_text = self.format_ban_expiry(active_ban.get("expires_at"))
            return False, (
                f"You are banned from ranked.\n"
                f"Reason: **{active_ban.get('reason', 'No reason')}**\n"
                f"Expires: **{expires_text}**"
            )

        other_party = await self.get_party_for_user(guild.id, user_id)
        if other_party:
            return False, "You are already in a party."

        if len(party["members"]) >= 4:
            invite["status"] = "expired"
            await self.save_party_doc(party)
            return False, "That party is already full."

        party["members"].append(user_id)
        party["members"] = party["members"][:4]
        invite["status"] = "accepted"
        await self.touch_party(party)

        await self.send_ranked_log(
            guild,
            "✅ Party Invite Accepted",
            f"<@{user_id}> joined a ranked party.",
            color=discord.Color.green(),
            fields=[
                ("Party ID", f"`{party['_id']}`", True),
                ("Owner", f"<@{party['owner_id']}>", True),
            ]
        )

        return True, "You joined the party."

    async def decline_party_invite(self, guild: discord.Guild, user_id: int, invite_id: str):
        party, invite = await self.get_party_invite_by_id(guild.id, invite_id)
        if not party or not invite:
            return False, "Party invite not found."

        if invite.get("invited_user_id") != user_id:
            return False, "This invite is not for you."

        if invite.get("status") != "pending":
            return False, "This invite is no longer pending."

        invite["status"] = "declined"
        await self.save_party_doc(party)

        await self.send_ranked_log(
            guild,
            "❌ Party Invite Declined",
            f"<@{user_id}> declined a ranked party invite.",
            color=discord.Color.orange(),
            fields=[
                ("Party ID", f"`{party['_id']}`", True),
                ("Owner", f"<@{party['owner_id']}>", True),
            ]
        )

        return True, "You declined the party invite."

    async def save_party_doc(self, party: dict):
        await parties_col.update_one(
            {"_id": party["_id"]},
            {"$set": party},
            upsert=True
        )

    async def delete_party_doc(self, party_id: str):
        await parties_col.delete_one({"_id": party_id})

    async def touch_party(self, party: dict):
        party["last_activity_at"] = discord.utils.utcnow()
        await self.save_party_doc(party)

    async def get_party_by_owner(self, guild_id: int, owner_id: int):
        key = self.build_party_cache_key(guild_id, owner_id)
        return self.parties.get(key)

    async def get_party_for_user(self, guild_id: int, user_id: int):
        for party in self.parties.values():
            if party["guild_id"] != guild_id:
                continue
            if user_id in party.get("members", []):
                return party
        return None

    async def disband_party(self, guild: discord.Guild, party: dict, reason: str = "Party disbanded."):
        key = self.build_party_cache_key(party["guild_id"], party["owner_id"])
        self.parties.pop(key, None)
        await self.delete_party_doc(party["_id"])

        await self.remove_party_from_all_queues(guild.id, party["_id"])
        if guild:
            await self.refresh_all_queue_messages(guild)

    async def remove_party_from_all_queues(self, guild_id: int, party_id: str):
        changed = False
        queue_state = self.get_guild_queue_state(guild_id)

        for mode, entries in queue_state.items():
            new_entries = [entry for entry in entries if entry.get("party_id") != party_id]
            if len(new_entries) != len(entries):
                queue_state[mode] = new_entries
                changed = True

        if changed:
            await self.save_queue_state(guild_id)

    def get_queue_entry_size(self, entry: dict) -> int:
        return len(entry.get("members", []))

    def flatten_queue_members(self, guild_id: int, mode: str) -> list[int]:
        members = []
        for entry in self.get_guild_queue_state(guild_id).get(mode, []):
            members.extend(entry.get("members", []))
        return members

    def is_user_in_any_queue(self, guild_id: int, user_id: int) -> str | None:
        for mode, entries in self.get_guild_queue_state(guild_id).items():
            for entry in entries:
                if user_id in entry.get("members", []):
                    return mode
        return None

    def remove_members_from_all_queue_entries(self, guild_id: int, member_ids: list[int]):
        member_set = set(int(x) for x in member_ids)
        changed = False
        queue_state = self.get_guild_queue_state(guild_id)

        for mode, entries in queue_state.items():
            new_entries = []

            for entry in entries:
                entry_members = [int(m) for m in entry.get("members", []) if isinstance(m, int) or str(m).isdigit()]
                original_party_id = entry.get("party_id")

                # full party/group entry containing any of these members -> drop it
                if any(member in member_set for member in entry_members):
                    changed = True
                    continue

                new_entries.append({
                    "party_id": original_party_id,
                    "members": entry_members
                })

            queue_state[mode] = new_entries

        return changed

    def build_queue_entry(self, party: dict | None, user_id: int) -> dict:
        if party:
            members = []
            for member_id in party.get("members", []):
                try:
                    members.append(int(member_id))
                except (TypeError, ValueError):
                    continue

            members = list(dict.fromkeys(members))

            return {
                "party_id": party["_id"],
                "members": members
            }

        return {
            "party_id": None,
            "members": [int(user_id)]
        }

    def can_party_join_mode(self, mode: str, party_size: int) -> bool:
        team_size = int(mode[0])
        return party_size <= team_size

    def shuffle_preserving_party_entries(self, entries: list[dict]) -> list[int]:
        shuffled_entries = list(entries)
        random.shuffle(shuffled_entries)

        flattened = []
        for entry in shuffled_entries:
            members = list(entry.get("members", []))
            random.shuffle(members)
            flattened.extend(members)

        return flattened

    def split_entries_into_balanced_teams(self, entries: list[dict], mode: str):
        team_size = int(mode[0])
        target_total = team_size * 2

        normalized_entries = []
        for entry in entries:
            members = entry.get("members", [])
            if not isinstance(members, list):
                continue

            cleaned_members = []
            for member_id in members:
                try:
                    cleaned_members.append(int(member_id))
                except (TypeError, ValueError):
                    continue

            if not cleaned_members:
                continue

            normalized_entries.append({
                "party_id": entry.get("party_id"),
                "members": cleaned_members
            })

        if sum(len(e.get("members", [])) for e in normalized_entries) != target_total:
            return None, None

        for _ in range(20):
            shuffled_entries = list(normalized_entries)
            random.shuffle(shuffled_entries)

            team1_entries = []
            team2_entries = []
            team1_count = 0
            team2_count = 0

            for entry in sorted(shuffled_entries, key=lambda e: len(e.get("members", [])), reverse=True):
                size = len(entry.get("members", []))

                if team1_count + size <= team_size and (team1_count <= team2_count or team2_count + size > team_size):
                    team1_entries.append(entry)
                    team1_count += size
                elif team2_count + size <= team_size:
                    team2_entries.append(entry)
                    team2_count += size
                elif team1_count + size <= team_size:
                    team1_entries.append(entry)
                    team1_count += size
                else:
                    break
            else:
                if team1_count == team_size and team2_count == team_size:
                    team1 = []
                    for entry in team1_entries:
                        members = list(entry.get("members", []))
                        random.shuffle(members)
                        team1.extend(members)

                    team2 = []
                    for entry in team2_entries:
                        members = list(entry.get("members", []))
                        random.shuffle(members)
                        team2.extend(members)

                    random.shuffle(team1)
                    random.shuffle(team2)
                    return team1, team2

        return None, None

    def extract_full_match_entries(self, guild_id: int, mode: str):
        team_size = int(mode[0])
        needed_players = team_size * 2

        entries = []
        for entry in self.get_guild_queue_state(guild_id).get(mode, []):
            members = entry.get("members", [])
            if not isinstance(members, list):
                continue

            cleaned_members = []
            for member_id in members:
                try:
                    cleaned_members.append(int(member_id))
                except (TypeError, ValueError):
                    continue

            if not cleaned_members:
                continue

            entries.append({
                "party_id": entry.get("party_id"),
                "members": cleaned_members
            })

        # Prefer grouped/larger entries first so parties stay intact.
        entries.sort(key=lambda e: len(e.get("members", [])), reverse=True)

        selected = []
        total = 0

        for entry in entries:
            size = len(entry.get("members", []))
            if total + size > needed_players:
                continue

            selected.append(entry)
            total += size

            if total == needed_players:
                break

        if total != needed_players:
            return None

        return selected

    async def refresh_all_queue_messages(self, guild: discord.Guild):
        for mode in ("1v1", "2v2", "3v3", "4v4"):
            await self.refresh_queue_message(guild, mode)

    async def restore_persistent_views(self):
        async for config in config_col.find({}):
            guild_id = config["_id"]
            queue_messages = config.get("queue_messages", {})
            queue_channels = config.get("queue_channels", {})

            guild = self.bot.get_guild(guild_id)
            if not guild:
                continue

            for mode in ("1v1", "2v2", "3v3", "4v4"):
                message_id = queue_messages.get(mode)
                channel_id = queue_channels.get(mode)

                if not message_id or not channel_id:
                    continue

                self.bot.add_view(QueueView(self, mode), message_id=message_id)

                channel = guild.get_channel(channel_id)
                if channel:
                    try:
                        message = await channel.fetch_message(message_id)
                        await message.edit(embed=self.build_queue_embed(guild.id, mode), view=QueueView(self, mode))
                    except discord.NotFound:
                        pass
                    except discord.HTTPException:
                        pass

        async for match in matches_col.find({"status": "ongoing"}):
            result_message_id = match.get("result_message_id")
            confirmation_message_id = match.get("confirmation_message_id")

            if match.get("score_submission") is None:
                if result_message_id:
                    result_view = self.build_result_view_for_match(match)

                    self.bot.add_view(
                        result_view,
                        message_id=result_message_id
                    )

                    guild = self.bot.get_guild(match.get("guild_id")) if match.get("guild_id") else None
                    thread = await self.resolve_thread(guild, match.get("thread_id")) if guild else None

                    if thread:
                        try:
                            result_message = await thread.fetch_message(result_message_id)
                            await result_message.edit(view=result_view)
                        except discord.NotFound:
                            pass
                        except discord.HTTPException:
                            pass
            else:
                if confirmation_message_id:
                    target_team = match.get("score_confirmation", {}).get("target_team")

                    if target_team == "team1":
                        required_votes = len(match["teams"]["team1"])
                    elif target_team == "team2":
                        required_votes = len(match["teams"]["team2"])
                    else:
                        required_votes = 0

                    self.bot.add_view(
                        ScoreConfirmationView(self, match["_id"], required_votes=required_votes),
                        message_id=confirmation_message_id
                    )
            guild = self.bot.get_guild(match.get("guild_id")) if match.get("guild_id") else None
            if guild:
                await self.update_match_visuals(guild, match["_id"])
                
    @commands.Cog.listener()
    async def on_ready(self):
        if getattr(self, "_views_restored", False):
            return

        await self.load_persistent_queue_state()
        await self.load_persistent_parties()
        await self.restore_persistent_views()
        self._views_restored = True

    # ================= CONFIG =================

    async def get_config(self, guild_id):
        config = await config_col.find_one({"_id": guild_id})
        if not config:
            config = {
                "_id": guild_id,
                "queue_channels": {},
                "match_channels": {},
                "match_log_channels": {},
                "allowed_channels": [],
                "leaderboard": {},
                "placement_games": 5,
                "queue_state": self.build_default_queue_state(),
                "ranked_logs_channel": None,
                "ping_role_id": None,
                "teams_channel_id": None,
                "current_season": SEASON_DEFAULT_NUMBER,
                "k_factor": 40,
                "mode_k_factors": {
                    "1v1": 40,
                    "2v2": 40,
                    "3v3": 40,
                    "4v4": 40
                }
            }
            await config_col.insert_one(config)
        return config

    async def get_placement_games(self, guild_id: int) -> int:
        config = await self.get_config(guild_id)
        return max(1, int(config.get("placement_games", 5)))

    async def get_mode_k_factor(self, guild_id: int, mode: str) -> int:
        config = await self.get_config(guild_id)

        mode_k_factors = config.get("mode_k_factors", {})
        if isinstance(mode_k_factors, dict):
            value = mode_k_factors.get(mode)
            if value is not None:
                try:
                    return max(1, int(value))
                except (TypeError, ValueError):
                    pass

        # fallback to global k_factor
        return max(1, int(config.get("k_factor", 40)))

    async def get_ranked_logs_channel(self, guild: discord.Guild):
        config = await self.get_config(guild.id)
        channel_id = config.get("ranked_logs_channel")
        if not channel_id:
            return None
        return guild.get_channel(channel_id)

    async def get_teams_channel(self, guild: discord.Guild):
        config = await self.get_config(guild.id)
        channel_id = config.get("teams_channel_id")
        if not channel_id:
            return None
        return guild.get_channel(channel_id)

    def build_default_team_doc(self, guild_id: int, owner_id: int, name: str) -> dict:
        now = discord.utils.utcnow()
        return {
            "_id": uuid.uuid4().hex[:12],
            "guild_id": guild_id,
            "name": name,
            "owner_id": owner_id,
            "members": [owner_id],
            "invites": [],
            "created_at": now,
            "updated_at": now,

            "xp": 0,
            "level": 1,
            "points": 0,

            "stats": {
                "wins": 0,
                "losses": 0,
                "matches": 0
            },

            "season": {
                "season_number": None,
                "xp": 0,
                "level": 1,
                "points": 0,
                "stats": {
                    "wins": 0,
                    "losses": 0,
                    "matches": 0
                },
                "quests": {
                    "weekly": {"period_key": None, "completed": [], "claimed": [], "progress": {}},
                    "seasonal": {"period_key": "season", "completed": [], "claimed": [], "progress": {}}
                }
            },

            "channel_message_id": None
        }

    async def get_team_by_id(self, team_id: str):
        return await teams_col.find_one({"_id": team_id})

    async def get_team_for_user(self, guild_id: int, user_id: int):
        return await teams_col.find_one({
            "guild_id": guild_id,
            "members": user_id
        })

    async def save_team(self, team: dict):
        team["updated_at"] = discord.utils.utcnow()
        await teams_col.update_one({"_id": team["_id"]}, {"$set": team}, upsert=True)

    def normalize_team_invites(self, team: dict) -> list[dict]:
        invites = team.get("invites")
        if not isinstance(invites, list):
            invites = []
            team["invites"] = invites
        return invites

    def create_team_invite_doc(self, team: dict, invited_user_id: int) -> dict:
        now = discord.utils.utcnow()
        return {
            "_id": uuid.uuid4().hex[:12],
            "team_id": team["_id"],
            "guild_id": team["guild_id"],
            "owner_id": team["owner_id"],
            "invited_user_id": invited_user_id,
            "created_at": now,
            "expires_at": now + timedelta(minutes=15),
            "status": "pending"
        }

    def is_team_invite_expired(self, invite: dict) -> bool:
        expires_at = self.normalize_invite_time(invite.get("expires_at"))
        if expires_at is None:
            return True
        return expires_at <= discord.utils.utcnow()

    async def cleanup_expired_team_invites(self, team: dict):
        invites = self.normalize_team_invites(team)
        changed = False

        for invite in invites:
            if invite.get("status") == "pending" and self.is_team_invite_expired(invite):
                invite["status"] = "expired"
                changed = True

        if changed:
            await self.save_team(team)
            await self.sync_team_channel_message(self.bot.get_guild(team["guild_id"]), team)

    async def get_pending_team_invite_for_user(self, guild_id: int, user_id: int):
        async for team in teams_col.find({"guild_id": guild_id}):
            await self.cleanup_expired_team_invites(team)

            for invite in self.normalize_team_invites(team):
                if invite.get("invited_user_id") != user_id:
                    continue
                if invite.get("status") != "pending":
                    continue
                if self.is_team_invite_expired(invite):
                    continue
                return team, invite

        return None, None

    async def build_pending_team_invites_embed(self, guild: discord.Guild, user_id: int) -> discord.Embed:
        invites = []

        async for team in teams_col.find({"guild_id": guild.id}):
            await self.cleanup_expired_team_invites(team)

            for invite in self.normalize_team_invites(team):
                if invite.get("invited_user_id") != user_id:
                    continue
                if invite.get("status") != "pending":
                    continue
                if self.is_team_invite_expired(invite):
                    continue

                invites.append((team, invite))

        embed = discord.Embed(
            title="📨 Pending Team Invites",
            color=discord.Color.blurple()
        )

        if not invites:
            embed.description = "You have no pending team invites."
            return embed

        lines = []
        for team, invite in invites[:10]:
            lines.append(
                f"**Invite ID:** `{invite['_id']}`\n"
                f"**Team:** **{team['name']}**\n"
                f"**Team ID:** `{team['_id']}`\n"
                f"**Owner:** <@{team['owner_id']}>\n"
                f"**Expires:** {discord.utils.format_dt(invite['expires_at'], style='R')}"
            )

        embed.description = "\n\n".join(lines)
        return embed

    async def get_team_invite_by_id(self, guild_id: int, invite_id: str):
        async for team in teams_col.find({"guild_id": guild_id}):
            await self.cleanup_expired_team_invites(team)

            for invite in self.normalize_team_invites(team):
                if invite.get("_id") == invite_id:
                    return team, invite

        return None, None

    async def accept_team_invite(self, guild: discord.Guild, user_id: int, invite_id: str):
        team, invite = await self.get_team_invite_by_id(guild.id, invite_id)
        if not team or not invite:
            return False, "Team invite not found."

        if invite.get("invited_user_id") != user_id:
            return False, "This invite is not for you."

        if invite.get("status") != "pending":
            return False, "This invite is no longer pending."

        if self.is_team_invite_expired(invite):
            invite["status"] = "expired"
            await self.save_team(team)
            return False, "This team invite has expired."

        active_ban = await self.get_active_ranked_ban(guild.id, user_id)
        if active_ban:
            expires_text = self.format_ban_expiry(active_ban.get("expires_at"))
            return False, (
                f"You are banned from ranked.\n"
                f"Reason: **{active_ban.get('reason', 'No reason')}**\n"
                f"Expires: **{expires_text}**"
            )

        other_team = await self.get_team_for_user(guild.id, user_id)
        if other_team:
            return False, "You are already in a team."

        if len(team.get("members", [])) >= 8:
            invite["status"] = "expired"
            await self.save_team(team)
            return False, "That team is already full."

        team["members"].append(user_id)
        team["members"] = list(dict.fromkeys(team["members"]))[:8]
        invite["status"] = "accepted"
        await self.save_team(team)
        await self.sync_team_channel_message(guild, team)

        current_season = await self.get_current_season_number(guild.id)
        profile = await self.get_season_profile(guild.id, user_id, current_season)
        profile["team_id"] = team["_id"]
        await self.save_season_profile(profile)

        await self.send_ranked_log(
            guild,
            "✅ Team Invite Accepted",
            f"<@{user_id}> joined a ranked team.",
            color=discord.Color.green(),
            fields=[
                ("Team ID", f"`{team['_id']}`", True),
                ("Team", team["name"], True),
            ]
        )

        return True, f"You joined **{team['name']}**."

    async def decline_team_invite(self, guild: discord.Guild, user_id: int, invite_id: str):
        team, invite = await self.get_team_invite_by_id(guild.id, invite_id)
        if not team or not invite:
            return False, "Team invite not found."

        if invite.get("invited_user_id") != user_id:
            return False, "This invite is not for you."

        if invite.get("status") != "pending":
            return False, "This invite is no longer pending."

        invite["status"] = "declined"
        await self.save_team(team)
        await self.sync_team_channel_message(guild, team)

        await self.send_ranked_log(
            guild,
            "❌ Team Invite Declined",
            f"<@{user_id}> declined a ranked team invite.",
            color=discord.Color.orange(),
            fields=[
                ("Team ID", f"`{team['_id']}`", True),
                ("Team", team["name"], True),
            ]
        )

        return True, "You declined the team invite."

    def ensure_team_season_state(self, team: dict, season_number: int):
        season = team.setdefault("season", {
            "season_number": None,
            "xp": 0,
            "level": 1,
            "points": 0,
            "stats": {"wins": 0, "losses": 0, "matches": 0},
            "quests": {
                "weekly": {"period_key": None, "completed": [], "claimed": [], "progress": {}},
                "seasonal": {"period_key": "season", "completed": [], "claimed": [], "progress": {}}
            }
        })

        if season.get("season_number") != season_number:
            season["season_number"] = season_number
            season["xp"] = 0
            season["level"] = 1
            season["points"] = 0
            season["stats"] = {"wins": 0, "losses": 0, "matches": 0}
            season["quests"] = {
                "weekly": {"period_key": None, "completed": [], "claimed": [], "progress": {}},
                "seasonal": {"period_key": "season", "completed": [], "claimed": [], "progress": {}}
            }

        return season

    def normalize_team_quests_for_period(self, team: dict, season_number: int, period: str):
        season = self.ensure_team_season_state(team, season_number)
        quests = season.setdefault("quests", {})
        slot = quests.setdefault(period, {
            "period_key": None,
            "completed": [],
            "claimed": [],
            "progress": {}
        })

        expected_key = self.get_period_key(period)
        if slot.get("period_key") != expected_key:
            slot["period_key"] = expected_key
            slot["completed"] = []
            slot["claimed"] = []
            slot["progress"] = {}

        return slot

    def add_team_quest_metric(self, team: dict, season_number: int, period: str, metric: str, amount: int):
        slot = self.normalize_team_quests_for_period(team, season_number, period)
        progress = slot.setdefault("progress", {})
        progress[metric] = progress.get(metric, 0) + amount

        for template in TEAM_QUEST_TEMPLATES.get(period, []):
            current_value = progress.get(template["metric"], 0)
            if current_value >= template["target"] and template["id"] not in slot["completed"]:
                slot["completed"].append(template["id"])

    def level_up_team_xp(self, xp_value: int, level_value: int):
        xp = int(xp_value)
        level = int(level_value)

        while xp >= 100:
            xp -= 100
            level += 1

        return xp, level

    async def build_team_embed(self, guild: discord.Guild, team: dict) -> discord.Embed:
        owner_mention = f"<@{team['owner_id']}>"
        members_text = "\n".join(f"<@{m}>" for m in team.get("members", [])) or "No members"

        current_season = await self.get_current_season_number(guild.id)
        season = self.ensure_team_season_state(team, current_season)

        embed = discord.Embed(
            title=f"🛡️ Team • {team['name']}",
            color=discord.Color.blurple()
        )
        embed.add_field(name="Team ID", value=f"`{team['_id']}`", inline=False)
        embed.add_field(name="Owner", value=owner_mention, inline=True)
        embed.add_field(name="Members", value=f"`{len(team.get('members', []))}`", inline=True)
        embed.add_field(name="Created", value=discord.utils.format_dt(team["created_at"], style="R"), inline=True)

        embed.add_field(name="Roster", value=members_text, inline=False)

        embed.add_field(name="Global XP", value=f"`{team.get('xp', 0)}`", inline=True)
        embed.add_field(name="Global Level", value=f"`{team.get('level', 1)}`", inline=True)
        embed.add_field(name="Global Points", value=f"`{team.get('points', 0)}`", inline=True)

        global_stats = team.get("stats", {})
        embed.add_field(
            name="Global Record",
            value=(
                f"`{global_stats.get('wins', 0)}W` • "
                f"`{global_stats.get('losses', 0)}L` • "
                f"`{global_stats.get('matches', 0)}M`"
            ),
            inline=False
        )

        embed.add_field(name=f"Season {current_season} XP", value=f"`{season.get('xp', 0)}`", inline=True)
        embed.add_field(name=f"Season {current_season} Level", value=f"`{season.get('level', 1)}`", inline=True)
        embed.add_field(name=f"Season {current_season} Points", value=f"`{season.get('points', 0)}`", inline=True)

        season_stats = season.get("stats", {})
        embed.add_field(
            name=f"Season {current_season} Record",
            value=(
                f"`{season_stats.get('wins', 0)}W` • "
                f"`{season_stats.get('losses', 0)}L` • "
                f"`{season_stats.get('matches', 0)}M`"
            ),
            inline=False
        )

        pending_invites = [
            invite for invite in self.normalize_team_invites(team)
            if invite.get("status") == "pending" and not self.is_team_invite_expired(invite)
        ]

        if pending_invites:
            embed.add_field(
                name="Pending Invites",
                value="\n".join(
                    f"`{invite['_id']}` • <@{invite['invited_user_id']}> • expires {discord.utils.format_dt(invite['expires_at'], style='R')}"
                    for invite in pending_invites[:10]
                ),
                inline=False
            )

        return embed

    async def build_team_quests_embed(self, guild: discord.Guild, team: dict) -> discord.Embed:
        current_season = await self.get_current_season_number(guild.id)
        season = self.ensure_team_season_state(team, current_season)

        embed = discord.Embed(
            title=f"🎯 Team Quests • {team['name']}",
            color=discord.Color.blurple()
        )

        for period in ("weekly", "seasonal"):
            slot = self.normalize_team_quests_for_period(team, current_season, period)
            lines = []

            for template in TEAM_QUEST_TEMPLATES.get(period, []):
                current_value = slot.get("progress", {}).get(template["metric"], 0)
                status = "✅ Claimed" if template["id"] in slot.get("claimed", []) else (
                    "🟢 Ready" if template["id"] in slot.get("completed", []) else "🔸 In Progress"
                )
                lines.append(
                    f"**{template['label']}**\n"
                    f"{status} • `{current_value}/{template['target']}` • reward `{template['reward_xp']} XP`\n"
                    f"Quest ID: `{template['id']}`"
                )

            embed.add_field(
                name=period.title(),
                value="\n\n".join(lines) if lines else "No quests.",
                inline=False
            )

        return embed

    async def claim_team_quest(self, guild: discord.Guild, user_id: int, period: str, quest_id: str):
        team = await self.get_team_for_user(guild.id, user_id)
        if not team:
            return False, "You are not in a team."

        current_season = await self.get_current_season_number(guild.id)
        slot = self.normalize_team_quests_for_period(team, current_season, period)

        template = None
        for candidate in TEAM_QUEST_TEMPLATES.get(period, []):
            if candidate["id"] == quest_id:
                template = candidate
                break

        if template is None:
            return False, "Unknown team quest id."

        if quest_id not in slot.get("completed", []):
            return False, "That team quest is not completed yet."

        if quest_id in slot.get("claimed", []):
            return False, "That team quest reward was already claimed."

        slot["claimed"].append(quest_id)

        reward_xp = int(template["reward_xp"])
        team["xp"] = int(team.get("xp", 0)) + reward_xp
        team["xp"], team["level"] = self.level_up_team_xp(team["xp"], team.get("level", 1))

        season = self.ensure_team_season_state(team, current_season)
        season["xp"] = int(season.get("xp", 0)) + reward_xp
        season["xp"], season["level"] = self.level_up_team_xp(season["xp"], season.get("level", 1))

        await self.save_team(team)
        await self.sync_team_channel_message(guild, team)

        return True, f"Claimed team quest `{quest_id}` for `+{reward_xp} XP`."

    async def claim_all_team_quests(self, guild: discord.Guild, user_id: int):
        team = await self.get_team_for_user(guild.id, user_id)
        if not team:
            return False, "You are not in a team."

        current_season = await self.get_current_season_number(guild.id)

        claimed_ids = []
        total_xp = 0

        for period in ("weekly", "seasonal"):
            slot = self.normalize_team_quests_for_period(team, current_season, period)

            for template in TEAM_QUEST_TEMPLATES.get(period, []):
                quest_id = template["id"]
                if quest_id in slot.get("completed", []) and quest_id not in slot.get("claimed", []):
                    slot["claimed"].append(quest_id)
                    total_xp += int(template["reward_xp"])
                    claimed_ids.append(quest_id)

        if not claimed_ids:
            return False, "No team quests are ready to claim."

        team["xp"] = int(team.get("xp", 0)) + total_xp
        team["xp"], team["level"] = self.level_up_team_xp(team["xp"], team.get("level", 1))

        season = self.ensure_team_season_state(team, current_season)
        season["xp"] = int(season.get("xp", 0)) + total_xp
        season["xp"], season["level"] = self.level_up_team_xp(season["xp"], season.get("level", 1))

        await self.save_team(team)
        await self.sync_team_channel_message(guild, team)

        return True, f"Claimed `{len(claimed_ids)}` team quest(s) for `+{total_xp} XP`."

    async def sync_team_channel_message(self, guild: discord.Guild, team: dict):
        channel = await self.get_teams_channel(guild)
        if not channel:
            return

        embed = await self.build_team_embed(guild, team)
        message_id = team.get("channel_message_id")

        if message_id:
            try:
                message = await channel.fetch_message(message_id)
                await message.edit(embed=embed)
                return
            except discord.NotFound:
                pass
            except discord.HTTPException:
                pass

        try:
            message = await channel.send(embed=embed)
            team["channel_message_id"] = message.id
            await self.save_team(team)
        except discord.HTTPException:
            pass

    def get_team_points_for_match(self, won: bool, mode: str) -> int:
        base_map = {
            "1v1": 10,
            "2v2": 14,
            "3v3": 18,
            "4v4": 22,
        }
        base = base_map.get(mode, 10)
        return base if won else max(2, base // 3)

    async def apply_team_match_result(self, guild: discord.Guild, team_id: str, user_id: int, mode: str, won: bool, elo_diff: int):
        if not team_id:
            return

        team = await self.get_team_by_id(team_id)
        if not team or team.get("guild_id") != guild.id:
            return

        current_season = await self.get_current_season_number(guild.id)
        season = self.ensure_team_season_state(team, current_season)

        team["stats"]["matches"] = team.get("stats", {}).get("matches", 0) + 1
        season["stats"]["matches"] = season.get("stats", {}).get("matches", 0) + 1

        if won:
            team["stats"]["wins"] = team.get("stats", {}).get("wins", 0) + 1
            season["stats"]["wins"] = season.get("stats", {}).get("wins", 0) + 1
        else:
            team["stats"]["losses"] = team.get("stats", {}).get("losses", 0) + 1
            season["stats"]["losses"] = season.get("stats", {}).get("losses", 0) + 1

        gained_points = self.get_team_points_for_match(won, mode)
        team["points"] = int(team.get("points", 0)) + gained_points
        season["points"] = int(season.get("points", 0)) + gained_points

        gained_xp = 40 if won else 15
        team["xp"] = int(team.get("xp", 0)) + gained_xp
        season["xp"] = int(season.get("xp", 0)) + gained_xp

        team["xp"], team["level"] = self.level_up_team_xp(team["xp"], team.get("level", 1))
        season["xp"], season["level"] = self.level_up_team_xp(season["xp"], season.get("level", 1))

        self.add_team_quest_metric(team, current_season, "weekly", "matches", 1)
        self.add_team_quest_metric(team, current_season, "seasonal", "matches", 1)
        self.add_team_quest_metric(team, current_season, "seasonal", "points", gained_points)

        if won:
            self.add_team_quest_metric(team, current_season, "weekly", "wins", 1)
            self.add_team_quest_metric(team, current_season, "seasonal", "wins", 1)

        await self.save_team(team)
        await self.sync_team_channel_message(guild, team)

        profile = await self.get_season_profile(guild.id, user_id, current_season)
        profile["team_id"] = team_id
        profile["team_points"] = int(profile.get("team_points", 0)) + gained_points

        contribution = profile.setdefault("team_contribution", {
            "wins": 0,
            "matches": 0,
            "elo_gained": 0
        })
        contribution["matches"] += 1
        if won:
            contribution["wins"] += 1
        contribution["elo_gained"] += max(0, elo_diff)

        await self.save_season_profile(profile)

    async def get_current_season_number(self, guild_id: int) -> int:
        config = await self.get_config(guild_id)
        return int(config.get("current_season", SEASON_DEFAULT_NUMBER))

    async def set_current_season_number(self, guild_id: int, season_number: int):
        await config_col.update_one(
            {"_id": guild_id},
            {"$set": {"current_season": int(season_number)}},
            upsert=True
        )

    async def ensure_season_exists(self, guild_id: int, season_number: int):
        season_doc = await ranked_seasons_col.find_one({
            "guild_id": guild_id,
            "season_number": int(season_number)
        })

        if season_doc:
            return season_doc

        now = discord.utils.utcnow()
        season_doc = {
            "_id": f"{guild_id}:{season_number}",
            "guild_id": guild_id,
            "season_number": int(season_number),
            "name": f"Season {season_number}",
            "status": "active",
            "started_at": now,
            "ended_at": None,
            "archived_at": None,
            "notes": None,
            "team_support_ready": True
        }
        await ranked_seasons_col.insert_one(season_doc)
        return season_doc

    async def get_or_create_current_season(self, guild_id: int):
        season_number = await self.get_current_season_number(guild_id)
        return await self.ensure_season_exists(guild_id, season_number)

    def get_period_key(self, period: str, now: datetime | None = None) -> str:
        now = now or discord.utils.utcnow()

        if period == "daily":
            return now.strftime("%Y-%m-%d")
        if period == "weekly":
            iso = now.isocalendar()
            return f"{iso.year}-W{iso.week}"
        if period == "monthly":
            return now.strftime("%Y-%m")
        if period == "seasonal":
            return "season"
        return "unknown"

    def build_default_season_profile(self, guild_id: int, user_id: int, season_number: int) -> dict:
        return {
            "_id": f"{guild_id}:{season_number}:{user_id}",
            "guild_id": guild_id,
            "season_number": int(season_number),
            "user_id": user_id,

            "season_elo": 0,
            "peak_elo": 0,
            "xp": 0,
            "level": 1,

            "stats": {
                "global": {"wins": 0, "losses": 0, "matches": 0},
                "1v1": {"wins": 0, "losses": 0, "matches": 0},
                "2v2": {"wins": 0, "losses": 0, "matches": 0},
                "3v3": {"wins": 0, "losses": 0, "matches": 0},
                "4v4": {"wins": 0, "losses": 0, "matches": 0},
            },

            "history": [],
            "mmr_history": [],
            "quests": {
                "daily": {"period_key": None, "completed": [], "claimed": [], "progress": {}},
                "weekly": {"period_key": None, "completed": [], "claimed": [], "progress": {}},
                "monthly": {"period_key": None, "completed": [], "claimed": [], "progress": {}},
                "seasonal": {"period_key": "season", "completed": [], "claimed": [], "progress": {}},
            },

            "daily_login": {
                "last_claim_date": None,
                "streak": 0,
                "best_streak": 0,
                "month_restore_uses": 0,
                "month_restore_key": None
            },

            "reward_track": {
                "claimed_levels": []
            },

            "team_id": None,
            "team_points": 0,
            "team_contribution": {
                "wins": 0,
                "matches": 0,
                "elo_gained": 0
            },

            "created_at": discord.utils.utcnow(),
            "updated_at": discord.utils.utcnow(),
        }

    async def get_season_profile(self, guild_id: int, user_id: int, season_number: int | None = None):
        if season_number is None:
            season_number = await self.get_current_season_number(guild_id)

        await self.ensure_season_exists(guild_id, season_number)

        profile = await season_profiles_col.find_one({
            "guild_id": guild_id,
            "season_number": int(season_number),
            "user_id": user_id
        })

        if not profile:
            profile = self.build_default_season_profile(guild_id, user_id, season_number)
            await season_profiles_col.insert_one(profile)

        return profile

    def normalize_season_quests_for_period(self, profile: dict, period: str):
        quests = profile.setdefault("quests", {})
        slot = quests.setdefault(period, {
            "period_key": None,
            "completed": [],
            "claimed": [],
            "progress": {}
        })

        expected_key = self.get_period_key(period)
        if slot.get("period_key") != expected_key:
            slot["period_key"] = expected_key
            slot["completed"] = []
            slot["claimed"] = []
            slot["progress"] = {}

        return slot

    def update_season_quest_metric(self, profile: dict, period: str, metric: str, value: int):
        slot = self.normalize_season_quests_for_period(profile, period)
        progress = slot.setdefault("progress", {})
        progress[metric] = max(progress.get(metric, 0), value)

        for template in SEASON_QUEST_TEMPLATES.get(period, []):
            current_value = progress.get(template["metric"], 0)
            if current_value >= template["target"] and template["id"] not in slot["completed"]:
                slot["completed"].append(template["id"])

    def add_season_quest_metric(self, profile: dict, period: str, metric: str, amount: int):
        slot = self.normalize_season_quests_for_period(profile, period)
        progress = slot.setdefault("progress", {})
        progress[metric] = progress.get(metric, 0) + amount

        for template in SEASON_QUEST_TEMPLATES.get(period, []):
            current_value = progress.get(template["metric"], 0)
            if current_value >= template["target"] and template["id"] not in slot["completed"]:
                slot["completed"].append(template["id"])

    async def save_season_profile(self, profile: dict):
        profile["updated_at"] = discord.utils.utcnow()
        await season_profiles_col.update_one(
            {"_id": profile["_id"]},
            {"$set": profile},
            upsert=True
        )

    def get_daily_login_reward_xp(self, streak: int) -> int:
        base = 20
        bonus = min(30, max(0, streak - 1) * 2)
        return base + bonus

    def is_nitro_booster(self, member: discord.Member | None) -> bool:
        if member is None:
            return False

        if getattr(member, "premium_since", None) is not None:
            return True

        premium_role = getattr(member.guild, "premium_subscriber_role", None)
        if premium_role and premium_role in member.roles:
            return True

        return False

    async def claim_daily_login_reward(self, guild: discord.Guild, user_id: int, season_number: int | None = None):
        if season_number is None:
            season_number = await self.get_current_season_number(guild.id)

        profile = await self.get_season_profile(guild.id, user_id, season_number)
        daily_login = profile.setdefault("daily_login", {
            "last_claim_date": None,
            "streak": 0,
            "best_streak": 0,
            "month_restore_uses": 0,
            "month_restore_key": None
        })

        now = discord.utils.utcnow()
        today = now.date()
        last_claim = daily_login.get("last_claim_date")
        if isinstance(last_claim, datetime):
            last_claim = last_claim.date()

        if last_claim == today:
            return False, "You already claimed today’s daily login reward."

        if last_claim is None:
            daily_login["streak"] = 1
        else:
            days_missed = (today - last_claim).days

            if days_missed == 1:
                daily_login["streak"] += 1
            elif days_missed <= 3:
                daily_login["streak"] = 1
            else:
                daily_login["streak"] = 1

        daily_login["best_streak"] = max(daily_login.get("best_streak", 0), daily_login["streak"])
        daily_login["last_claim_date"] = datetime.combine(today, datetime.min.time(), tzinfo=timezone.utc)

        reward_xp = self.get_daily_login_reward_xp(daily_login["streak"])
        profile["xp"] += reward_xp

        level_ups = 0
        while profile["xp"] >= 100:
            profile["xp"] -= 100
            profile["level"] += 1
            level_ups += 1

        await self.save_season_profile(profile)
        return True, f"Claimed daily login: `+{reward_xp} XP` • streak: `{daily_login['streak']}` • level ups: `{level_ups}`"

    async def build_daily_login_embed(self, guild: discord.Guild, user_id: int, season_number: int | None = None) -> discord.Embed:
        if season_number is None:
            season_number = await self.get_current_season_number(guild.id)

        profile = await self.get_season_profile(guild.id, user_id, season_number)
        daily_login = profile.setdefault("daily_login", {
            "last_claim_date": None,
            "streak": 0,
            "best_streak": 0,
            "month_restore_uses": 0,
            "month_restore_key": None
        })

        next_reward = self.get_daily_login_reward_xp(max(1, daily_login.get("streak", 0) + 1))
        embed = discord.Embed(
            title=f"📅 Daily Login • Season {season_number}",
            color=discord.Color.blurple()
        )
        embed.add_field(name="Current Streak", value=f"`{daily_login.get('streak', 0)}`", inline=True)
        embed.add_field(name="Best Streak", value=f"`{daily_login.get('best_streak', 0)}`", inline=True)
        embed.add_field(name="Next Reward", value=f"`{next_reward} XP`", inline=True)

        last_claim = daily_login.get("last_claim_date")
        if last_claim:
            embed.add_field(name="Last Claim", value=discord.utils.format_dt(last_claim, style="R"), inline=False)
        else:
            embed.add_field(name="Last Claim", value="Never", inline=False)

        embed.set_footer(text="Daily streak resets if you miss too long. Restore system can be added later.")
        return embed

    async def restore_daily_login_streak(self, guild: discord.Guild, user_id: int, season_number: int | None = None):
        if season_number is None:
            season_number = await self.get_current_season_number(guild.id)

        member = guild.get_member(user_id)
        if not self.is_nitro_booster(member):
            return False, "Only Nitro boosters can restore a broken daily login streak."

        profile = await self.get_season_profile(guild.id, user_id, season_number)
        daily_login = profile.setdefault("daily_login", {
            "last_claim_date": None,
            "streak": 0,
            "best_streak": 0,
            "month_restore_uses": 0,
            "month_restore_key": None
        })

        now = discord.utils.utcnow()
        today = now.date()

        last_claim = daily_login.get("last_claim_date")
        if isinstance(last_claim, datetime):
            last_claim = last_claim.date()

        if last_claim is None:
            return False, "You do not have a previous daily streak to restore."

        days_missed = (today - last_claim).days

        if days_missed <= 1:
            return False, "Your streak is not broken in a way that needs restoring."

        if days_missed > 3:
            return False, "Your streak cannot be restored after more than 3 days."

        month_key = now.strftime("%Y-%m")
        if daily_login.get("month_restore_key") != month_key:
            daily_login["month_restore_key"] = month_key
            daily_login["month_restore_uses"] = 0

        if daily_login.get("month_restore_uses", 0) >= 5:
            return False, "You already used all 5 daily streak restores for this month."

        daily_login["month_restore_uses"] += 1
        daily_login["last_claim_date"] = datetime.combine(today - timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)

        await self.save_season_profile(profile)
        return True, (
            f"Your daily streak was restored.\n"
            f"Restores used this month: `{daily_login['month_restore_uses']}/5`"
        )

    async def build_daily_restore_embed(self, guild: discord.Guild, user_id: int, season_number: int | None = None) -> discord.Embed:
        if season_number is None:
            season_number = await self.get_current_season_number(guild.id)

        profile = await self.get_season_profile(guild.id, user_id, season_number)
        daily_login = profile.setdefault("daily_login", {
            "last_claim_date": None,
            "streak": 0,
            "best_streak": 0,
            "month_restore_uses": 0,
            "month_restore_key": None
        })

        member = guild.get_member(user_id)
        is_booster = self.is_nitro_booster(member)

        now = discord.utils.utcnow()
        month_key = now.strftime("%Y-%m")
        used = daily_login.get("month_restore_uses", 0) if daily_login.get("month_restore_key") == month_key else 0
        remaining = max(0, 5 - used)

        embed = discord.Embed(
            title=f"💎 Daily Streak Restore • Season {season_number}",
            color=discord.Color.blurple()
        )
        embed.add_field(name="Nitro Booster", value="Yes" if is_booster else "No", inline=True)
        embed.add_field(name="Restores Used", value=f"`{used}/5`", inline=True)
        embed.add_field(name="Restores Remaining", value=f"`{remaining}`", inline=True)
        embed.set_footer(text="A streak can only be restored within 3 days of missing it.")
        return embed

    async def build_season_profile_embed(self, guild: discord.Guild, target_user_id: int, season_number: int | None = None) -> discord.Embed:
        if season_number is None:
            season_number = await self.get_current_season_number(guild.id)

        profile = await self.get_season_profile(guild.id, target_user_id, season_number)
        target_obj = await self.resolve_member_or_user(guild, target_user_id)
        target_name = target_obj.display_name if isinstance(target_obj, discord.Member) else (
            target_obj.name if target_obj else str(target_user_id)
        )

        rank_info = get_rank_info(profile.get("season_elo", 0))
        global_stats = profile.get("stats", {}).get("global", {"wins": 0, "losses": 0, "matches": 0})

        embed = discord.Embed(
            title=f"🌦️ Season {season_number} Profile • {target_name}",
            color=discord.Color.blurple()
        )
        embed.add_field(name="Season MMR", value=f"`{profile.get('season_elo', 0)}`", inline=True)
        embed.add_field(name="Peak MMR", value=f"`{profile.get('peak_elo', 0)}`", inline=True)
        embed.add_field(name="Rank", value=format_rank_display(rank_info["name"]), inline=True)

        embed.add_field(name="Level", value=str(profile.get("level", 1)), inline=True)
        embed.add_field(name="XP", value=str(profile.get("xp", 0)), inline=True)
        embed.add_field(name="Progress", value=format_rank_progress(profile.get("season_elo", 0)), inline=False)

        embed.add_field(
            name="Season Record",
            value=(
                f"`{global_stats.get('wins', 0)}W` • "
                f"`{global_stats.get('losses', 0)}L` • "
                f"`{global_stats.get('matches', 0)}M` • "
                f"`{self.calculate_winrate(global_stats.get('wins', 0), global_stats.get('matches', 0))}`"
            ),
            inline=False
        )

        team_id = profile.get("team_id")
        team_points = profile.get("team_points", 0)
        team_contribution = profile.get("team_contribution", {"wins": 0, "matches": 0, "elo_gained": 0})

        embed.add_field(
            name="Season Team",
            value=f"`{team_id}`" if team_id else "No team assigned yet",
            inline=True
        )
        embed.add_field(name="Team Points", value=f"`{team_points}`", inline=True)
        embed.add_field(
            name="Contribution",
            value=(
                f"`{team_contribution.get('wins', 0)} wins` • "
                f"`{team_contribution.get('matches', 0)} matches` • "
                f"`{team_contribution.get('elo_gained', 0)} elo`"
            ),
            inline=False
        )
        return embed

    async def build_season_stats_embed(self, guild: discord.Guild, user_id: int, season_number: int | None = None) -> discord.Embed:
        if season_number is None:
            season_number = await self.get_current_season_number(guild.id)

        profile = await self.get_season_profile(guild.id, user_id, season_number)
        target_obj = await self.resolve_member_or_user(guild, user_id)
        target_name = target_obj.display_name if isinstance(target_obj, discord.Member) else (
            target_obj.name if target_obj else str(user_id)
        )

        stats = profile.get("stats", {})
        global_stats = stats.get("global", {"wins": 0, "losses": 0, "matches": 0})

        embed = discord.Embed(
            title=f"📊 Season {season_number} Stats • {target_name}",
            color=discord.Color.blurple()
        )

        embed.add_field(name="Wins", value=f"`{global_stats.get('wins', 0)}`", inline=True)
        embed.add_field(name="Losses", value=f"`{global_stats.get('losses', 0)}`", inline=True)
        embed.add_field(name="Matches", value=f"`{global_stats.get('matches', 0)}`", inline=True)

        embed.add_field(
            name="Winrate",
            value=self.calculate_winrate(global_stats.get("wins", 0), global_stats.get("matches", 0)),
            inline=True
        )
        embed.add_field(name="Season MMR", value=f"`{profile.get('season_elo', 0)}`", inline=True)
        embed.add_field(name="Peak MMR", value=f"`{profile.get('peak_elo', 0)}`", inline=True)

        mode_lines = []
        for mode_name in ("1v1", "2v2", "3v3", "4v4"):
            mode_stats = stats.get(mode_name, {"wins": 0, "losses": 0, "matches": 0})
            mode_lines.append(
                f"**{mode_name}** • `{mode_stats.get('wins', 0)}W-{mode_stats.get('losses', 0)}L` • `{mode_stats.get('matches', 0)}M`"
            )

        embed.add_field(name="Mode Breakdown", value="\n".join(mode_lines), inline=False)
        return embed

    async def build_season_activity_embed(self, guild: discord.Guild, season_number: int | None = None) -> discord.Embed:
        if season_number is None:
            season_number = await self.get_current_season_number(guild.id)

        total_players = 0
        total_matches = 0
        total_wins = 0
        total_xp = 0
        total_levels = 0
        team_members = 0
        mode_counter = Counter()
        player_activity = []

        async for profile in season_profiles_col.find({
            "guild_id": guild.id,
            "season_number": int(season_number)
        }):
            total_players += 1

            stats = profile.get("stats", {})
            global_stats = stats.get("global", {})
            matches = global_stats.get("matches", 0)
            wins = global_stats.get("wins", 0)

            total_matches += matches
            total_wins += wins
            total_xp += int(profile.get("xp", 0))
            total_levels += int(profile.get("level", 1))

            if profile.get("team_id"):
                team_members += 1

            for mode_name in ("1v1", "2v2", "3v3", "4v4"):
                mode_counter[mode_name] += stats.get(mode_name, {}).get("matches", 0)

            player_activity.append((profile["user_id"], matches, profile.get("season_elo", 0)))

        player_activity.sort(key=lambda x: (x[1], x[2]), reverse=True)

        embed = discord.Embed(
            title=f"📊 Season {season_number} Activity",
            color=discord.Color.blurple()
        )

        embed.add_field(name="Tracked Players", value=f"`{total_players}`", inline=True)
        embed.add_field(name="Total Matches", value=f"`{total_matches}`", inline=True)
        embed.add_field(name="Total Wins", value=f"`{total_wins}`", inline=True)

        avg_level = round(total_levels / total_players, 2) if total_players else 0
        avg_xp = round(total_xp / total_players, 2) if total_players else 0

        embed.add_field(name="Avg Level", value=f"`{avg_level}`", inline=True)
        embed.add_field(name="Avg XP", value=f"`{avg_xp}`", inline=True)
        embed.add_field(name="Players In Teams", value=f"`{team_members}`", inline=True)

        mode_lines = []
        for mode_name in ("1v1", "2v2", "3v3", "4v4"):
            mode_lines.append(f"**{mode_name}** • `{mode_counter.get(mode_name, 0)}` matches")
        embed.add_field(name="Mode Activity", value="\n".join(mode_lines), inline=False)

        if player_activity:
            top_lines = []
            for index, (user_id, matches, mmr) in enumerate(player_activity[:10], start=1):
                top_lines.append(f"**{index}.** <@{user_id}> • `{matches}` matches • `{mmr} season MMR`")
            embed.add_field(name="Most Active Players", value="\n".join(top_lines), inline=False)
        else:
            embed.add_field(name="Most Active Players", value="No season activity yet.", inline=False)

        return embed

    async def build_season_progress_embed(self, guild: discord.Guild, user_id: int, season_number: int | None = None) -> discord.Embed:
        if season_number is None:
            season_number = await self.get_current_season_number(guild.id)

        profile = await self.get_season_profile(guild.id, user_id, season_number)
        target_obj = await self.resolve_member_or_user(guild, user_id)
        target_name = target_obj.display_name if isinstance(target_obj, discord.Member) else (
            target_obj.name if target_obj else str(user_id)
        )

        level = profile.get("level", 1)
        xp = profile.get("xp", 0)
        season_elo = profile.get("season_elo", 0)
        peak_elo = profile.get("peak_elo", 0)
        rank_info = get_rank_info(season_elo)

        filled = max(0, min(10, int((xp / 100) * 10)))
        bar = "█" * filled + "░" * (10 - filled)

        next_rewards = []
        for reward_level in sorted(SEASON_REWARD_TRACK.keys()):
            if reward_level >= level:
                reward = SEASON_REWARD_TRACK[reward_level]
                next_rewards.append(
                    f"**Lv {reward_level}** • {reward['label']} • `+{reward['xp_bonus']} XP`"
                )
            if len(next_rewards) >= 5:
                break

        embed = discord.Embed(
            title=f"📈 Season {season_number} Progress • {target_name}",
            color=discord.Color.blurple()
        )
        embed.add_field(name="Level", value=f"`{level}`", inline=True)
        embed.add_field(name="XP", value=f"`{xp}/100`", inline=True)
        embed.add_field(name="Rank", value=format_rank_display(rank_info["name"]), inline=True)

        embed.add_field(name="Progress Bar", value=f"`{bar}`", inline=False)
        embed.add_field(name="Season MMR", value=f"`{season_elo}`", inline=True)
        embed.add_field(name="Peak MMR", value=f"`{peak_elo}`", inline=True)
        embed.add_field(name="MMR Progress", value=format_rank_progress(season_elo), inline=False)

        embed.add_field(
            name="Next Reward Track Milestones",
            value="\n".join(next_rewards) if next_rewards else "No upcoming milestones.",
            inline=False
        )

        return embed

    def get_claimable_season_reward_levels(self, profile: dict) -> list[int]:
        level = int(profile.get("level", 1))
        reward_track = profile.setdefault("reward_track", {"claimed_levels": []})
        claimed_levels = reward_track.setdefault("claimed_levels", [])

        claimable = []
        for reward_level in sorted(SEASON_REWARD_TRACK.keys()):
            if reward_level <= level and reward_level not in claimed_levels:
                claimable.append(reward_level)

        return claimable

    async def build_season_rewards_embed(self, guild: discord.Guild, user_id: int, season_number: int | None = None) -> discord.Embed:
        if season_number is None:
            season_number = await self.get_current_season_number(guild.id)

        profile = await self.get_season_profile(guild.id, user_id, season_number)
        reward_track = profile.setdefault("reward_track", {"claimed_levels": []})
        claimed_levels = reward_track.setdefault("claimed_levels", [])

        target_obj = await self.resolve_member_or_user(guild, user_id)
        target_name = target_obj.display_name if isinstance(target_obj, discord.Member) else (
            target_obj.name if target_obj else str(user_id)
        )

        embed = discord.Embed(
            title=f"🎁 Season {season_number} Rewards • {target_name}",
            color=discord.Color.blurple()
        )

        lines = []
        current_level = int(profile.get("level", 1))

        for reward_level in sorted(SEASON_REWARD_TRACK.keys()):
            reward = SEASON_REWARD_TRACK[reward_level]

            if reward_level in claimed_levels:
                status = "✅ Claimed"
            elif reward_level <= current_level:
                status = "🟢 Ready to claim"
            else:
                status = "🔒 Locked"

            lines.append(
                f"**Level {reward_level}** • {reward['label']}\n"
                f"{status} • reward: `+{reward['xp_bonus']} XP`"
            )

        embed.description = "\n\n".join(lines) if lines else "No reward track entries found."
        embed.set_footer(text="Use claimseasonreward <level> or claimallseasonrewards")
        return embed

    async def claim_season_reward(self, guild: discord.Guild, user_id: int, reward_level: int, season_number: int | None = None):
        if season_number is None:
            season_number = await self.get_current_season_number(guild.id)

        profile = await self.get_season_profile(guild.id, user_id, season_number)
        reward_track = profile.setdefault("reward_track", {"claimed_levels": []})
        claimed_levels = reward_track.setdefault("claimed_levels", [])

        if reward_level not in SEASON_REWARD_TRACK:
            return False, "That reward level does not exist."

        current_level = int(profile.get("level", 1))
        if reward_level > current_level:
            return False, "That reward is still locked."

        if reward_level in claimed_levels:
            return False, "That reward was already claimed."

        reward = SEASON_REWARD_TRACK[reward_level]
        reward_xp = int(reward.get("xp_bonus", 0))

        claimed_levels.append(reward_level)

        # reward is extra XP on top of current progress
        profile["xp"] += reward_xp

        level_ups = 0
        while profile["xp"] >= 100:
            profile["xp"] -= 100
            profile["level"] += 1
            level_ups += 1

        await self.save_season_profile(profile)

        return True, (
            f"Claimed season reward for **Level {reward_level}**.\n"
            f"Reward: `+{reward_xp} XP`\n"
            f"Level ups: `{level_ups}`"
        )

    async def claim_all_season_rewards(self, guild: discord.Guild, user_id: int, season_number: int | None = None):
        if season_number is None:
            season_number = await self.get_current_season_number(guild.id)

        profile = await self.get_season_profile(guild.id, user_id, season_number)
        reward_track = profile.setdefault("reward_track", {"claimed_levels": []})
        claimed_levels = reward_track.setdefault("claimed_levels", [])

        current_level = int(profile.get("level", 1))
        claimable_levels = [
            reward_level
            for reward_level in sorted(SEASON_REWARD_TRACK.keys())
            if reward_level <= current_level and reward_level not in claimed_levels
        ]

        if not claimable_levels:
            return False, "No season rewards are ready to claim."

        total_xp = 0
        for reward_level in claimable_levels:
            claimed_levels.append(reward_level)
            total_xp += int(SEASON_REWARD_TRACK[reward_level].get("xp_bonus", 0))

        profile["xp"] += total_xp

        level_ups = 0
        while profile["xp"] >= 100:
            profile["xp"] -= 100
            profile["level"] += 1
            level_ups += 1

        await self.save_season_profile(profile)

        return True, (
            f"Claimed `{len(claimable_levels)}` season reward(s).\n"
            f"Total reward: `+{total_xp} XP`\n"
            f"Level ups: `{level_ups}`"
        )

    async def build_season_quests_embed(self, guild: discord.Guild, user_id: int, season_number: int | None = None) -> discord.Embed:
        if season_number is None:
            season_number = await self.get_current_season_number(guild.id)

        profile = await self.get_season_profile(guild.id, user_id, season_number)
        embed = discord.Embed(
            title=f"🎯 Season {season_number} Quests",
            color=discord.Color.blurple()
        )

        for period in ("daily", "weekly", "monthly", "seasonal"):
            slot = self.normalize_season_quests_for_period(profile, period)
            lines = []

            for template in SEASON_QUEST_TEMPLATES.get(period, []):
                current_value = slot.get("progress", {}).get(template["metric"], 0)
                status = "✅ Claimed" if template["id"] in slot.get("claimed", []) else (
                    "🟢 Ready" if template["id"] in slot.get("completed", []) else "🔸 In Progress"
                )
                lines.append(
                    f"**{template['label']}**\n"
                    f"{status} • `{current_value}/{template['target']}` • reward `{template['reward_xp']} XP`\n"
                    f"Quest ID: `{template['id']}`"
                )

            embed.add_field(
                name=period.title(),
                value="\n\n".join(lines) if lines else "No quests.",
                inline=False
            )

        return embed

    async def build_season_quests_page_embed(self, guild: discord.Guild, user_id: int, season_number: int | None = None, page: int = 1) -> discord.Embed:
        if season_number is None:
            season_number = await self.get_current_season_number(guild.id)

        periods = ["daily", "weekly", "monthly", "seasonal"]
        page = max(1, min(page, len(periods)))
        period = periods[page - 1]

        profile = await self.get_season_profile(guild.id, user_id, season_number)
        slot = self.normalize_season_quests_for_period(profile, period)

        embed = discord.Embed(
            title=f"🎯 Season {season_number} Quests • {period.title()}",
            color=discord.Color.blurple()
        )

        lines = []
        for template in SEASON_QUEST_TEMPLATES.get(period, []):
            current_value = slot.get("progress", {}).get(template["metric"], 0)
            status = "✅ Claimed" if template["id"] in slot.get("claimed", []) else (
                "🟢 Ready" if template["id"] in slot.get("completed", []) else "🔸 In Progress"
            )
            lines.append(
                f"**{template['label']}**\n"
                f"{status} • `{current_value}/{template['target']}` • reward `{template['reward_xp']} XP`\n"
                f"Quest ID: `{template['id']}`"
            )

        embed.description = "\n\n".join(lines) if lines else "No quests."
        embed.set_footer(text=f"Page {page}/4")
        return embed

    async def claim_season_quest(self, guild: discord.Guild, user_id: int, period: str, quest_id: str, season_number: int | None = None):
        if season_number is None:
            season_number = await self.get_current_season_number(guild.id)

        profile = await self.get_season_profile(guild.id, user_id, season_number)
        slot = self.normalize_season_quests_for_period(profile, period)

        template = None
        for candidate in SEASON_QUEST_TEMPLATES.get(period, []):
            if candidate["id"] == quest_id:
                template = candidate
                break

        if template is None:
            return False, "Unknown quest id."

        if quest_id not in slot.get("completed", []):
            return False, "That quest is not completed yet."

        if quest_id in slot.get("claimed", []):
            return False, "That quest reward was already claimed."

        slot["claimed"].append(quest_id)
        profile["xp"] += template["reward_xp"]

        while profile["xp"] >= 100:
            profile["xp"] -= 100
            profile["level"] += 1

        await self.save_season_profile(profile)
        return True, f"Claimed `{template['reward_xp']} XP` from `{quest_id}`."

    async def claim_all_season_quests(self, guild: discord.Guild, user_id: int, season_number: int | None = None):
        if season_number is None:
            season_number = await self.get_current_season_number(guild.id)

        profile = await self.get_season_profile(guild.id, user_id, season_number)

        total_xp = 0
        claimed_ids = []

        for period in ("daily", "weekly", "monthly", "seasonal"):
            slot = self.normalize_season_quests_for_period(profile, period)

            for template in SEASON_QUEST_TEMPLATES.get(period, []):
                quest_id = template["id"]
                if quest_id in slot.get("completed", []) and quest_id not in slot.get("claimed", []):
                    slot["claimed"].append(quest_id)
                    total_xp += template["reward_xp"]
                    claimed_ids.append(quest_id)

        if not claimed_ids:
            return False, "No quests are ready to claim."

        profile["xp"] += total_xp

        level_ups = 0
        while profile["xp"] >= 100:
            profile["xp"] -= 100
            profile["level"] += 1
            level_ups += 1

        await self.save_season_profile(profile)
        return True, f"Claimed `{len(claimed_ids)}` quest(s) for `+{total_xp} XP` • level ups: `{level_ups}`"

    async def archive_season(self, guild_id: int, season_number: int, notes: str | None = None):
        season = await self.ensure_season_exists(guild_id, season_number)

        leaderboard = []
        async for profile in season_profiles_col.find({
            "guild_id": guild_id,
            "season_number": int(season_number)
        }):
            leaderboard.append({
                "user_id": profile["user_id"],
                "season_elo": profile.get("season_elo", 0),
                "peak_elo": profile.get("peak_elo", 0),
                "stats": profile.get("stats", {}),
                "level": profile.get("level", 1),
                "xp": profile.get("xp", 0),
                "team_id": profile.get("team_id"),
                "team_points": profile.get("team_points", 0),
            })

        leaderboard.sort(key=lambda x: (x["season_elo"], x["peak_elo"]), reverse=True)

        team_rows = {}
        for row in leaderboard:
            team_id = row.get("team_id")
            if not team_id:
                continue

            bucket = team_rows.setdefault(team_id, {
                "team_id": team_id,
                "members": [],
                "total_points": 0,
                "total_wins": 0,
                "total_matches": 0,
                "total_elo": 0,
            })

            bucket["members"].append(row["user_id"])
            bucket["total_points"] += int(row.get("team_points", 0))
            bucket["total_elo"] += int(row.get("season_elo", 0))

            stats = row.get("stats", {}).get("global", {})
            bucket["total_wins"] += int(stats.get("wins", 0))
            bucket["total_matches"] += int(stats.get("matches", 0))

        team_leaderboard = list(team_rows.values())
        team_leaderboard.sort(
            key=lambda x: (x["total_points"], x["total_wins"], x["total_elo"]),
            reverse=True
        )

        archive_doc = {
            "_id": f"{guild_id}:{season_number}",
            "guild_id": guild_id,
            "season_number": int(season_number),
            "archived_at": discord.utils.utcnow(),
            "notes": notes,
            "leaderboard": leaderboard,
            "team_leaderboard": team_leaderboard,
            "team_support_ready": True,
        }

        await season_archives_col.update_one(
            {"_id": archive_doc["_id"]},
            {"$set": archive_doc},
            upsert=True
        )

        await ranked_seasons_col.update_one(
            {"guild_id": guild_id, "season_number": int(season_number)},
            {
                "$set": {
                    "status": "archived",
                    "ended_at": discord.utils.utcnow(),
                    "archived_at": discord.utils.utcnow(),
                    "notes": notes
                }
            },
            upsert=True
        )

    async def execute_season_danger_action(self, guild: discord.Guild, action: str, actor_id: int, payload: dict):
        actor_mention = f"<@{actor_id}>"

        if action == "seasonstart":
            current = await self.get_current_season_number(guild.id)

            current_doc = await ranked_seasons_col.find_one({
                "guild_id": guild.id,
                "season_number": int(current)
            })

            if current_doc and current_doc.get("status") != "archived":
                await self.archive_season(
                    guild.id,
                    current,
                    notes=f"Auto-archived when starting new season by {actor_id}"
                )

            season_number = int(payload["season_number"])
            name = payload.get("name")

            await self.set_current_season_number(guild.id, season_number)
            season_doc = await self.ensure_season_exists(guild.id, season_number)

            await ranked_seasons_col.update_one(
                {"guild_id": guild.id, "season_number": int(season_number)},
                {
                    "$set": {
                        "status": "active",
                        "started_at": discord.utils.utcnow(),
                        "ended_at": None,
                        "archived_at": None,
                        "name": name or season_doc.get("name", f"Season {season_number}")
                    }
                },
                upsert=True
            )

            await self.send_ranked_log(
                guild,
                "🌦️ Season Started",
                f"{actor_mention} started a new ranked season.",
                color=discord.Color.green(),
                fields=[
                    ("Season", f"`{season_number}`", True),
                    ("Name", name or season_doc.get("name", f"Season {season_number}"), True),
                    ("Reset Type", "Fresh seasonal profiles", False),
                ]
            )

            return True, f"Started **Season {season_number}**."

        if action == "seasonend":
            season_number = int(payload["season_number"])
            notes = payload.get("notes")

            await self.archive_season(guild.id, season_number, notes=notes)

            await self.send_ranked_log(
                guild,
                "🗂️ Season Archived",
                f"{actor_mention} archived a ranked season.",
                color=discord.Color.gold(),
                fields=[
                    ("Season", f"`{season_number}`", True),
                    ("Notes", notes or "No notes", False),
                ]
            )

            return True, f"Archived **Season {season_number}**."

        if action == "setseason":
            season_number = int(payload["season_number"])

            await self.set_current_season_number(guild.id, season_number)
            await self.ensure_season_exists(guild.id, season_number)

            await self.send_ranked_log(
                guild,
                "🛠️ Current Season Changed",
                f"{actor_mention} changed the active season.",
                color=discord.Color.orange(),
                fields=[
                    ("Season", f"`{season_number}`", False),
                ]
            )

            return True, f"Current season set to **Season {season_number}**."

        if action == "seasonsnapshot":
            season_number = int(payload["season_number"])
            notes = payload.get("notes")

            await self.archive_season(guild.id, season_number, notes=notes)

            await self.send_ranked_log(
                guild,
                "📸 Season Snapshot Saved",
                f"{actor_mention} created a season snapshot.",
                color=discord.Color.gold(),
                fields=[
                    ("Season", f"`{season_number}`", True),
                    ("Notes", notes or "No notes", False),
                ]
            )

            return True, f"Snapshot saved for **Season {season_number}**."

        return False, "Unknown season action."

    async def build_season_archive_embed(self, guild: discord.Guild, season_number: int) -> discord.Embed:
        archive = await season_archives_col.find_one({
            "guild_id": guild.id,
            "season_number": int(season_number)
        })

        embed = discord.Embed(
            title=f"🗂️ Season {season_number} Archive",
            color=discord.Color.gold()
        )

        if not archive:
            embed.description = "No archive found for that season."
            return embed

        leaderboard = archive.get("leaderboard", [])
        if not leaderboard:
            embed.description = "Archive exists but contains no leaderboard data."
            return embed

        lines = []
        for index, row in enumerate(leaderboard[:15], start=1):
            lines.append(
                f"**{index}.** <@{row['user_id']}> • "
                f"`{row.get('season_elo', 0)} MMR` • "
                f"`{row.get('peak_elo', 0)} peak`"
            )

        embed.description = "\n".join(lines)
        embed.set_footer(text=f"Archived at {archive.get('archived_at')}")
        return embed

    async def build_season_history_embed(self, guild: discord.Guild, user_id: int, season_number: int | None = None) -> discord.Embed:
        target_obj = await self.resolve_member_or_user(guild, user_id)
        target_name = target_obj.display_name if isinstance(target_obj, discord.Member) else (
            target_obj.name if target_obj else str(user_id)
        )

        embed = discord.Embed(
            title=f"📚 Season History • {target_name}",
            color=discord.Color.gold()
        )

        rows = []

        async for archive in season_archives_col.find({"guild_id": guild.id}).sort("season_number", -1):
            season_number_archive = archive.get("season_number")
            if season_number is not None and season_number_archive != int(season_number):
                continue

            leaderboard = archive.get("leaderboard", [])
            found = None
            for index, row in enumerate(leaderboard, start=1):
                if row.get("user_id") == user_id:
                    found = (index, row)
                    break

            if found:
                position, row = found
                rows.append(
                    f"**Season {season_number_archive}** • `#{position}` • "
                    f"`{row.get('season_elo', 0)} MMR` • "
                    f"`{row.get('peak_elo', 0)} peak`"
                )

        if not rows:
            embed.description = "No archived season history found."
        else:
            embed.description = "\n".join(rows[:15])

        return embed

    async def build_season_recap_embed(self, guild: discord.Guild, user_id: int, season_number: int) -> discord.Embed:
        target_obj = await self.resolve_member_or_user(guild, user_id)
        target_name = target_obj.display_name if isinstance(target_obj, discord.Member) else (
            target_obj.name if target_obj else str(user_id)
        )

        archive = await season_archives_col.find_one({
            "guild_id": guild.id,
            "season_number": int(season_number)
        })

        embed = discord.Embed(
            title=f"🧾 Season {season_number} Recap • {target_name}",
            color=discord.Color.gold()
        )

        if not archive:
            embed.description = "No archive found for that season."
            return embed

        leaderboard = archive.get("leaderboard", [])
        found = None
        final_rank_info = get_rank_info(row.get("season_elo", 0))
        for index, row in enumerate(leaderboard, start=1):
            if row.get("user_id") == user_id:
                found = (index, row)
                break

        if not found:
            embed.description = "That player was not found in this season archive."
            return embed

        position, row = found
        stats = row.get("stats", {}).get("global", {"wins": 0, "losses": 0, "matches": 0})

        embed.add_field(name="Final Placement", value=f"`#{position}`", inline=True)
        embed.add_field(
            name="Final MMR",
            value=f"`{row.get('season_elo', 0)}` • {get_rank_emoji(final_rank_info['name'])}",
            inline=True
        )
        embed.add_field(name="Peak MMR", value=f"`{row.get('peak_elo', 0)}`", inline=True)

        embed.add_field(name="Wins", value=f"`{stats.get('wins', 0)}`", inline=True)
        embed.add_field(name="Losses", value=f"`{stats.get('losses', 0)}`", inline=True)
        embed.add_field(name="Matches", value=f"`{stats.get('matches', 0)}`", inline=True)

        embed.add_field(name="Level", value=f"`{row.get('level', 1)}`", inline=True)
        embed.add_field(name="XP", value=f"`{row.get('xp', 0)}`", inline=True)

        return embed

    async def build_season_leaderboard_embed(self, guild: discord.Guild, season_number: int, mode: str = "global", page: int = 1, per_page: int = 20) -> discord.Embed:
        archive = await season_archives_col.find_one({
            "guild_id": guild.id,
            "season_number": int(season_number)
        })

        if archive:
            rows = archive.get("leaderboard", [])
            embed = discord.Embed(
                title=f"🏆 Season {season_number} Leaderboard • {mode.upper()}",
                color=discord.Color.gold()
            )

            if mode != "global":
                filtered = []
                for row in rows:
                    stats = row.get("stats", {}).get(mode, {})
                    if stats.get("wins", 0) or stats.get("losses", 0) or stats.get("matches", 0):
                        filtered.append(row)
                rows = filtered

            total = len(rows)
            total_pages = max(1, (total + per_page - 1) // per_page)
            page = max(1, min(page, total_pages))
            chunk = rows[(page - 1) * per_page: page * per_page]

            lines = []
            for index, row in enumerate(chunk, start=(page - 1) * per_page + 1):
                rank_info = get_rank_info(row.get("season_elo", 0))
                lines.append(
                    f"**{index}.** <@{row['user_id']}> • "
                    f"`{row.get('season_elo', 0)} MMR` • {get_rank_emoji(rank_info['name'])}"
                )

            embed.description = "\n".join(lines) if lines else "No archived players."
            embed.set_footer(text=f"Page {page}/{total_pages} • Archived Season {season_number}")
            return embed

        # live season leaderboard
        mode = self.normalize_leaderboard_mode(mode)
        embed = discord.Embed(
            title=f"🏆 Season {season_number} Leaderboard • {mode.upper()}",
            color=discord.Color.gold()
        )

        ranked_profiles = []
        async for profile in season_profiles_col.find({
            "guild_id": guild.id,
            "season_number": int(season_number)
        }):
            include_profile = True
            if mode != "global":
                stats = profile.get("stats", {}).get(mode, {})
                include_profile = (
                    stats.get("wins", 0) > 0 or
                    stats.get("losses", 0) > 0 or
                    stats.get("matches", 0) > 0
                )
            if include_profile:
                ranked_profiles.append(profile)

        ranked_profiles.sort(
            key=lambda p: (p.get("season_elo", 0), p.get("peak_elo", 0)),
            reverse=True
        )

        total = len(ranked_profiles)
        total_pages = max(1, (total + per_page - 1) // per_page)
        page = max(1, min(page, total_pages))
        chunk = ranked_profiles[(page - 1) * per_page: page * per_page]

        lines = []
        for index, profile in enumerate(chunk, start=(page - 1) * per_page + 1):
            rank_info = get_rank_info(profile.get("season_elo", 0))
            lines.append(
                f"**{index}.** <@{profile['user_id']}> • "
                f"`{profile.get('season_elo', 0)} MMR` • "
                f"`{profile.get('peak_elo', 0)} peak` • "
                f"{get_rank_emoji(rank_info['name'])}"
            )

        embed.description = "\n".join(lines) if lines else "No season players yet."
        embed.set_footer(text=f"Page {page}/{total_pages} • Live Season {season_number}")
        return embed

    async def build_season_team_leaderboard_embed(self, guild: discord.Guild, season_number: int | None = None, page: int = 1, per_page: int = 10) -> discord.Embed:
        if season_number is None:
            season_number = await self.get_current_season_number(guild.id)

        archive = await season_archives_col.find_one({
            "guild_id": guild.id,
            "season_number": int(season_number)
        })

        if archive and archive.get("team_leaderboard") is not None:
            team_rows = archive.get("team_leaderboard", [])
        else:
            team_map = {}
            async for profile in season_profiles_col.find({
                "guild_id": guild.id,
                "season_number": int(season_number)
            }):
                team_id = profile.get("team_id")
                if not team_id:
                    continue

                bucket = team_map.setdefault(team_id, {
                    "team_id": team_id,
                    "members": [],
                    "total_points": 0,
                    "total_wins": 0,
                    "total_matches": 0,
                    "total_elo": 0,
                })

                bucket["members"].append(profile["user_id"])
                bucket["total_points"] += int(profile.get("team_points", 0))
                bucket["total_elo"] += int(profile.get("season_elo", 0))

                stats = profile.get("stats", {}).get("global", {})
                bucket["total_wins"] += int(stats.get("wins", 0))
                bucket["total_matches"] += int(stats.get("matches", 0))

            team_rows = list(team_map.values())
            team_rows.sort(
                key=lambda x: (x["total_points"], x["total_wins"], x["total_elo"]),
                reverse=True
            )

        total = len(team_rows)
        total_pages = max(1, (total + per_page - 1) // per_page)
        page = max(1, min(page, total_pages))
        chunk = team_rows[(page - 1) * per_page: page * per_page]

        embed = discord.Embed(
            title=f"🏆 Season {season_number} Team Leaderboard",
            color=discord.Color.gold()
        )

        if not chunk:
            embed.description = "No season teams yet."
            return embed

        lines = []
        for index, row in enumerate(chunk, start=(page - 1) * per_page + 1):
            lines.append(
                f"**{index}.** Team `{row['team_id']}` • "
                f"`{row['total_points']} pts` • "
                f"`{row['total_wins']} wins` • "
                f"`{len(row['members'])}` members"
            )

        embed.description = "\n".join(lines)
        embed.set_footer(text=f"Page {page}/{total_pages}")
        return embed

    async def build_team_leaderboard_embed(self, guild: discord.Guild, page: int = 1, per_page: int = 10) -> discord.Embed:
        teams = []
        async for team in teams_col.find({"guild_id": guild.id}):
            teams.append(team)

        teams.sort(
            key=lambda t: (
                int(t.get("points", 0)),
                int(t.get("stats", {}).get("wins", 0)),
                int(t.get("xp", 0)),
                int(t.get("level", 1))
            ),
            reverse=True
        )

        total = len(teams)
        total_pages = max(1, (total + per_page - 1) // per_page)
        page = max(1, min(page, total_pages))
        chunk = teams[(page - 1) * per_page: page * per_page]

        embed = discord.Embed(
            title="🏆 Team Leaderboard",
            color=discord.Color.gold()
        )

        if not chunk:
            embed.description = "No teams yet."
            return embed

        lines = []
        for index, team in enumerate(chunk, start=(page - 1) * per_page + 1):
            stats = team.get("stats", {})
            lines.append(
                f"**{index}.** **{team['name']}** • "
                f"`{team.get('points', 0)} pts` • "
                f"`Lv {team.get('level', 1)}` • "
                f"`{stats.get('wins', 0)}W-{stats.get('losses', 0)}L`"
            )

        embed.description = "\n".join(lines)
        embed.set_footer(text=f"Page {page}/{total_pages}")
        return embed

    async def get_team_leaderboard_total_pages(self, guild_id: int, per_page: int = 10) -> int:
        count = await teams_col.count_documents({"guild_id": guild_id})
        return max(1, (count + per_page - 1) // per_page)

    async def get_season_leaderboard_total_pages(self, guild_id: int, season_number: int, mode: str = "global", per_page: int = 20) -> int:
        archive = await season_archives_col.find_one({
            "guild_id": guild_id,
            "season_number": int(season_number)
        })
        if archive:
            rows = archive.get("leaderboard", [])
            if mode != "global":
                filtered = []
                for row in rows:
                    stats = row.get("stats", {}).get(mode, {})
                    if stats.get("wins", 0) or stats.get("losses", 0) or stats.get("matches", 0):
                        filtered.append(row)
                rows = filtered
            return max(1, (len(rows) + per_page - 1) // per_page)

        count = 0
        async for profile in season_profiles_col.find({
            "guild_id": guild_id,
            "season_number": int(season_number)
        }):
            if mode == "global":
                count += 1
            else:
                stats = profile.get("stats", {}).get(mode, {})
                if stats.get("wins", 0) or stats.get("losses", 0) or stats.get("matches", 0):
                    count += 1

        return max(1, (count + per_page - 1) // per_page)

    async def get_season_team_leaderboard_total_pages(self, guild_id: int, season_number: int | None = None, per_page: int = 10) -> int:
        if season_number is None:
            season_number = await self.get_current_season_number(guild_id)

        archive = await season_archives_col.find_one({
            "guild_id": guild_id,
            "season_number": int(season_number)
        })

        if archive and archive.get("team_leaderboard") is not None:
            count = len(archive.get("team_leaderboard", []))
            return max(1, (count + per_page - 1) // per_page)

        team_ids = set()
        async for profile in season_profiles_col.find({
            "guild_id": guild_id,
            "season_number": int(season_number)
        }):
            team_id = profile.get("team_id")
            if team_id:
                team_ids.add(team_id)

        return max(1, (len(team_ids) + per_page - 1) // per_page)

    def normalize_rank_role_key(self, rank_name: str) -> str:
        return rank_name.strip().lower()

    async def get_rank_roles_config(self, guild_id: int) -> dict:
        config = await self.get_config(guild_id)
        raw = config.get("rank_roles", {})
        if not isinstance(raw, dict):
            return {}
        return {str(k).lower(): v for k, v in raw.items()}

    async def get_rank_role_for_elo(self, guild: discord.Guild, elo: int):
        rank_info = get_rank_info(elo)
        rank_roles = await self.get_rank_roles_config(guild.id)
        role_id = rank_roles.get(self.normalize_rank_role_key(rank_info["name"]))
        if not role_id:
            return None
        return guild.get_role(role_id)

    async def sync_member_rank_role(self, guild: discord.Guild, member: discord.Member):
        rank_roles = await self.get_rank_roles_config(guild.id)
        if not rank_roles:
            return

        player = await self.get_player(member.id)
        current_rank_role = await self.get_rank_role_for_elo(guild, player.get("elo", 0))

        managed_rank_roles = []
        for _, role_id in rank_roles.items():
            role = guild.get_role(role_id)
            if role:
                managed_rank_roles.append(role)

        roles_to_remove = [role for role in managed_rank_roles if role in member.roles]
        roles_to_add = [current_rank_role] if current_rank_role and current_rank_role not in member.roles else []

        try:
            if roles_to_remove:
                await member.remove_roles(*roles_to_remove, reason="Ranked role sync")
            if roles_to_add:
                await member.add_roles(*roles_to_add, reason="Ranked role sync")
        except discord.HTTPException:
            pass

    async def sync_rank_roles_for_user_id(self, guild: discord.Guild, user_id: int):
        member = guild.get_member(user_id)
        if member:
            await self.sync_member_rank_role(guild, member)

    def normalize_mmr_history(self, player: dict) -> list:
        history = player.get("mmr_history")
        if not isinstance(history, list):
            history = []
            player["mmr_history"] = history
        return history

    def append_mmr_history_point(self, player: dict, mmr: int, reason: str, games_played: int | None = None):
        history = self.normalize_mmr_history(player)

        if games_played is None:
            games_played = player.get("stats", {}).get("global", {}).get("matches", 0)

        history.append({
            "mmr": int(mmr),
            "reason": reason,
            "games_played": int(games_played),
            "at": discord.utils.utcnow()
        })
        player["mmr_history"] = history[-100:]

    def build_mmr_graph_analytics(self, history: list[dict]) -> dict:
        cleaned = []

        for index, point in enumerate(history, start=1):
            mmr = point.get("mmr")
            if mmr is None:
                continue

            games_played = point.get("games_played")
            if games_played is None:
                games_played = index

            cleaned.append({
                "mmr": int(mmr),
                "games_played": int(games_played),
                "at": point.get("at"),
                "reason": point.get("reason", "Unknown")
            })

        if not cleaned:
            return {
                "points": 0,
                "start_mmr": 0,
                "end_mmr": 0,
                "peak_mmr": 0,
                "lowest_mmr": 0,
                "net_change": 0,
                "best_gain": 0,
                "worst_drop": 0,
                "avg_change": 0.0
            }

        mmrs = [p["mmr"] for p in cleaned]
        deltas = []

        for i in range(1, len(cleaned)):
            deltas.append(cleaned[i]["mmr"] - cleaned[i - 1]["mmr"])

        return {
            "points": len(cleaned),
            "start_mmr": cleaned[0]["mmr"],
            "end_mmr": cleaned[-1]["mmr"],
            "peak_mmr": max(mmrs),
            "lowest_mmr": min(mmrs),
            "net_change": cleaned[-1]["mmr"] - cleaned[0]["mmr"],
            "best_gain": max(deltas) if deltas else 0,
            "worst_drop": min(deltas) if deltas else 0,
            "avg_change": round(sum(deltas) / len(deltas), 2) if deltas else 0.0
        }

    async def build_mmr_graph_file(self, guild: discord.Guild, target_user_id: int, graph_type: str = "games"):
        player = await self.get_player(target_user_id)
        history = self.normalize_mmr_history(player)

        target_obj = await self.resolve_member_or_user(guild, target_user_id)
        target_name = target_obj.display_name if isinstance(target_obj, discord.Member) else (
            target_obj.name if target_obj else str(target_user_id)
        )

        if not history:
            return None, None

        cleaned = []
        for index, point in enumerate(history, start=1):
            mmr = point.get("mmr")
            if mmr is None:
                continue

            games_played = point.get("games_played")
            if games_played is None:
                games_played = index

            at = point.get("at")
            if hasattr(at, "tzinfo") and at is not None and at.tzinfo is None:
                at = at.replace(tzinfo=timezone.utc)

            cleaned.append({
                "mmr": int(mmr),
                "games_played": int(games_played),
                "at": at,
                "reason": point.get("reason", "Unknown")
            })

        if not cleaned:
            return None, None

        analytics = self.build_mmr_graph_analytics(cleaned)

        fig, ax = plt.subplots(figsize=(12, 4.5))
        fig.patch.set_facecolor("#041f4a")
        ax.set_facecolor("#041f4a")

        for spine in ax.spines.values():
            spine.set_color("#355a8a")

        ax.tick_params(axis="x", colors="white")
        ax.tick_params(axis="y", colors="white")
        ax.grid(True, alpha=0.25)

        graph_type = (graph_type or "games").lower()

        if graph_type == "time":
            x_vals = [p["at"] for p in cleaned if p["at"] is not None]
            y_vals = [p["mmr"] for p in cleaned if p["at"] is not None]

            if not x_vals or not y_vals:
                return None, None

            ax.plot(x_vals, y_vals, linewidth=2)
            ax.set_title(f"{target_name} • MMR vs Time", color="white", pad=12)
            ax.set_xlabel("Time", color="white")
            ax.set_ylabel("MMR", color="white")
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
            fig.autofmt_xdate()

        elif graph_type == "delta":
            if len(cleaned) < 2:
                return None, None

            x_vals = list(range(2, len(cleaned) + 1))
            y_vals = [cleaned[i]["mmr"] - cleaned[i - 1]["mmr"] for i in range(1, len(cleaned))]

            ax.plot(x_vals, y_vals, linewidth=2)
            ax.axhline(0, linewidth=1, alpha=0.5)
            ax.set_title(f"{target_name} • MMR Change per Game", color="white", pad=12)
            ax.set_xlabel("Games Played", color="white")
            ax.set_ylabel("MMR Change", color="white")

        elif graph_type == "movingavg":
            x_vals = [p["games_played"] for p in cleaned]
            y_vals = [p["mmr"] for p in cleaned]

            ax.plot(x_vals, y_vals, linewidth=1.8, alpha=0.5)

            window = 5
            moving = []
            moving_x = []
            for i in range(len(y_vals)):
                start = max(0, i - window + 1)
                chunk = y_vals[start:i + 1]
                moving.append(sum(chunk) / len(chunk))
                moving_x.append(x_vals[i])

            ax.plot(moving_x, moving, linewidth=2.4)
            ax.set_title(f"{target_name} • MMR vs Games Played (Moving Avg)", color="white", pad=12)
            ax.set_xlabel("Games Played", color="white")
            ax.set_ylabel("MMR", color="white")

        else:
            x_vals = [p["games_played"] for p in cleaned]
            y_vals = [p["mmr"] for p in cleaned]

            ax.plot(x_vals, y_vals, linewidth=2)
            ax.set_title(f"{target_name} • MMR vs Games Played", color="white", pad=12)
            ax.set_xlabel("Games Played", color="white")
            ax.set_ylabel("MMR", color="white")

        buf = io.BytesIO()
        plt.tight_layout()
        plt.savefig(buf, format="png", dpi=160, transparent=False)
        plt.close(fig)
        buf.seek(0)

        return discord.File(buf, filename="mmr_graph.png"), analytics

    async def build_season_graph_file(self, guild: discord.Guild, user_id: int, season_number: int | None = None, graph_type: str = "games"):
        if season_number is None:
            season_number = await self.get_current_season_number(guild.id)

        profile = await self.get_season_profile(guild.id, user_id, season_number)
        history = profile.get("mmr_history", [])
        if not history:
            return None, None

        target_obj = await self.resolve_member_or_user(guild, user_id)
        target_name = target_obj.display_name if isinstance(target_obj, discord.Member) else (
            target_obj.name if target_obj else str(user_id)
        )

        cleaned = []
        for index, point in enumerate(history, start=1):
            mmr = point.get("mmr")
            if mmr is None:
                continue

            games_played = point.get("games_played")
            if games_played is None:
                games_played = index

            at = point.get("at")
            if hasattr(at, "tzinfo") and at is not None and at.tzinfo is None:
                at = at.replace(tzinfo=timezone.utc)

            cleaned.append({
                "mmr": int(mmr),
                "games_played": int(games_played),
                "at": at,
                "reason": point.get("reason", "Unknown")
            })

        if not cleaned:
            return None, None

        analytics = self.build_mmr_graph_analytics(cleaned)

        fig, ax = plt.subplots(figsize=(12, 4.5))
        fig.patch.set_facecolor("#041f4a")
        ax.set_facecolor("#041f4a")

        for spine in ax.spines.values():
            spine.set_color("#355a8a")

        ax.tick_params(axis="x", colors="white")
        ax.tick_params(axis="y", colors="white")
        ax.grid(True, alpha=0.25)

        graph_type = (graph_type or "games").lower()

        if graph_type == "time":
            x_vals = [p["at"] for p in cleaned if p["at"] is not None]
            y_vals = [p["mmr"] for p in cleaned if p["at"] is not None]
            if not x_vals or not y_vals:
                return None, None

            ax.plot(x_vals, y_vals, linewidth=2)
            ax.set_title(f"{target_name} • Season {season_number} MMR vs Time", color="white", pad=12)
            ax.set_xlabel("Time", color="white")
            ax.set_ylabel("MMR", color="white")
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
            fig.autofmt_xdate()
        else:
            x_vals = [p["games_played"] for p in cleaned]
            y_vals = [p["mmr"] for p in cleaned]
            ax.plot(x_vals, y_vals, linewidth=2)
            ax.set_title(f"{target_name} • Season {season_number} MMR vs Games", color="white", pad=12)
            ax.set_xlabel("Games Played", color="white")
            ax.set_ylabel("MMR", color="white")

        buf = io.BytesIO()
        plt.tight_layout()
        plt.savefig(buf, format="png", dpi=160, transparent=False)
        plt.close(fig)
        buf.seek(0)

        return discord.File(buf, filename="season_graph.png"), analytics

    async def build_graph_compare_file(self, guild: discord.Guild, user1_id: int, user2_id: int):
        p1 = await self.get_player(user1_id)
        p2 = await self.get_player(user2_id)

        h1 = self.normalize_mmr_history(p1)
        h2 = self.normalize_mmr_history(p2)

        if not h1 or not h2:
            return None

        def clean(history):
            cleaned = []
            for i, point in enumerate(history, start=1):
                mmr = point.get("mmr")
                if mmr is None:
                    continue
                gp = point.get("games_played", i)
                cleaned.append((gp, mmr))
            return cleaned

        c1 = clean(h1)
        c2 = clean(h2)

        if not c1 or not c2:
            return None

        u1 = await self.resolve_member_or_user(guild, user1_id)
        u2 = await self.resolve_member_or_user(guild, user2_id)

        name1 = u1.display_name if isinstance(u1, discord.Member) else (u1.name if u1 else str(user1_id))
        name2 = u2.display_name if isinstance(u2, discord.Member) else (u2.name if u2 else str(user2_id))

        fig, ax = plt.subplots(figsize=(12, 4.5))
        fig.patch.set_facecolor("#041f4a")
        ax.set_facecolor("#041f4a")

        for spine in ax.spines.values():
            spine.set_color("#355a8a")

        ax.tick_params(axis="x", colors="white")
        ax.tick_params(axis="y", colors="white")
        ax.grid(True, alpha=0.25)

        x1, y1 = zip(*c1)
        x2, y2 = zip(*c2)

        ax.plot(x1, y1, linewidth=2, label=name1)
        ax.plot(x2, y2, linewidth=2, linestyle="dashed", label=name2)

        ax.set_title(f"{name1} vs {name2} • MMR Comparison", color="white", pad=12)
        ax.set_xlabel("Games Played", color="white")
        ax.set_ylabel("MMR", color="white")

        legend = ax.legend()
        for text in legend.get_texts():
            text.set_color("white")

        buf = io.BytesIO()
        plt.tight_layout()
        plt.savefig(buf, format="png", dpi=160)
        plt.close(fig)
        buf.seek(0)

        return discord.File(buf, filename="graph_compare.png")

    async def send_ranked_log(
        self,
        guild: discord.Guild,
        title: str,
        description: str,
        *,
        color: discord.Color = discord.Color.blurple(),
        fields: list[tuple[str, str, bool]] | None = None
    ):
        channel = await self.get_ranked_logs_channel(guild)
        if not channel:
            return

        embed = discord.Embed(title=title, description=description, color=color)
        if fields:
            for name, value, inline in fields:
                embed.add_field(name=name, value=value, inline=inline)

        try:
            await channel.send(embed=embed)
        except discord.HTTPException:
            pass

    def get_required_cancel_votes(self, match: dict) -> int:
        total_players = len(match.get("players", []))
        return max(1, math.ceil(total_players * 0.85))

    def build_result_view_for_match(self, match: dict) -> ResultView:
        view = ResultView(self, match["players"], match["mode"], match["_id"])
        required_votes = self.get_required_cancel_votes(match)
        cancel_votes = len(match.get("cancel_match", {}).get("votes", []))
        view.update_cancel_label(cancel_votes, required_votes)
        return view

    async def apply_score_confirmation_vote(self, match_id: str, user_id: int, vote_type: str):
        if vote_type not in {"confirm", "decline"}:
            raise ValueError(f"Unsupported vote type: {vote_type}")

        base_filter = {
            "_id": match_id,
            "status": "ongoing",
            "score_submission": {"$ne": None},
        }

        await matches_col.update_one(
            base_filter,
            {
                "$pull": {
                    "score_confirmation.confirmations": user_id,
                    "score_confirmation.declines": user_id,
                }
            }
        )

        target_field = "score_confirmation.confirmations" if vote_type == "confirm" else "score_confirmation.declines"
        await matches_col.update_one(
            base_filter,
            {
                "$addToSet": {
                    target_field: user_id
                }
            }
        )

        return await matches_col.find_one({"_id": match_id})

    async def toggle_cancel_match_vote(self, match_id: str, voter_id: int):
        removed = await matches_col.update_one(
            {
                "_id": match_id,
                "status": "ongoing",
                "cancel_match.votes": voter_id
            },
            {
                "$pull": {
                    "cancel_match.votes": voter_id
                }
            }
        )

        if removed.modified_count:
            updated_match = await matches_col.find_one({"_id": match_id})
            return updated_match, "removed your cancel match vote"

        added = await matches_col.update_one(
            {
                "_id": match_id,
                "status": "ongoing",
                "cancel_match.votes": {"$ne": voter_id}
            },
            {
                "$addToSet": {
                    "cancel_match.votes": voter_id
                }
            }
        )

        if added.modified_count:
            updated_match = await matches_col.find_one({"_id": match_id})
            return updated_match, "voted to cancel the match"

        return await matches_col.find_one({"_id": match_id}), None

    async def find_match_for_context(self, ctx, match_id: str | None = None):
        if match_id:
            return await matches_col.find_one({"_id": match_id, "guild_id": ctx.guild.id})

        if isinstance(ctx.channel, discord.Thread):
            return await matches_col.find_one({
                "guild_id": ctx.guild.id,
                "thread_id": ctx.channel.id,
                "status": "ongoing"
            })

        return None

    async def get_active_ranked_ban(self, guild_id: int, user_id: int):
        ban = await ranked_bans_col.find_one({
            "guild_id": guild_id,
            "user_id": user_id,
            "active": True
        })

        if not ban:
            return None

        expires_at = ban.get("expires_at")
        if expires_at is not None:
            if not hasattr(expires_at, "tzinfo"):
                expires_at = None
            elif expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)

        now = discord.utils.utcnow()

        if expires_at is not None and expires_at <= now:
            await ranked_bans_col.update_one(
                {"_id": ban["_id"]},
                {"$set": {"active": False, "expired_automatically": True}}
            )
            return None

        return ban

    async def is_ranked_banned(self, guild_id: int, user_id: int) -> bool:
        ban = await self.get_active_ranked_ban(guild_id, user_id)
        return ban is not None

    def format_ban_expiry(self, expires_at) -> str:
        if expires_at is None:
            return "Permanent"

        if not hasattr(expires_at, "tzinfo"):
            return str(expires_at)

        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)

        return discord.utils.format_dt(expires_at, style="F")

    async def build_ranked_ban_list_embed(self, guild: discord.Guild) -> discord.Embed:
        embed = discord.Embed(
            title="⛔ Ranked Ban List",
            color=discord.Color.red()
        )

        lines = []

        async for ban in ranked_bans_col.find({
            "guild_id": guild.id,
            "active": True
        }).sort("created_at", -1):
            user_id = ban.get("user_id")
            reason = ban.get("reason", "No reason")
            expires_at = ban.get("expires_at")
            expires_text = self.format_ban_expiry(expires_at)

            lines.append(
                f"<@{user_id}> • expires: {expires_text}\n"
                f"Reason: {reason}"
            )

        if not lines:
            embed.description = "No active ranked bans."
        else:
            embed.description = "\n\n".join(lines[:10])

        return embed

    async def build_player_report_history_embed(self, guild: discord.Guild, target_user_id: int) -> discord.Embed:
        target_obj = await self.resolve_member_or_user(guild, target_user_id)
        target_name = target_obj.display_name if isinstance(target_obj, discord.Member) else (
            target_obj.name if target_obj else str(target_user_id)
        )

        embed = discord.Embed(
            title=f"📄 Player Report History • {target_name}",
            color=discord.Color.red()
        )

        reports = []
        async for report in ranked_reports_col.find({
            "guild_id": guild.id,
            "reported_user_id": target_user_id,
            "type": "player_report"
        }).sort("created_at", -1):
            reports.append(report)

        if not reports:
            embed.description = "No player reports found for this user."
            return embed

        lines = []
        for report in reports[:10]:
            created_at = report.get("created_at")
            created_text = discord.utils.format_dt(created_at, style="R") if created_at else "Unknown time"
            reason = report.get("reason", "No reason")
            reporter_id = report.get("reporter_id")

            lines.append(
                f"**{report.get('_id', 'unknown')}** • by <@{reporter_id}> • {created_text}\n"
                f"Reason: {reason}"
            )

        embed.add_field(name="Total Reports", value=f"`{len(reports)}`", inline=True)
        embed.add_field(name="Showing", value=f"`{min(len(reports), 10)}` most recent", inline=True)
        embed.add_field(name="Reports", value="\n\n".join(lines), inline=False)

        return embed

    async def build_pending_invites_embed(self, guild: discord.Guild, user_id: int) -> discord.Embed:
        invites = []

        for party in self.parties.values():
            if party["guild_id"] != guild.id:
                continue

            await self.cleanup_expired_party_invites(party)

            for invite in self.normalize_party_invites(party):
                if invite.get("invited_user_id") != user_id:
                    continue
                if invite.get("status") != "pending":
                    continue
                if self.is_party_invite_expired(invite):
                    continue

                invites.append((party, invite))

        embed = discord.Embed(
            title="📨 Pending Party Invites",
            color=discord.Color.blurple()
        )

        if not invites:
            embed.description = "You have no pending party invites."
            return embed

        lines = []
        for party, invite in invites[:10]:
            lines.append(
                f"**Invite ID:** `{invite['_id']}`\n"
                f"**Party ID:** `{party['_id']}`\n"
                f"**Owner:** <@{party['owner_id']}>\n"
                f"**Expires:** {discord.utils.format_dt(invite['expires_at'], style='R')}"
            )

        embed.description = "\n\n".join(lines)
        return embed

    async def build_party_queue_embed(self, guild: discord.Guild, user_id: int) -> discord.Embed:
        embed = discord.Embed(
            title="🎮 Party Queue Status",
            color=discord.Color.blurple()
        )

        party = await self.get_party_for_user(guild.id, user_id)
        if not party:
            embed.description = "You are not in a party."
            return embed

        for mode, entries in self.get_guild_queue_state(guild.id).items():
            for entry in entries:
                if entry.get("party_id") == party["_id"]:
                    embed.add_field(name="Party ID", value=f"`{party['_id']}`", inline=False)
                    embed.add_field(name="Queue Mode", value=mode, inline=True)
                    embed.add_field(name="Party Size", value=str(len(entry.get("members", []))), inline=True)
                    embed.add_field(
                        name="Members",
                        value="\n".join(f"<@{m}>" for m in entry.get("members", [])),
                        inline=False
                    )
                    return embed

        embed.description = "Your party is not currently queued."
        return embed

    async def build_party_owner_embed(self, guild: discord.Guild, user_id: int) -> discord.Embed:
        embed = discord.Embed(
            title="👑 Party Owner",
            color=discord.Color.blurple()
        )

        party = await self.get_party_for_user(guild.id, user_id)
        if not party:
            embed.description = "You are not in a party."
            return embed

        embed.add_field(name="Party ID", value=f"`{party['_id']}`", inline=False)
        embed.add_field(name="Owner", value=f"<@{party['owner_id']}>", inline=False)
        return embed

    async def build_recent_reports_embed(self, guild: discord.Guild, reporter_id: int) -> discord.Embed:
        embed = discord.Embed(
            title="📝 Your Recent Reports",
            color=discord.Color.red()
        )

        reports = []
        async for report in ranked_reports_col.find({
            "guild_id": guild.id,
            "reporter_id": reporter_id,
            "type": "player_report"
        }).sort("created_at", -1):
            reports.append(report)

        if not reports:
            embed.description = "You have not submitted any player reports."
            return embed

        lines = []
        for report in reports[:10]:
            created_at = report.get("created_at")
            created_text = discord.utils.format_dt(created_at, style="R") if created_at else "Unknown time"
            lines.append(
                f"`{report['_id']}` • against <@{report['reported_user_id']}> • {created_text}\n"
                f"Reason: {report.get('reason', 'No reason')}"
            )

        embed.description = "\n\n".join(lines)
        return embed

    async def build_peak_embed(self, guild: discord.Guild, target_user_id: int) -> discord.Embed:
        player = await self.get_player(target_user_id)
        target_obj = await self.resolve_member_or_user(guild, target_user_id)
        target_name = target_obj.display_name if isinstance(target_obj, discord.Member) else (
            target_obj.name if target_obj else str(target_user_id)
        )

        history = self.normalize_mmr_history(player)
        peak_mmr = max([point.get("mmr", 0) for point in history], default=player.get("elo", 0))
        peak_rank = get_rank_info(peak_mmr)

        embed = discord.Embed(
            title=f"📈 Peak Ranked Stats • {target_name}",
            color=discord.Color.gold()
        )
        embed.add_field(name="Peak MMR", value=f"`{peak_mmr}`", inline=True)
        embed.add_field(name="Peak Rank", value=format_rank_display(peak_rank["name"]), inline=True)
        embed.add_field(name="Current MMR", value=f"`{player.get('elo', 0)}`", inline=True)
        return embed

    async def build_graph_compare_embed(self, guild: discord.Guild, user1_id: int, user2_id: int) -> discord.Embed:
        p1 = await self.get_player(user1_id)
        p2 = await self.get_player(user2_id)

        u1 = await self.resolve_member_or_user(guild, user1_id)
        u2 = await self.resolve_member_or_user(guild, user2_id)

        name1 = u1.display_name if isinstance(u1, discord.Member) else (u1.name if u1 else str(user1_id))
        name2 = u2.display_name if isinstance(u2, discord.Member) else (u2.name if u2 else str(user2_id))

        h1 = self.build_mmr_graph_analytics(self.normalize_mmr_history(p1))
        h2 = self.build_mmr_graph_analytics(self.normalize_mmr_history(p2))

        embed = discord.Embed(
            title=f"📊 Graph Compare • {name1} vs {name2}",
            color=discord.Color.blurple()
        )

        embed.add_field(
            name=name1,
            value=(
                f"MMR: `{p1.get('elo', 0)}` • {get_rank_emoji(get_rank_info(p1.get('elo', 0))['name'])}\n"
                f"Peak: `{h1['peak_mmr']}`\n"
                f"Net: `{h1['net_change']:+}`\n"
                f"Best Gain: `{h1['best_gain']:+}`\n"
                f"Worst Drop: `{h1['worst_drop']}`"
            ),
            inline=True
        )

        embed.add_field(
            name=name2,
            value=(
                f"MMR: `{p2.get('elo', 0)}` • {get_rank_emoji(get_rank_info(p2.get('elo', 0))['name'])}\n"
                f"Peak: `{h2['peak_mmr']}`\n"
                f"Net: `{h2['net_change']:+}`\n"
                f"Best Gain: `{h2['best_gain']:+}`\n"
                f"Worst Drop: `{h2['worst_drop']}`"
            ),
            inline=True
        )

        return embed

    async def build_my_bans_embed(self, guild: discord.Guild, user_id: int, page: int = 1, per_page: int = 5) -> discord.Embed:
        bans = []
        async for ban in ranked_bans_col.find({
            "guild_id": guild.id,
            "user_id": user_id
        }).sort("created_at", -1):
            bans.append(ban)

        total_pages = max(1, math.ceil(len(bans) / per_page))
        page = max(1, min(page, total_pages))
        start = (page - 1) * per_page
        chunk = bans[start:start + per_page]

        embed = discord.Embed(
            title="⛔ Your Ranked Ban History",
            color=discord.Color.red()
        )

        if not chunk:
            embed.description = "No ranked bans found."
            return embed

        lines = []
        for ban in chunk:
            created_at = ban.get("created_at")
            created_text = discord.utils.format_dt(created_at, style="R") if created_at else "Unknown time"
            expires_text = self.format_ban_expiry(ban.get("expires_at"))
            status = "Active" if ban.get("active") else "Inactive"
            lines.append(
                f"**{status}** • {created_text}\n"
                f"Expires: {expires_text}\n"
                f"Reason: {ban.get('reason', 'No reason')}"
            )

        embed.description = "\n\n".join(lines)
        embed.set_footer(text=f"Page {page}/{total_pages} • {len(bans)} total bans")
        return embed

    async def get_my_bans_total_pages(self, guild_id: int, user_id: int, per_page: int = 5) -> int:
        count = await ranked_bans_col.count_documents({
            "guild_id": guild_id,
            "user_id": user_id
        })
        return max(1, math.ceil(count / per_page))

    async def build_party_info_embed(self, guild: discord.Guild, party_id: str) -> discord.Embed | None:
        party = await parties_col.find_one({"_id": party_id, "guild_id": guild.id})
        if not party:
            return None

        party = self.normalize_party_doc(party)
        if not party:
            return None

        embed = discord.Embed(
            title="👥 Party Info",
            color=discord.Color.blurple()
        )
        embed.add_field(name="Party ID", value=f"`{party['_id']}`", inline=False)
        embed.add_field(name="Owner", value=f"<@{party['owner_id']}>", inline=True)
        embed.add_field(name="Size", value=str(len(party["members"])), inline=True)
        embed.add_field(name="Members", value="\n".join(f"<@{m}>" for m in party["members"]), inline=False)

        if party.get("last_activity_at"):
            embed.add_field(
                name="Last Activity",
                value=discord.utils.format_dt(party["last_activity_at"], style="R"),
                inline=False
            )

        return embed

    async def build_party_list_embed(self, guild: discord.Guild, page: int = 1, per_page: int = 10) -> discord.Embed:
        parties = []
        async for doc in parties_col.find({"guild_id": guild.id}).sort("created_at", -1):
            normalized = self.normalize_party_doc(doc)
            if normalized:
                parties.append(normalized)

        total_pages = max(1, math.ceil(len(parties) / per_page))
        page = max(1, min(page, total_pages))
        start = (page - 1) * per_page
        chunk = parties[start:start + per_page]

        embed = discord.Embed(
            title="👥 Active Ranked Parties",
            color=discord.Color.blurple()
        )

        if not chunk:
            embed.description = "No active parties."
            return embed

        lines = []
        for party in chunk:
            lines.append(
                f"`{party['_id']}` • owner <@{party['owner_id']}> • "
                f"`{len(party['members'])}` member(s)"
            )

        embed.description = "\n".join(lines)
        embed.set_footer(text=f"Page {page}/{total_pages} • {len(parties)} total parties")
        return embed

    async def get_party_list_total_pages(self, guild_id: int, per_page: int = 10) -> int:
        count = await parties_col.count_documents({"guild_id": guild_id})
        return max(1, math.ceil(count / per_page))

    async def build_warns_embed(self, guild: discord.Guild, target_user_id: int) -> discord.Embed:
        target_obj = await self.resolve_member_or_user(guild, target_user_id)
        target_name = target_obj.display_name if isinstance(target_obj, discord.Member) else (
            target_obj.name if target_obj else str(target_user_id)
        )

        warns = []
        async for warn in ranked_warns_col.find({
            "guild_id": guild.id,
            "user_id": target_user_id
        }).sort("created_at", -1):
            warns.append(warn)

        embed = discord.Embed(
            title=f"⚠️ Ranked Warns • {target_name}",
            color=discord.Color.orange()
        )

        if not warns:
            embed.description = "No warns found."
            return embed

        lines = []
        for warn in warns[:10]:
            created_at = warn.get("created_at")
            created_text = discord.utils.format_dt(created_at, style="R") if created_at else "Unknown time"
            moderator_id = warn.get("moderator_id")
            lines.append(
                f"`{warn['_id']}` • by <@{moderator_id}> • {created_text}\n"
                f"Reason: {warn.get('reason', 'No reason')}"
            )

        embed.add_field(name="Total Warns", value=f"`{len(warns)}`", inline=True)
        embed.add_field(name="Showing", value=f"`{min(len(warns), 10)}` most recent", inline=True)
        embed.add_field(name="Warn Entries", value="\n\n".join(lines), inline=False)
        return embed

    async def build_warns_leaderboard_embed(self, guild: discord.Guild, page: int = 1, per_page: int = 15) -> discord.Embed:
        counter = Counter()

        async for warn in ranked_warns_col.find({"guild_id": guild.id}):
            counter[warn.get("user_id")] += 1

        ranked = sorted(counter.items(), key=lambda x: x[1], reverse=True)
        total_pages = max(1, math.ceil(len(ranked) / per_page))
        page = max(1, min(page, total_pages))
        start = (page - 1) * per_page
        chunk = ranked[start:start + per_page]

        embed = discord.Embed(
            title="⚠️ Warned Users Leaderboard",
            color=discord.Color.orange()
        )

        if not chunk:
            embed.description = "No warned users."
            return embed

        lines = []
        for index, (user_id, total_warns) in enumerate(chunk, start=start + 1):
            lines.append(f"**{index}.** <@{user_id}> • `{total_warns}` warns")

        embed.description = "\n".join(lines)
        embed.set_footer(text=f"Page {page}/{total_pages} • {len(ranked)} warned users")
        return embed

    async def get_warns_leaderboard_total_pages(self, guild_id: int, per_page: int = 15) -> int:
        counter = Counter()
        async for warn in ranked_warns_col.find({"guild_id": guild_id}):
            counter[warn.get("user_id")] += 1
        return max(1, math.ceil(len(counter) / per_page))

    async def reset_match_score_state(self, guild: discord.Guild, match_id: str) -> bool:
        match = await matches_col.find_one({"_id": match_id})
        if not match:
            return False

        await matches_col.update_one(
            {"_id": match_id},
            {
                "$set": {
                    "score_submission": None,
                    "score_confirmation": {
                        "target_team": None,
                        "confirmations": [],
                        "declines": []
                    },
                    "confirmation_message_id": None
                }
            }
        )

        thread = await self.resolve_thread(guild, match.get("thread_id"))
        if thread:
            confirmation_message_id = match.get("confirmation_message_id")
            if confirmation_message_id:
                try:
                    confirmation_message = await thread.fetch_message(confirmation_message_id)
                    await confirmation_message.edit(view=None)
                except discord.NotFound:
                    pass
                except discord.HTTPException:
                    pass

            result_message_id = match.get("result_message_id")
            if result_message_id:
                try:
                    refreshed_match = await matches_col.find_one({"_id": match_id})
                    if refreshed_match:
                        result_view = self.build_result_view_for_match(refreshed_match)
                        result_message = await thread.fetch_message(result_message_id)
                        await result_message.edit(view=result_view)
                except discord.NotFound:
                    pass
                except discord.HTTPException:
                    pass

            reset_embed = discord.Embed(
                title="🔄 Score Reset",
                description="The score submission was reset. A new score can now be submitted.",
                color=discord.Color.orange()
            )
            await thread.send(embed=reset_embed)

        await self.update_match_visuals(guild, match_id)
        return True

    def build_result_panel_embed(self, match: dict) -> discord.Embed:
        mode = match.get("mode", "unknown")
        match_id = match.get("_id", "unknown")
        team1 = match.get("teams", {}).get("team1", [])
        team2 = match.get("teams", {}).get("team2", [])

        embed = discord.Embed(
            title=f"⚔️ Match Thread • {mode}",
            description="One player submits the score first. Then the opposing team confirms or declines it.",
            color=discord.Color.blurple()
        )
        embed.add_field(name="Match ID", value=f"`{match_id}`", inline=False)
        embed.add_field(name="Team A", value=", ".join(f"<@{p}>" for p in team1) or "Unknown", inline=False)
        embed.add_field(name="Team B", value=", ".join(f"<@{p}>" for p in team2) or "Unknown", inline=False)
        return embed

    async def refresh_match_panels(self, guild: discord.Guild, match_id: str) -> bool:
        match = await matches_col.find_one({"_id": match_id})
        if not match:
            return False

        thread = await self.resolve_thread(guild, match.get("thread_id"))
        if not thread:
            return False

        result_message_id = match.get("result_message_id")
        confirmation_message_id = match.get("confirmation_message_id")

        if match.get("status") != "ongoing":
            return True

        if match.get("score_submission") is None:
            if result_message_id:
                try:
                    result_message = await thread.fetch_message(result_message_id)
                    await result_message.edit(
                        embed=self.build_result_panel_embed(match),
                        view=self.build_result_view_for_match(match)
                    )
                except discord.NotFound:
                    pass
                except discord.HTTPException:
                    pass
        else:
            if confirmation_message_id:
                target_team = match.get("score_confirmation", {}).get("target_team")

                if target_team == "team1":
                    required_votes = len(match.get("teams", {}).get("team1", []))
                elif target_team == "team2":
                    required_votes = len(match.get("teams", {}).get("team2", []))
                else:
                    required_votes = 0

                try:
                    confirmation_message = await thread.fetch_message(confirmation_message_id)
                    await confirmation_message.edit(
                        view=ScoreConfirmationView(self, match_id, required_votes=required_votes)
                    )
                except discord.NotFound:
                    pass
                except discord.HTTPException:
                    pass

        return True

    async def rebuild_match_panel_core(self, guild: discord.Guild, match_id: str) -> tuple[bool, str]:
        match = await matches_col.find_one({"_id": match_id})
        if not match:
            return False, "Match not found."

        if match.get("status") != "ongoing":
            return False, "Only ongoing matches can rebuild the live result panel."

        if match.get("score_submission") is not None:
            return False, "Cannot rebuild the result panel while a score is awaiting confirmation."

        thread = await self.resolve_thread(guild, match.get("thread_id"))
        if not thread:
            return False, "Match thread could not be found."

        old_result_message_id = match.get("result_message_id")
        if old_result_message_id:
            try:
                old_result_message = await thread.fetch_message(old_result_message_id)
                await old_result_message.edit(view=None)
            except discord.NotFound:
                pass
            except discord.HTTPException:
                pass

        new_result_message = await thread.send(
            embed=self.build_result_panel_embed(match),
            view=self.build_result_view_for_match(match)
        )

        await matches_col.update_one(
            {"_id": match_id},
            {
                "$set": {
                    "result_message_id": new_result_message.id
                }
            }
        )

        return True, "Result panel rebuilt."

    async def reset_match_votes_core(self, guild: discord.Guild, match_id: str) -> tuple[bool, str]:
        match = await matches_col.find_one({"_id": match_id})
        if not match:
            return False, "Match not found."

        await matches_col.update_one(
            {"_id": match_id},
            {
                "$set": {
                    "cancel_match.votes": [],
                    "score_confirmation.confirmations": [],
                    "score_confirmation.declines": []
                }
            }
        )

        refreshed = await matches_col.find_one({"_id": match_id})
        if not refreshed:
            return False, "Match not found after update."

        thread = await self.resolve_thread(guild, refreshed.get("thread_id"))
        if thread and refreshed.get("confirmation_message_id"):
            target_team = refreshed.get("score_confirmation", {}).get("target_team")
            if target_team == "team1":
                required_votes = len(refreshed.get("teams", {}).get("team1", []))
            elif target_team == "team2":
                required_votes = len(refreshed.get("teams", {}).get("team2", []))
            else:
                required_votes = 0

            try:
                confirmation_message = await thread.fetch_message(refreshed.get("confirmation_message_id"))
                await confirmation_message.edit(
                    view=ScoreConfirmationView(self, match_id, required_votes=required_votes)
                )
            except discord.NotFound:
                pass
            except discord.HTTPException:
                pass

        await self.refresh_match_panels(guild, match_id)
        return True, "Match votes reset."

    async def reopen_match_core(self, guild: discord.Guild, match_id: str) -> tuple[bool, str]:
        match = await matches_col.find_one({"_id": match_id})
        if not match:
            return False, "Match not found."

        if match.get("status") != "cancelled":
            return False, "Only cancelled matches can be reopened."

        await matches_col.update_one(
            {"_id": match_id},
            {
                "$set": {
                    "status": "ongoing",
                    "finished_at": None,
                    "winner": None,
                    "loser": None,
                    "score": None,
                    "score_submission": None,
                    "score_confirmation": {
                        "target_team": None,
                        "confirmations": [],
                        "declines": []
                    },
                    "confirmation_message_id": None,
                    "cancel_match": {
                        "votes": []
                    }
                }
            }
        )

        ok, message = await self.rebuild_match_panel_core(guild, match_id)
        if not ok:
            return False, message

        await self.update_match_visuals(guild, match_id)
        return True, "Match reopened."

    async def submit_match_report(
        self,
        guild: discord.Guild,
        match: dict,
        reporter_id: int,
        reason: str,
        thread: discord.Thread | None = None
    ) -> bool:
        report_entry = {
            "reported_by": reporter_id,
            "reason": reason,
            "created_at": discord.utils.utcnow()
        }

        await matches_col.update_one(
            {"_id": match["_id"]},
            {
                "$push": {
                    "reports": report_entry
                }
            }
        )

        mode = match.get("mode", "unknown")
        config = await self.get_config(guild.id)
        log_channel_id = config.get("match_log_channels", {}).get(mode)
        log_channel = guild.get_channel(log_channel_id) if log_channel_id else None

        if thread is None:
            thread = await self.resolve_thread(guild, match.get("thread_id"))

        report_embed = discord.Embed(
            title="🚨 Match Report",
            color=discord.Color.red()
        )
        report_embed.add_field(name="Match ID", value=f"`{match['_id']}`", inline=False)
        report_embed.add_field(name="Reported By", value=f"<@{reporter_id}>", inline=False)
        report_embed.add_field(name="Reason", value=reason, inline=False)
        report_embed.add_field(
            name="Thread",
            value=f"[Open Thread]({thread.jump_url})" if thread else "Thread unavailable",
            inline=False
        )

        if log_channel:
            await log_channel.send(embed=report_embed)

        await self.send_ranked_log(
            guild,
            "🚨 Match Report",
            "A ranked match report was submitted.",
            color=discord.Color.red(),
            fields=[
                ("Match ID", f"`{match['_id']}`", False),
                ("Reported By", f"<@{reporter_id}>", True),
                ("Mode", mode, True),
                ("Reason", reason, False),
                ("Thread", f"[Open Thread]({thread.jump_url})" if thread else "Thread unavailable", False),
            ]
        )

        return True

    async def submit_player_report(
        self,
        guild: discord.Guild,
        reporter_id: int,
        reported_user_id: int,
        reason: str
    ) -> str:
        report_id = uuid.uuid4().hex[:12]

        doc = {
            "_id": report_id,
            "guild_id": guild.id,
            "reporter_id": reporter_id,
            "reported_user_id": reported_user_id,
            "reason": reason,
            "created_at": discord.utils.utcnow(),
            "type": "player_report"
        }

        await ranked_reports_col.insert_one(doc)

        await self.send_ranked_log(
            guild,
            "🚨 Player Report",
            f"New ranked player report created.",
            color=discord.Color.red(),
            fields=[
                ("Report ID", f"`{report_id}`", False),
                ("Reporter", f"<@{reporter_id}>", True),
                ("Reported Player", f"<@{reported_user_id}>", True),
                ("Reason", reason, False),
            ]
        )

        return report_id

    async def process_cancel_match_vote(
        self,
        guild: discord.Guild,
        match: dict,
        voter_id: int
    ):
        if match.get("status") != "ongoing":
            return False, "This match is no longer ongoing.", None

        if voter_id not in match.get("players", []):
            return False, "You are not part of this match.", None

        total_players = len(match.get("players", []))
        if total_players <= 0:
            return False, "Invalid match state.", None

        required_votes = self.get_required_cancel_votes(match)
        updated_match, action = await self.toggle_cancel_match_vote(match["_id"], voter_id)
        if not updated_match:
            return False, "The match state changed before your vote could be recorded.", None

        if updated_match.get("status") != "ongoing":
            return False, "This match is no longer ongoing.", None

        cancel_match = updated_match.get("cancel_match", {"votes": []})
        votes = list(cancel_match.get("votes", []))
        if action is None:
            return False, "Your vote could not be updated.", None

        if len(votes) >= required_votes:
            await self.cancel_match_core(
                guild,
                updated_match["_id"],
                cancelled_by_text="Cancelled by player vote.",
                score_text="CANCELLED BY VOTE"
            )
            return True, f"Match cancelled by vote (`{len(votes)}/{required_votes}`).", {
                "votes": len(votes),
                "required": required_votes,
                "cancelled": True
            }

        return True, f"You {action}. Current cancel votes: **{len(votes)}/{required_votes}**.", {
            "votes": len(votes),
            "required": required_votes,
            "cancelled": False
        }

    async def cancel_match_core(
        self,
        guild: discord.Guild,
        match_id: str,
        cancelled_by_text: str = "This match was cancelled.",
        score_text: str = "CANCELLED"
    ) -> bool:
        match = await matches_col.find_one({"_id": match_id})
        if not match:
            return False

        await matches_col.update_one(
            {"_id": match_id},
            {
                "$set": {
                    "status": "cancelled",
                    "finished_at": discord.utils.utcnow(),
                    "score_submission": None,
                    "score_confirmation": {
                        "target_team": None,
                        "confirmations": [],
                        "declines": []
                    },
                    "confirmation_message_id": None,
                    "score": score_text
                }
            }
        )

        thread = await self.resolve_thread(guild, match.get("thread_id"))
        if thread:
            confirmation_message_id = match.get("confirmation_message_id")
            if confirmation_message_id:
                try:
                    confirmation_message = await thread.fetch_message(confirmation_message_id)
                    await confirmation_message.edit(view=None)
                except discord.NotFound:
                    pass
                except discord.HTTPException:
                    pass

            result_message_id = match.get("result_message_id")
            if result_message_id:
                try:
                    result_message = await thread.fetch_message(result_message_id)
                    await result_message.edit(view=None)
                except discord.NotFound:
                    pass
                except discord.HTTPException:
                    pass

            cancel_embed = discord.Embed(
                title="⚫ Match Cancelled",
                description=cancelled_by_text,
                color=discord.Color.dark_grey()
            )
            cancel_embed.add_field(name="Match ID", value=f"`{match_id}`", inline=False)
            await thread.send(embed=cancel_embed)

        await self.update_match_visuals(guild, match_id)
        return True

    async def get_player(self, user_id):
        player = await players_col.find_one({"_id": user_id})
        if not player:
            player = {
                "_id": user_id,

                "elo": 0,
                "xp": 0,
                "level": 1,

                "stats": {
                    "global": {
                        "wins": 0,
                        "losses": 0,
                        "matches": 0
                    },
                    "1v1": {"wins": 0, "losses": 0, "matches": 0},
                    "2v2": {"wins": 0, "losses": 0, "matches": 0},
                    "3v3": {"wins": 0, "losses": 0, "matches": 0},
                    "4v4": {"wins": 0, "losses": 0, "matches": 0}
                },

                "streaks": {
                    "global": {"current": 0, "best": 0},
                    "1v1": {"current": 0, "best": 0},
                    "2v2": {"current": 0, "best": 0},
                    "3v3": {"current": 0, "best": 0},
                    "4v4": {"current": 0, "best": 0}
                },

                "history": [],  # last matches ids

                "placements": {
                    "completed": False,
                    "matches_played": 0,
                    "wins": 0,
                    "losses": 0
                },

                "mmr_history": [],
                "season_meta": {
                    "last_seen_season": SEASON_DEFAULT_NUMBER
                }
            }
            await players_col.insert_one(player)
        return player
    
    async def is_in_placements(self, guild_id: int, player: dict) -> bool:
        placements = player.get("placements", {})
        placement_games = await self.get_placement_games(guild_id)
        return not placements.get("completed", False) and placements.get("matches_played", 0) < placement_games
    
    def is_admin_member(self, member: discord.abc.User | discord.Member) -> bool:
        return isinstance(member, discord.Member) and member.guild_permissions.administrator


    async def ensure_ranked_channel_ctx(self, ctx: commands.Context) -> bool:
        if self.is_admin_member(ctx.author):
            return True

        config = await self.get_config(ctx.guild.id)
        allowed_channels = config.get("allowed_channels", [])

        if ctx.channel.id in allowed_channels:
            return True

        await ctx.send("This command can only be used in ranked channels.")
        return False

    def calculate_winrate(self, wins: int, matches: int) -> str:
        if matches <= 0:
            return "0%"
        return f"{round((wins / matches) * 100, 1)}%"

    def build_default_streaks(self) -> dict:
        return {
            "global": {"current": 0, "best": 0},
            "1v1": {"current": 0, "best": 0},
            "2v2": {"current": 0, "best": 0},
            "3v3": {"current": 0, "best": 0},
            "4v4": {"current": 0, "best": 0}
        }

    def normalize_player_streaks(self, player: dict) -> dict:
        streaks = player.get("streaks")

        if not isinstance(streaks, dict):
            streaks = self.build_default_streaks()
            player["streaks"] = streaks

        for mode in ("global", "1v1", "2v2", "3v3", "4v4"):
            mode_data = streaks.get(mode)
            if not isinstance(mode_data, dict):
                streaks[mode] = {"current": 0, "best": 0}
                continue

            mode_data.setdefault("current", 0)
            mode_data.setdefault("best", 0)

        return streaks

    def get_player_streak_data(self, player: dict, mode: str = "global") -> dict:
        streaks = self.normalize_player_streaks(player)
        return streaks.get(mode, {"current": 0, "best": 0})

    async def resolve_member_or_user(self, guild: discord.Guild, user_id: int):
        member = guild.get_member(user_id)
        if member:
            return member
        user = self.bot.get_user(user_id)
        if user:
            return user
        try:
            return await self.bot.fetch_user(user_id)
        except discord.HTTPException:
            return None

    async def resolve_thread(self, guild: discord.Guild, thread_id: int | None):
        if not thread_id:
            return None

        thread = guild.get_thread(thread_id)
        if isinstance(thread, discord.Thread):
            return thread

        channel = guild.get_channel(thread_id)
        if isinstance(channel, discord.Thread):
            return channel

        try:
            fetched = await self.bot.fetch_channel(thread_id)
            if isinstance(fetched, discord.Thread):
                return fetched
        except discord.HTTPException:
            return None

        return None

    def build_queue_embed(self, guild_id: int, mode: str) -> discord.Embed:
        queue_entries = self.get_guild_queue_state(guild_id).get(mode, [])
        queued_players = self.flatten_queue_members(guild_id, mode)
        team_size = int(mode[0])
        needed_players = team_size * 2

        embed = discord.Embed(
            title=f"🎮 {mode} Ranked Queue",
            description="Join or leave the queue using the buttons below."
        )
        embed.add_field(name="Players in Queue", value=f"{len(queued_players)}/{needed_players}", inline=True)
        embed.add_field(name="Queue Entries", value=str(len(queue_entries)), inline=True)
        embed.add_field(name="Match Size", value=f"{mode}", inline=True)

        if queue_entries:
            lines = []
            for entry in queue_entries[:20]:
                members = entry.get("members", [])
                if entry.get("party_id"):
                    lines.append(f"**Party ({len(members)})** • " + ", ".join(f"<@{m}>" for m in members))
                else:
                    lines.append(", ".join(f"<@{m}>" for m in members))
            embed.add_field(name="Queued Groups", value="\n".join(lines), inline=False)
        else:
            embed.add_field(name="Queued Groups", value="No players in queue.", inline=False)

        return embed
    
    async def refresh_queue_message(self, guild: discord.Guild, mode: str):
        config = await self.get_config(guild.id)
        channel_id = config.get("queue_channels", {}).get(mode)
        message_id = config.get("queue_messages", {}).get(mode)

        if not channel_id or not message_id:
            return

        channel = guild.get_channel(channel_id)
        if not channel:
            return

        try:
            message = await channel.fetch_message(message_id)
        except discord.NotFound:
            return
        except discord.HTTPException:
            return

        await message.edit(embed=self.build_queue_embed(guild.id, mode), view=QueueView(self, mode))

    async def recreate_queue_message(self, guild: discord.Guild, mode: str):
        config = await self.get_config(guild.id)
        channel_id = config.get("queue_channels", {}).get(mode)
        old_message_id = config.get("queue_messages", {}).get(mode)

        if not channel_id:
            return False

        channel = guild.get_channel(channel_id)
        if not channel:
            return False

        if old_message_id:
            try:
                old_message = await channel.fetch_message(old_message_id)
                await old_message.delete()
            except discord.NotFound:
                pass
            except discord.HTTPException:
                pass

        view = QueueView(self, mode)
        new_message = await channel.send(embed=self.build_queue_embed(guild.id, mode), view=view)

        config.setdefault("queue_messages", {})[mode] = new_message.id
        await config_col.update_one({"_id": guild.id}, {"$set": config})

        self.bot.add_view(QueueView(self, mode), message_id=new_message.id)
        return True

    def build_match_found_embed(
        self,
        mode: str,
        match_id: str,
        team1: list[int],
        team2: list[int],
        status_emoji: str = "🟢",
        status_label: str = "Active",
        thread_text: str | None = None,
        failed_to_add: list | None = None,
        ) -> discord.Embed:
        embed = discord.Embed(
            title=f"📋 {mode} Match Log",
            description=f"**Match ID:** `{match_id}`"
        )
        embed.add_field(name="Status", value=f"{status_emoji} {status_label}", inline=False)
        embed.add_field(name="Team A", value=", ".join(f"<@{p}>" for p in team1), inline=False)
        embed.add_field(name="Team B", value=", ".join(f"<@{p}>" for p in team2), inline=False)

        if thread_text:
            embed.add_field(name="Thread", value=thread_text, inline=False)

        if failed_to_add:
            embed.add_field(
                name="⚠️ Failed to Add",
                value="\n".join(f"<@{user_id}> — {reason}" for user_id, reason in failed_to_add[:10]),
                inline=False
            )

        return embed
    
    def build_match_rules_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title="📌 Match Rules & Info",
            description="Please read this before starting your match.",
            color=discord.Color.blurple()
        )

        embed.add_field(
            name="Rules",
            value=(
                "• Must send proof of every player in the match and the final score (screenshot or vid)\n"
                "• Players records the match (**highly recommended**).\n"
                "• If there are any real issues, ping an admin.\n"
                "-# Example: the other team lies or does not cooperate."
            ),
            inline=False
        )

        embed.add_field(
            name="Before You Start",
            value=(
                "Talk together and agree on a **server** and **arena** to play on.\n"
                "You can use this command:\n"
                "`howmanydrifters [region] [server abbreviation]`\n\n"
                "**Example:**\n"
                "`howmanydrifters EU EA`"
            ),
            inline=False
        )

        embed.add_field(
            name="Server Abbreviations",
            value=(
                "`ARES` • `BB` • `CH` • `COMP` • `CORE` • `EA` • `FR` • `GOLF` • `GUC` • `GWD` • "
                "`ODC` • `PC` • `RD` • `RF` • `SCRIMS` • `STRK` • `VRML`"
            ),
            inline=False
        )

        return embed

    async def update_match_visuals(self, guild: discord.Guild, match_id: str):
        match = await matches_col.find_one({"_id": match_id})
        if not match:
            return

        status_data = get_match_status_data(match)
        mode = match["mode"]
        team1 = match["teams"]["team1"]
        team2 = match["teams"]["team2"]

        thread = await self.resolve_thread(guild, match.get("thread_id"))
        thread_text = f"[Open Thread]({thread.jump_url})" if thread else "Thread unavailable"

        match_message_id = match.get("match_message_id")
        log_channel_id = (await self.get_config(guild.id)).get("match_log_channels", {}).get(mode)
        log_channel = guild.get_channel(log_channel_id) if log_channel_id else None

        if log_channel and match_message_id:
            try:
                match_message = await log_channel.fetch_message(match_message_id)
                await match_message.edit(
                    embed=self.build_match_found_embed(
                        mode=mode,
                        match_id=match_id,
                        team1=team1,
                        team2=team2,
                        status_emoji=status_data["emoji"],
                        status_label=status_data["label"],
                        thread_text=thread_text,
                    )
                )
            except discord.NotFound:
                pass
            except discord.HTTPException:
                pass

        if thread and isinstance(thread, discord.Thread):
            try:
                short_id = str(match_id)[:8]
                new_name = f"{status_data['emoji']} {mode} match • {short_id}"

                if thread.name != new_name:
                    print(f"[RANKED] Renaming thread {thread.id} from '{thread.name}' to '{new_name}'")
                    await thread.edit(name=new_name)
            except discord.HTTPException as e:
                print(
                    f"-------------------RANKED ERROR-------------------\n"
                    f"[RANKED] Failed to rename thread {thread.id}: {e}"
                )
    
    async def build_leaderboard_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title="🏆 ODFR Ranked Leaderboard",
            description="**Top 15 players**",
            color=discord.Color.gold()
        )

        medals = {
            1: "🥇",
            2: "🥈",
            3: "🥉"
        }

        lines = []
        rank = 1

        async for player in players_col.find().sort("elo", -1).limit(15):
            prefix = medals.get(rank, "🔹")
            rank_info = get_rank_info(player["elo"])
            lines.append(
                f"{prefix} **{rank}.** <@{player['_id']}> • `{player['elo']} ELO` • {get_rank_emoji(rank_info['name'])}"
            )
            rank += 1

        if not lines:
            embed.description = "No ranked players yet."
        else:
            embed.description = "\n".join(lines)

        embed.set_footer(text="Updates automatically every 15 minutes if rankings change.")
        return embed

    def normalize_leaderboard_mode(self, mode: str | None) -> str:
        if not mode:
            return "global"

        mode = mode.lower()
        allowed = {"global", "1v1", "2v2", "3v3", "4v4"}
        return mode if mode in allowed else "global"

    def normalize_leaderboard_metric(self, metric: str | None) -> str | None:
        if not metric:
            return "elo"

        metric = metric.lower().replace("-", "_")

        aliases = {
            "elo": "elo",
            "mmr": "elo",
            "rank": "elo",

            "xp": "xp",
            "level": "level",
            "lvl": "level",

            "streak": "live_streak",
            "live": "live_streak",
            "live_streak": "live_streak",
            "livestreak": "live_streak",
            "currentstreak": "live_streak",
            "current_streak": "live_streak",

            "best": "best_streak",
            "best_streak": "best_streak",
            "beststreak": "best_streak",
            "peakstreak": "best_streak",
            "peak_streak": "best_streak",
        }

        return aliases.get(metric)

    def parse_leaderboard_args(self, *args: str):
        mode = "global"
        metric = "elo"

        if not args:
            return mode, metric

        tokens = [arg.lower() for arg in args if arg]
        if not tokens:
            return mode, metric

        first = tokens[0]

        if first in {"global", "1v1", "2v2", "3v3", "4v4"}:
            mode = self.normalize_leaderboard_mode(first)

            if len(tokens) >= 2:
                parsed_metric = self.normalize_leaderboard_metric(tokens[1])
                if parsed_metric is None:
                    return None, None
                metric = parsed_metric
        else:
            parsed_metric = self.normalize_leaderboard_metric(first)
            if parsed_metric is None:
                return None, None
            metric = parsed_metric

        return mode, metric

    def get_leaderboard_metric_label(self, metric: str) -> str:
        labels = {
            "elo": "MMR",
            "xp": "XP",
            "level": "Level",
            "live_streak": "Live Streak",
            "best_streak": "Best Streak",
        }
        return labels.get(metric, "MMR")

    async def get_leaderboard_total_pages(self, mode: str, metric: str = "elo", per_page: int = 20) -> int:
        mode = self.normalize_leaderboard_mode(mode)

        count = 0
        async for player in players_col.find():
            if mode == "global":
                count += 1
            else:
                stats = player.get("stats", {}).get(mode, {})
                if stats.get("wins", 0) > 0 or stats.get("losses", 0) > 0 or stats.get("matches", 0) > 0:
                    count += 1

        return max(1, (count + per_page - 1) // per_page)

    async def build_mode_leaderboard_embed(self, mode: str, metric: str = "elo", page: int = 1, per_page: int = 20) -> discord.Embed:
        mode = self.normalize_leaderboard_mode(mode)
        metric = self.normalize_leaderboard_metric(metric) or "elo"
        metric_label = self.get_leaderboard_metric_label(metric)

        embed = discord.Embed(
            title=f"🏆 ODFR Leaderboard • {mode.upper()} • {metric_label}",
            color=discord.Color.gold()
        )

        medals = {1: "🥇", 2: "🥈", 3: "🥉"}
        lines = []
        ranked_players = []

        async for player in players_col.find():
            include_player = True

            if mode != "global":
                stats = player.get("stats", {}).get(mode, {})
                include_player = (
                    stats.get("wins", 0) > 0 or
                    stats.get("losses", 0) > 0 or
                    stats.get("matches", 0) > 0
                )

            if not include_player:
                continue

            streak_mode = "global" if mode == "global" else mode
            streak_data = self.get_player_streak_data(player, streak_mode)

            if metric == "live_streak":
                value = streak_data.get("current", 0)
            elif metric == "best_streak":
                value = streak_data.get("best", 0)
            elif metric == "xp":
                value = player.get("xp", 0)
            elif metric == "level":
                value = player.get("level", 1)
            else:
                value = player.get("elo", 0)

            ranked_players.append((player, value))

        ranked_players.sort(
            key=lambda x: (
                x[1],
                x[0].get("elo", 0),
                self.get_player_streak_data(x[0], "global").get("best", 0),
                self.get_player_streak_data(x[0], "global").get("current", 0),
            ),
            reverse=True
        )

        total_players = len(ranked_players)
        total_pages = max(1, (total_players + per_page - 1) // per_page)
        page = max(1, min(page, total_pages))

        start_index = (page - 1) * per_page
        end_index = start_index + per_page
        page_players = ranked_players[start_index:end_index]

        for index, (player, value) in enumerate(page_players, start=start_index + 1):
            prefix = medals.get(index, "🔹")
            rank_info = get_rank_info(player.get("elo", 0))

            streak_mode = "global" if mode == "global" else mode
            streak_data = self.get_player_streak_data(player, streak_mode)

            if metric == "live_streak":
                lines.append(
                    f"{prefix} **{index}.** <@{player['_id']}> • "
                    f"`{streak_data.get('current', 0)} live` • "
                    f"`{streak_data.get('best', 0)} best` • "
                    f"{get_rank_emoji(rank_info['name'])}"
                )
            elif metric == "best_streak":
                lines.append(
                    f"{prefix} **{index}.** <@{player['_id']}> • "
                    f"`{streak_data.get('best', 0)} best` • "
                    f"`{streak_data.get('current', 0)} live` • "
                    f"{get_rank_emoji(rank_info['name'])}"
                )
            elif metric == "xp":
                lines.append(
                    f"{prefix} **{index}.** <@{player['_id']}> • "
                    f"`{player.get('xp', 0)} XP` • "
                    f"`Level {player.get('level', 1)}` • "
                    f"**{rank_info['name']}**"
                )
            elif metric == "level":
                lines.append(
                    f"{prefix} **{index}.** <@{player['_id']}> • "
                    f"`Level {player.get('level', 1)}` • "
                    f"`{player.get('xp', 0)} XP` • "
                    f"**{rank_info['name']}**"
                )
            else:
                lines.append(
                    f"{prefix} **{index}.** <@{player['_id']}> • "
                    f"`{player.get('elo', 0)} ELO` • {get_rank_emoji(rank_info['name'])}"
                )

        if not lines:
            embed.description = "No ranked players yet."
        else:
            embed.description = "\n".join(lines)

        embed.set_footer(text=f"Page {page}/{total_pages} • {total_players} players total • {metric_label}")
        return embed

    async def build_profile_embed(self, guild: discord.Guild, target_user_id: int) -> discord.Embed:
        player = await self.get_player(target_user_id)
        stats = player.get("stats", {})
        global_stats = stats.get("global", {"wins": 0, "losses": 0, "matches": 0})
        placements = player.get("placements", {"completed": False, "matches_played": 0, "wins": 0, "losses": 0})

        target_obj = await self.resolve_member_or_user(guild, target_user_id)
        target_name = target_obj.display_name if isinstance(target_obj, discord.Member) else (
            target_obj.name if target_obj else str(target_user_id)
        )

        rank_info = get_rank_info(player.get("elo", 0))
        global_streak = self.get_player_streak_data(player, "global")
        avatar_url = target_obj.display_avatar.url if target_obj and getattr(target_obj, "display_avatar", None) else None

        mmr_history = self.normalize_mmr_history(player)
        peak_mmr = max([point.get("mmr", 0) for point in mmr_history], default=player.get("elo", 0))

        recent_match_ids = player.get("history", [])[-5:]
        recent_form = []
        for match_id in recent_match_ids:
            match = await matches_col.find_one({"_id": match_id})
            if not match:
                continue
            winners = match.get("winner", [])
            if target_user_id in winners:
                recent_form.append("W")
            elif match.get("status") == "cancelled":
                recent_form.append("C")
            else:
                recent_form.append("L")

        report_count = await ranked_reports_col.count_documents({
            "guild_id": guild.id,
            "reported_user_id": target_user_id,
            "type": "player_report"
        })

        embed = discord.Embed(
            title=f"👤 Ranked Profile • {target_name}",
            description="Player overview and ranked progression.",
            color=discord.Color.blurple()
        )

        if avatar_url:
            embed.set_thumbnail(url=avatar_url)

        embed.add_field(name="Rank", value=format_rank_display(rank_info["name"]), inline=True)
        embed.add_field(name="MMR", value=str(player.get("elo", 0)), inline=True)
        embed.add_field(name="Peak MMR", value=str(peak_mmr), inline=True)

        embed.add_field(name="Level", value=str(player.get("level", 1)), inline=True)
        embed.add_field(name="Live Streak", value=str(global_streak.get("current", 0)), inline=True)
        embed.add_field(name="Best Streak", value=str(global_streak.get("best", 0)), inline=True)

        embed.add_field(name="Progress", value=format_rank_progress(player.get("elo", 0)), inline=False)

        placement_games = await self.get_placement_games(guild.id)
        placement_text = (
            "Complete"
            if placements.get("completed", False)
            else f"{placements.get('matches_played', 0)}/{placement_games} • {placements.get('wins', 0)}W-{placements.get('losses', 0)}L"
        )

        embed.add_field(name="Placements", value=placement_text, inline=True)
        embed.add_field(name="Reports", value=str(report_count), inline=True)
        embed.add_field(name="Recent Form", value=" ".join(recent_form) if recent_form else "No recent matches", inline=True)

        embed.add_field(
            name="Global Record",
            value=(
                f"`{global_stats.get('wins', 0)}W` • "
                f"`{global_stats.get('losses', 0)}L` • "
                f"`{global_stats.get('matches', 0)}M` • "
                f"`{self.calculate_winrate(global_stats.get('wins', 0), global_stats.get('matches', 0))}`"
            ),
            inline=False
        )

        mode_lines = []
        for mode_name in ("1v1", "2v2", "3v3", "4v4"):
            mode_stats = stats.get(mode_name, {"wins": 0, "losses": 0, "matches": 0})
            streak_data = self.get_player_streak_data(player, mode_name)
            mode_lines.append(
                f"**{mode_name}** • "
                f"`{mode_stats.get('wins', 0)}W-{mode_stats.get('losses', 0)}L` • "
                f"`{streak_data.get('current', 0)} live / {streak_data.get('best', 0)} best`"
            )

        embed.add_field(name="Mode Breakdown", value="\n".join(mode_lines), inline=False)
        embed.set_footer(text="Use the mmrgraph command to view MMR progression.")
        return embed

    async def build_rank_progress_embed(self, guild: discord.Guild, target_user_id: int) -> discord.Embed:
        player = await self.get_player(target_user_id)
        rank_info = get_rank_info(player.get("elo", 0))

        target_obj = await self.resolve_member_or_user(guild, target_user_id)
        target_name = target_obj.display_name if isinstance(target_obj, discord.Member) else (
            target_obj.name if target_obj else str(target_user_id)
        )

        current_elo = player.get("elo", 0)
        next_rank_name = rank_info["next_rank"]
        next_rank_elo = rank_info["next_rank_elo"]

        if next_rank_elo is None:
            next_rank_text = "Max rank reached"
            remaining_text = "0"
        else:
            next_rank_text = next_rank_name
            remaining_text = str(max(0, next_rank_elo - current_elo))

        embed = discord.Embed(
            title=f"📈 Rank Progress • {target_name}",
            color=discord.Color.gold()
        )

        embed.add_field(name="Current Rank", value=rank_info["name"], inline=True)
        embed.add_field(name="Current MMR", value=str(current_elo), inline=True)
        embed.add_field(name="Next Rank", value=next_rank_text, inline=True)
        embed.add_field(name="MMR Remaining", value=remaining_text, inline=True)
        embed.add_field(name="Progress", value=format_rank_progress(current_elo), inline=False)

        return embed
    
    async def build_placement_embed(self, guild: discord.Guild, target_user_id: int) -> discord.Embed:
        player = await self.get_player(target_user_id)
        placements = player.get("placements", {
            "completed": False,
            "matches_played": 0,
            "wins": 0,
            "losses": 0
        })

        target_obj = await self.resolve_member_or_user(guild, target_user_id)
        target_name = target_obj.display_name if isinstance(target_obj, discord.Member) else (
            target_obj.name if target_obj else str(target_user_id)
        )

        embed = discord.Embed(
            title=f"🧭 Placements • {target_name}",
            color=discord.Color.blurple()
        )

        embed.add_field(
            name="Status",
            value="Complete" if placements.get("completed", False) else "In progress",
            inline=True
        )
        placement_games = await self.get_placement_games(guild.id)

        embed.add_field(
            name="Matches Played",
            value=f"{placements.get('matches_played', 0)}/{placement_games}",
            inline=True
        )
        embed.add_field(
            name="Record",
            value=f"{placements.get('wins', 0)}W-{placements.get('losses', 0)}L",
            inline=True
        )

        return embed

    async def build_compare_embed(self, guild: discord.Guild, user1_id: int, user2_id: int) -> discord.Embed:
        p1 = await self.get_player(user1_id)
        p2 = await self.get_player(user2_id)

        u1 = await self.resolve_member_or_user(guild, user1_id)
        u2 = await self.resolve_member_or_user(guild, user2_id)

        name1 = u1.display_name if isinstance(u1, discord.Member) else (u1.name if u1 else str(user1_id))
        name2 = u2.display_name if isinstance(u2, discord.Member) else (u2.name if u2 else str(user2_id))

        rank1 = get_rank_info(p1.get("elo", 0))
        rank2 = get_rank_info(p2.get("elo", 0))

        s1 = self.get_player_streak_data(p1, "global")
        s2 = self.get_player_streak_data(p2, "global")

        g1 = p1.get("stats", {}).get("global", {"wins": 0, "losses": 0, "matches": 0})
        g2 = p2.get("stats", {}).get("global", {"wins": 0, "losses": 0, "matches": 0})

        embed = discord.Embed(
            title="⚖️ Ranked Comparison",
            color=discord.Color.blurple()
        )

        embed.add_field(
            name=name1,
            value=(
                f"**Rank:** {rank1['name']}\n"
                f"**MMR:** `{p1.get('elo', 0)}`\n"
                f"**Record:** `{g1.get('wins', 0)}W-{g1.get('losses', 0)}L`\n"
                f"**Winrate:** `{self.calculate_winrate(g1.get('wins', 0), g1.get('matches', 0))}`\n"
                f"**Live / Best Streak:** `{s1.get('current', 0)}` / `{s1.get('best', 0)}`"
            ),
            inline=True
        )

        embed.add_field(
            name=name2,
            value=(
                f"**Rank:** {rank2['name']}\n"
                f"**MMR:** `{p2.get('elo', 0)}`\n"
                f"**Record:** `{g2.get('wins', 0)}W-{g2.get('losses', 0)}L`\n"
                f"**Winrate:** `{self.calculate_winrate(g2.get('wins', 0), g2.get('matches', 0))}`\n"
                f"**Live / Best Streak:** `{s2.get('current', 0)}` / `{s2.get('best', 0)}`"
            ),
            inline=True
        )

        mmr_diff = p1.get("elo", 0) - p2.get("elo", 0)
        if mmr_diff > 0:
            mmr_line = f"{name1} is ahead by `{mmr_diff}` MMR"
        elif mmr_diff < 0:
            mmr_line = f"{name2} is ahead by `{abs(mmr_diff)}` MMR"
        else:
            mmr_line = "Both players have the same MMR"

        embed.add_field(name="Difference", value=mmr_line, inline=False)
        return embed

    async def build_ranked_info_embed(self, guild_id: int) -> discord.Embed:
        embed = discord.Embed(
            title="ℹ️ Ranked System Info",
            color=discord.Color.blurple()
        )

        embed.add_field(
            name="Queues",
            value=(
                "Players queue through the queue panels for `1v1`, `2v2`, `3v3`, and `4v4`.\n"
                "When enough players are found, a private match thread is created."
            ),
            inline=False
        )

        embed.add_field(
            name="Match Flow",
            value=(
                "1. Players join queue\n"
                "2. Match thread is created\n"
                "3. One player submits score\n"
                "4. Opposing team confirms or declines\n"
                "5. Result is finalized"
            ),
            inline=False
        )

        placement_games = await self.get_placement_games(guild_id)

        embed.add_field(
            name="Placements",
            value=(
                f"Each player has `{placement_games} placement matches`.\n"
                "During placements, progress is tracked and an initial MMR is assigned when placements finish."
            ),
            inline=False
        )

        embed.add_field(
            name="MMR / Elo",
            value=(
                "MMR changes are calculated individually using the enemy team average as reference.\n"
                "Upsets give bigger gains, and losing to much lower MMR can punish more."
            ),
            inline=False
        )

        embed.add_field(
            name="Streaks",
            value=(
                "The system tracks both `live streaks` and `best streaks` globally and per mode."
            ),
            inline=False
        )

        embed.add_field(
            name="Useful Commands",
            value=(
                "`stats`, `profile`, `mmrgraph`, `rank`, `rankprogress`, `history`, `recent`, `lastmatch`, "
                "`leaderboard`, `queue`, `myqueue`, `mymatch`, `whereisqueue`, `compare`, "
                "`headtohead`, `placement`, `placementinfo`, `ranklist`, `rules`, `reporthistory`, "
                "`reportplayer`, `pingranked`, `rankedactivity`, `partycreate`, `partyinvite`, "
                "`partyaccept`, `partydecline`, `partykick`, `partytransfer`, `partyleave`, `partydisband`, `party`"
            ),
            inline=False
        )

        return embed

    async def build_activity_embed(self, guild: discord.Guild) -> discord.Embed:
        now = discord.utils.utcnow()
        last_24h = now - timedelta(hours=24)
        last_7d = now - timedelta(days=7)

        matches_24h = []
        matches_7d = []
        cancelled_7d = 0
        reports_7d = 0

        async for match in matches_col.find({"guild_id": guild.id}):
            finished_at = match.get("finished_at")
            if not finished_at:
                continue

            if not hasattr(finished_at, "tzinfo"):
                continue

            if finished_at.tzinfo is None:
                finished_at = finished_at.replace(tzinfo=timezone.utc)

            if finished_at >= last_7d:
                matches_7d.append(match)
                if match.get("status") == "cancelled":
                    cancelled_7d += 1
                reports_7d += len(match.get("reports", []))

            if finished_at >= last_24h:
                matches_24h.append(match)

        player_counter = Counter()
        mode_counter = Counter()
        active_players = set()

        for match in matches_7d:
            for user_id in match.get("players", []):
                player_counter[user_id] += 1
                active_players.add(user_id)
            mode_counter[match.get("mode", "unknown")] += 1

        finished_7d = sum(1 for m in matches_7d if m.get("status") == "finished")
        avg_per_day = round(len(matches_7d) / 7, 2) if matches_7d else 0

        embed = discord.Embed(
            title="📈 Ranked Activity",
            description="Ranked activity snapshot for the last 24 hours and 7 days.",
            color=discord.Color.blurple()
        )

        embed.add_field(name="Matches (24h)", value=f"`{len(matches_24h)}`", inline=True)
        embed.add_field(name="Matches (7d)", value=f"`{len(matches_7d)}`", inline=True)
        embed.add_field(name="Avg / Day", value=f"`{avg_per_day}`", inline=True)

        embed.add_field(name="Finished (7d)", value=f"`{finished_7d}`", inline=True)
        embed.add_field(name="Cancelled (7d)", value=f"`{cancelled_7d}`", inline=True)
        embed.add_field(name="Reports (7d)", value=f"`{reports_7d}`", inline=True)

        embed.add_field(name="Active Players (7d)", value=f"`{len(active_players)}`", inline=True)

        if mode_counter:
            mode_lines = []
            for mode, count in mode_counter.most_common(4):
                mode_lines.append(f"**{mode}** • `{count}`")
            embed.add_field(name="Mode Activity (7d)", value="\n".join(mode_lines), inline=False)
        else:
            embed.add_field(name="Mode Activity (7d)", value="No data.", inline=False)

        if player_counter:
            top_players = []
            for rank, (user_id, count) in enumerate(player_counter.most_common(10), start=1):
                top_players.append(f"**{rank}.** <@{user_id}> • `{count}` matches")
            embed.add_field(name="Most Active Players (7d)", value="\n".join(top_players), inline=False)
        else:
            embed.add_field(name="Most Active Players (7d)", value="No recent activity.", inline=False)

        return embed

    async def force_finalize_match(self, guild: discord.Guild, match_id: str, winning_team_key: str):
        match = await matches_col.find_one({"_id": match_id})
        if not match:
            return False, "Match not found."

        if match.get("status") != "ongoing":
            return False, "That match is not ongoing."

        if winning_team_key not in {"team1", "team2"}:
            return False, "Winning team must be `team1` or `team2`."

        if winning_team_key == "team1":
            submission = {
                "submitted_by": 0,
                "submitting_team": "team1",
                "target_team": "team2",
                "team1_score": 1,
                "team2_score": 0
            }
        else:
            submission = {
                "submitted_by": 0,
                "submitting_team": "team2",
                "target_team": "team1",
                "team1_score": 0,
                "team2_score": 1
            }

        await matches_col.update_one(
            {"_id": match_id},
            {
                "$set": {
                    "score_submission": submission,
                    "score_confirmation": {
                        "target_team": None,
                        "confirmations": [],
                        "declines": []
                    },
                    "confirmation_message_id": None
                }
            }
        )

        thread = await self.resolve_thread(guild, match.get("thread_id"))
        if thread is None:
            return False, "Match thread could not be found."

        await self.finalize_confirmed_score(guild, match_id, thread)
        return True, None

    async def build_debug_match_embed(self, guild: discord.Guild, match_id: str) -> discord.Embed | None:
        match = await matches_col.find_one({"_id": match_id})
        if not match:
            return None

        thread = await self.resolve_thread(guild, match.get("thread_id"))
        status_data = get_match_status_data(match)

        submission = match.get("score_submission")
        confirmation = match.get("score_confirmation", {})
        cancel_match = match.get("cancel_match", {"votes": []})
        reports = match.get("reports", [])
        required_cancel_votes = self.get_required_cancel_votes(match)

        embed = discord.Embed(
            title=f"🛠️ Debug Match • {match_id}",
            color=discord.Color.orange()
        )

        embed.add_field(name="Status", value=f"{status_data['emoji']} {status_data['label']}", inline=True)
        embed.add_field(name="Mode", value=match.get("mode", "unknown"), inline=True)
        embed.add_field(name="Score", value=str(match.get("score", "None")), inline=True)

        embed.add_field(
            name="Thread",
            value=f"[Open Thread]({thread.jump_url})" if thread else f"`{match.get('thread_id')}`",
            inline=False
        )

        embed.add_field(
            name="Message IDs",
            value=(
                f"Match Log: `{match.get('match_message_id')}`\n"
                f"Result: `{match.get('result_message_id')}`\n"
                f"Confirmation: `{match.get('confirmation_message_id')}`"
            ),
            inline=False
        )

        embed.add_field(
            name="Teams",
            value=(
                f"**Team A:** {', '.join(f'<@{p}>' for p in match.get('teams', {}).get('team1', [])) or 'None'}\n"
                f"**Team B:** {', '.join(f'<@{p}>' for p in match.get('teams', {}).get('team2', [])) or 'None'}"
            ),
            inline=False
        )

        embed.add_field(
            name="Score Submission",
            value=(
                f"`{submission}`" if submission else "None"
            ),
            inline=False
        )

        embed.add_field(
            name="Score Confirmation",
            value=(
                f"Target Team: `{confirmation.get('target_team')}`\n"
                f"Confirmations: `{confirmation.get('confirmations', [])}`\n"
                f"Declines: `{confirmation.get('declines', [])}`"
            ),
            inline=False
        )

        embed.add_field(
            name="Cancel Match Votes",
            value=(
                f"Votes: `{cancel_match.get('votes', [])}`\n"
                f"Required: `{required_cancel_votes}`"
            ),
            inline=False
        )

        embed.add_field(
            name="Reports",
            value=f"`{len(reports)}` report(s)",
            inline=False
        )

        return embed

    async def build_history_embed(self, guild: discord.Guild, target_user_id: int) -> discord.Embed:
        player = await self.get_player(target_user_id)
        history_ids = player.get("history", [])[-10:][::-1]

        target_obj = await self.resolve_member_or_user(guild, target_user_id)
        target_name = target_obj.display_name if isinstance(target_obj, discord.Member) else (
            target_obj.name if target_obj else str(target_user_id)
        )

        embed = discord.Embed(
            title=f"📜 Match History • {target_name}",
            color=discord.Color.blurple()
        )

        if not history_ids:
            embed.description = "No match history found."
            return embed

        lines = []

        for match_id in history_ids:
            match = await matches_col.find_one({"_id": match_id})
            if not match:
                continue

            mode = match.get("mode", "unknown")
            score = match.get("score", "N/A")
            status = match.get("status", "unknown")
            winners = match.get("winner", [])
            result = "Win" if target_user_id in winners else "Loss"

            lines.append(
                f"**{result}** • `{mode}` • Match `{match_id[:8]}` • Score `{score}` • `{status}`"
            )

        embed.description = "\n".join(lines) if lines else "No readable match history found."
        return embed

    async def build_recent_embed(self, guild: discord.Guild, target_user_id: int, limit: int = 5) -> discord.Embed:
        player = await self.get_player(target_user_id)
        history_ids = player.get("history", [])[-limit:][::-1]

        target_obj = await self.resolve_member_or_user(guild, target_user_id)
        target_name = target_obj.display_name if isinstance(target_obj, discord.Member) else (
            target_obj.name if target_obj else str(target_user_id)
        )

        embed = discord.Embed(
            title=f"🕘 Recent Matches • {target_name}",
            color=discord.Color.blurple()
        )

        if not history_ids:
            embed.description = "No recent matches found."
            return embed

        lines = []

        for match_id in history_ids:
            match = await matches_col.find_one({"_id": match_id})
            if not match:
                continue

            mode = match.get("mode", "unknown")
            score = match.get("score", "N/A")
            status = match.get("status", "unknown")
            winners = match.get("winner", [])
            result = "✅ Win" if target_user_id in winners else "❌ Loss"

            thread_text = ""
            thread_id = match.get("thread_id")
            if thread_id:
                thread = await self.resolve_thread(guild, thread_id)
                if thread:
                    thread_text = f" • [Thread]({thread.jump_url})"

            lines.append(
                f"{result} • `{mode}` • `{score}` • `{status}` • `{match_id[:8]}`{thread_text}"
            )

        embed.description = "\n".join(lines) if lines else "No readable recent matches found."
        return embed

    async def build_lastmatch_embed(self, guild: discord.Guild, target_user_id: int) -> discord.Embed:
        player = await self.get_player(target_user_id)
        history_ids = player.get("history", [])

        target_obj = await self.resolve_member_or_user(guild, target_user_id)
        target_name = target_obj.display_name if isinstance(target_obj, discord.Member) else (
            target_obj.name if target_obj else str(target_user_id)
        )

        embed = discord.Embed(
            title=f"🎯 Last Match • {target_name}",
            color=discord.Color.blurple()
        )

        if not history_ids:
            embed.description = "No match history found."
            return embed

        match_id = history_ids[-1]
        match = await matches_col.find_one({"_id": match_id})

        if not match:
            embed.description = "Last match could not be found."
            return embed

        mode = match.get("mode", "unknown")
        score = match.get("score", "N/A")
        status = match.get("status", "unknown")
        winners = match.get("winner", [])
        result = "Win" if target_user_id in winners else "Loss"

        team1 = match.get("teams", {}).get("team1", [])
        team2 = match.get("teams", {}).get("team2", [])

        embed.add_field(name="Match ID", value=f"`{match_id}`", inline=False)
        embed.add_field(name="Mode", value=mode, inline=True)
        embed.add_field(name="Result", value=result, inline=True)
        embed.add_field(name="Status", value=status.title(), inline=True)
        embed.add_field(name="Score", value=f"`{score}`", inline=False)
        embed.add_field(name="Team A", value=", ".join(f"<@{p}>" for p in team1) or "Unknown", inline=False)
        embed.add_field(name="Team B", value=", ".join(f"<@{p}>" for p in team2) or "Unknown", inline=False)

        thread_id = match.get("thread_id")
        if thread_id:
            thread = await self.resolve_thread(guild, thread_id)
            if thread:
                embed.add_field(name="Thread", value=f"[Open Thread]({thread.jump_url})", inline=False)

        return embed

    async def build_headtohead_embed(self, guild: discord.Guild, user1_id: int, user2_id: int) -> discord.Embed:
        u1 = await self.resolve_member_or_user(guild, user1_id)
        u2 = await self.resolve_member_or_user(guild, user2_id)

        name1 = u1.display_name if isinstance(u1, discord.Member) else (u1.name if u1 else str(user1_id))
        name2 = u2.display_name if isinstance(u2, discord.Member) else (u2.name if u2 else str(user2_id))

        total = 0
        u1_wins = 0
        u2_wins = 0
        recent_lines = []

        mode_stats = {
            "1v1": {"total": 0, "u1_wins": 0, "u2_wins": 0},
            "2v2": {"total": 0, "u1_wins": 0, "u2_wins": 0},
            "3v3": {"total": 0, "u1_wins": 0, "u2_wins": 0},
            "4v4": {"total": 0, "u1_wins": 0, "u2_wins": 0},
        }

        latest_by_mode = {}

        async for match in matches_col.find({
            "guild_id": guild.id,
            "players": {"$all": [user1_id, user2_id]},
            "status": {"$in": ["finished", "cancelled"]}
        }).sort("created_at", -1):
            total += 1

            mode = match.get("mode", "unknown")
            winners = match.get("winner", [])
            score = match.get("score", "N/A")
            status = match.get("status", "unknown")

            if user1_id in winners:
                u1_wins += 1
                result = f"{name1} won"
            elif user2_id in winners:
                u2_wins += 1
                result = f"{name2} won"
            else:
                result = "No winner"

            if mode in mode_stats:
                mode_stats[mode]["total"] += 1
                if user1_id in winners:
                    mode_stats[mode]["u1_wins"] += 1
                elif user2_id in winners:
                    mode_stats[mode]["u2_wins"] += 1

                if mode not in latest_by_mode:
                    latest_by_mode[mode] = {
                        "score": score,
                        "status": status,
                        "result": result,
                        "match_id": match["_id"][:8]
                    }

            if len(recent_lines) < 5:
                recent_lines.append(f"`{mode}` • `{score}` • `{status}` • {result}")

        embed = discord.Embed(
            title=f"🤝 Head to Head • {name1} vs {name2}",
            color=discord.Color.blurple()
        )

        embed.add_field(name=name1, value=f"`{u1_wins}` wins", inline=True)
        embed.add_field(name=name2, value=f"`{u2_wins}` wins", inline=True)
        embed.add_field(name="Total Matches", value=f"`{total}`", inline=True)

        mode_lines = []
        for mode in ("1v1", "2v2", "3v3", "4v4"):
            data = mode_stats[mode]
            if data["total"] > 0:
                mode_lines.append(
                    f"**{mode}** • `{name1}: {data['u1_wins']}` • `{name2}: {data['u2_wins']}` • `{data['total']} total`"
                )

        embed.add_field(
            name="Mode Breakdown",
            value="\n".join(mode_lines) if mode_lines else "No shared matches in ranked modes.",
            inline=False
        )

        latest_mode_lines = []
        for mode in ("1v1", "2v2", "3v3", "4v4"):
            latest = latest_by_mode.get(mode)
            if latest:
                latest_mode_lines.append(
                    f"**{mode}** • `{latest['score']}` • `{latest['status']}` • {latest['result']} • `{latest['match_id']}`"
                )

        embed.add_field(
            name="Latest Meeting Per Mode",
            value="\n".join(latest_mode_lines) if latest_mode_lines else "No latest mode meetings found.",
            inline=False
        )

        if recent_lines:
            embed.add_field(name="Recent Meetings", value="\n".join(recent_lines), inline=False)
        else:
            embed.add_field(name="Recent Meetings", value="No shared matches found.", inline=False)

        return embed

    async def build_placement_info_embed(self, guild_id: int) -> discord.Embed:
        placement_games = await self.get_placement_games(guild_id)

        embed = discord.Embed(
            title="🧭 Placement Info",
            color=discord.Color.blurple()
        )

        embed.add_field(
            name="How Placements Work",
            value=(
                f"You must complete **{placement_games} placement matches**.\n"
                "During placements, wins and losses are tracked separately from your visible MMR progression."
            ),
            inline=False
        )

        embed.add_field(
            name="After Placements",
            value=(
                "When placements are complete, your starting MMR is seeded based on your placement wins.\n"
                "Better placement record = better starting MMR."
            ),
            inline=False
        )

        embed.add_field(
            name="Tracked Stats",
            value=(
                "• Matches played\n"
                "• Wins / losses\n"
                "• Placement completion status"
            ),
            inline=False
        )

        return embed

    async def build_report_history_embed(self, guild: discord.Guild, target_user_id: int) -> discord.Embed:
        target_obj = await self.resolve_member_or_user(guild, target_user_id)
        target_name = target_obj.display_name if isinstance(target_obj, discord.Member) else (
            target_obj.name if target_obj else str(target_user_id)
        )

        embed = discord.Embed(
            title=f"🚨 Report History • {target_name}",
            color=discord.Color.red()
        )

        reported_matches = []

        async for match in matches_col.find({
            "guild_id": guild.id,
            "players": target_user_id
        }).sort("created_at", -1):
            reports = match.get("reports", [])
            if reports:
                reported_matches.append(match)

        if not reported_matches:
            embed.description = "No reports found for this player."
            return embed

        lines = []
        total_reports = 0

        for match in reported_matches[:10]:
            reports = match.get("reports", [])
            total_reports += len(reports)
            mode = match.get("mode", "unknown")
            status = match.get("status", "unknown")
            lines.append(
                f"`{match['_id'][:8]}` • `{mode}` • `{status}` • `{len(reports)}` report(s)"
            )

        embed.add_field(name="Matches With Reports", value=f"`{len(reported_matches)}`", inline=True)
        embed.add_field(name="Total Reports", value=f"`{total_reports}`", inline=True)
        embed.add_field(name="Recent Reported Matches", value="\n".join(lines), inline=False)

        return embed

    async def build_rank_list_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title="🏅 Ranked Tiers",
            color=discord.Color.gold()
        )

        lines = []
        for rank_name, min_elo, max_elo in RANKS:
            emoji = get_rank_emoji(rank_name)

            if max_elo is None:
                lines.append(f"{emoji} **{rank_name}** • `{min_elo}+`")
            else:
                lines.append(f"{emoji} **{rank_name}** • `{min_elo}-{max_elo}`")

        embed.description = "\n".join(lines)
        embed.set_footer(text="Ranks are based on MMR (ELO)")
        return embed

    async def build_stats_embed(self, guild: discord.Guild, target_user_id: int) -> discord.Embed:
        player = await self.get_player(target_user_id)
        stats = player.get("stats", {})
        global_stats = stats.get("global", {"wins": 0, "losses": 0, "matches": 0})
        placements = player.get("placements", {"completed": False, "matches_played": 0, "wins": 0, "losses": 0})

        target_obj = await self.resolve_member_or_user(guild, target_user_id)
        target_name = target_obj.display_name if isinstance(target_obj, discord.Member) else (
            target_obj.name if target_obj else str(target_user_id)
        )

        rank_info = get_rank_info(player.get("elo", 0))
        global_streak = self.get_player_streak_data(player, "global")

        embed = discord.Embed(
            title=f"📊 Ranked Stats • {target_name}",
            color=discord.Color.blurple()
        )

        embed.add_field(name="Rank", value=rank_info["name"], inline=True)
        embed.add_field(name="ELO", value=str(player.get("elo", 0)), inline=True)
        embed.add_field(name="Level", value=str(player.get("level", 1)), inline=True)

        embed.add_field(
            name="Global",
            value=(
                f"Wins: `{global_stats.get('wins', 0)}`\n"
                f"Losses: `{global_stats.get('losses', 0)}`\n"
                f"Matches: `{global_stats.get('matches', 0)}`\n"
                f"Winrate: `{self.calculate_winrate(global_stats.get('wins', 0), global_stats.get('matches', 0))}`"
            ),
            inline=False
        )

        per_mode_lines = []
        for mode in ("1v1", "2v2", "3v3", "4v4"):
            mode_stats = stats.get(mode, {"wins": 0, "losses": 0, "matches": 0})
            per_mode_lines.append(
                f"**{mode}** • `{mode_stats.get('wins', 0)}W` / `{mode_stats.get('losses', 0)}L` / "
                f"`{mode_stats.get('matches', 0)}M` / `{self.calculate_winrate(mode_stats.get('wins', 0), mode_stats.get('matches', 0))}`"
            )

        embed.add_field(name="Modes", value="\n".join(per_mode_lines), inline=False)

        placement_games = await self.get_placement_games(guild.id)

        placement_text = (
            "Complete"
            if placements.get("completed", False)
            else f"{placements.get('matches_played', 0)}/{placement_games} • "
                f"{placements.get('wins', 0)}W-{placements.get('losses', 0)}L"
        )
        embed.add_field(name="Placements", value=placement_text, inline=True)
        embed.add_field(name="Current Streak", value=str(global_streak.get("current", 0)), inline=True)
        embed.add_field(name="Best Streak", value=str(global_streak.get("best", 0)), inline=True)

        mode_streak_lines = []
        for streak_mode in ("1v1", "2v2", "3v3", "4v4"):
            streak_data = self.get_player_streak_data(player, streak_mode)
            mode_streak_lines.append(
                f"**{streak_mode}** • `{streak_data.get('current', 0)} live` / `{streak_data.get('best', 0)} best`"
            )

        embed.add_field(name="Mode Streaks", value="\n".join(mode_streak_lines), inline=False)

        return embed
    
    async def build_rank_embed(self, guild: discord.Guild, target_user_id: int) -> discord.Embed:
        player = await self.get_player(target_user_id)
        rank_info = get_rank_info(player.get("elo", 0))

        target_obj = await self.resolve_member_or_user(guild, target_user_id)
        target_name = target_obj.display_name if isinstance(target_obj, discord.Member) else (
            target_obj.name if target_obj else str(target_user_id)
        )

        embed = discord.Embed(
            title=f"🏅 Rank • {target_name}",
            color=discord.Color.gold()
        )

        embed.add_field(name="Current Rank", value=rank_info["name"], inline=True)
        embed.add_field(name="ELO", value=str(player.get("elo", 0)), inline=True)
        embed.add_field(name="Progress", value=format_rank_progress(player.get("elo", 0)), inline=False)

        return embed


    # ================= MATCHMAKING =================

    async def join_queue(self, interaction, mode):
        user_id = interaction.user.id
        guild_id = interaction.guild.id
        queue_state = self.get_guild_queue_state(guild_id)

        active_ban = await self.get_active_ranked_ban(guild_id, user_id)
        if active_ban:
            expires_text = self.format_ban_expiry(active_ban.get("expires_at"))
            return await interaction.response.send_message(
                f"You are banned from ranked.\nReason: **{active_ban.get('reason', 'No reason')}**\nExpires: **{expires_text}**",
                ephemeral=True
            )

        party = await self.get_party_for_user(guild_id, user_id)
        party_size = len(party["members"]) if party else 1

        members_to_check = party["members"] if party else [user_id]

        for member_id in members_to_check:
            member_ban = await self.get_active_ranked_ban(guild_id, member_id)
            if member_ban:
                expires_text = self.format_ban_expiry(member_ban.get("expires_at"))
                return await interaction.response.send_message(
                    f"<@{member_id}> is banned from ranked.\n"
                    f"Reason: **{member_ban.get('reason', 'No reason')}**\n"
                    f"Expires: **{expires_text}**",
                    ephemeral=True
                )

        if not self.can_party_join_mode(mode, party_size):
            return await interaction.response.send_message(
                f"Your party has **{party_size}** players and cannot join **{mode}**.\n"
                f"Maximum party size for {mode} is **{int(mode[0])}**.",
                ephemeral=True
            )

        already_queued_modes = []
        for member_id in members_to_check:
            queued_mode = self.is_user_in_any_queue(guild_id, member_id)
            if queued_mode:
                already_queued_modes.append((member_id, queued_mode))

        # If this is a party join, clear stale/old queue entries for the whole party first,
        # then re-add the correct grouped entry.
        if party and already_queued_modes:
            changed = self.remove_members_from_all_queue_entries(guild_id, members_to_check)
            if changed:
                await self.save_queue_state(guild_id)
        elif already_queued_modes:
            member_id, queued_mode = already_queued_modes[0]
            return await interaction.response.send_message(
                f"<@{member_id}> is already in the **{queued_mode}** queue.",
                ephemeral=True
            )

        entry = self.build_queue_entry(party, user_id)
        queue_state[mode].append(entry)
        await self.save_queue_state(guild_id)

        if party:
            await self.touch_party(party)
            await interaction.response.send_message(
                f"Your party joined the **{mode}** queue as a group of **{party_size}**.",
                ephemeral=True
            )
        else:
            await interaction.response.send_message(f"Joined {mode} queue.", ephemeral=True)

        await self.refresh_queue_message(interaction.guild, mode)

        selected_entries = self.extract_full_match_entries(guild_id, mode)
        if selected_entries:
            previous_entries = list(queue_state[mode])
            selected_party_ids = {entry.get("party_id") for entry in selected_entries if entry.get("party_id")}
            queue_state[mode] = [
                entry for entry in queue_state[mode]
                if entry not in selected_entries
            ]
            await self.save_queue_state(guild_id)
            await self.refresh_queue_message(interaction.guild, mode)
            created, error_message = await self.create_match(interaction.guild, selected_entries, mode)

            if not created:
                queue_state[mode] = previous_entries
                await self.save_queue_state(guild_id)
                await self.refresh_queue_message(interaction.guild, mode)
                await interaction.followup.send(
                    f"Match creation failed: {error_message}. The queue was restored.",
                    ephemeral=True
                )
                return

            for party_id in selected_party_ids:
                for party_obj in self.parties.values():
                    if party_obj["_id"] == party_id:
                        await self.touch_party(party_obj)
                        break

    async def leave_queue(self, interaction, mode):
        user_id = interaction.user.id

        queue_entries = self.get_guild_queue_state(interaction.guild.id).get(mode, [])
        target_entry = None

        for entry in queue_entries:
            if user_id in entry.get("members", []):
                target_entry = entry
                break

        if target_entry is None:
            return await interaction.response.send_message("You are not in queue.", ephemeral=True)

        queue_entries.remove(target_entry)
        await self.save_queue_state(interaction.guild.id)

        if target_entry.get("party_id"):
            await interaction.response.send_message(
                "Your party was removed from the queue.",
                ephemeral=True
            )
        else:
            await interaction.response.send_message(f"Left {mode} queue.", ephemeral=True)

        await self.refresh_queue_message(interaction.guild, mode)

    async def create_match(self, guild, selected_entries, mode):
        config = await self.get_config(guild.id)

        thread_channel_id = config.get("match_channels", {}).get(mode)
        log_channel_id = config.get("match_log_channels", {}).get(mode)

        if not thread_channel_id:
            return False, f"no match channel is configured for {mode}"

        thread_channel = guild.get_channel(thread_channel_id)
        if not thread_channel:
            return False, "the configured match channel could not be found"

        log_channel = guild.get_channel(log_channel_id) if log_channel_id else None

        match_id = str(uuid.uuid4())
        team1, team2 = self.split_entries_into_balanced_teams(selected_entries, mode)
        if team1 is None or team2 is None:
            return False, "the queued entries could not be split into valid teams"

        players = team1 + team2

        match_message = None
        if log_channel:
            match_message = await log_channel.send(
                embed=self.build_match_found_embed(mode, match_id, team1, team2)
            )

        thread = await thread_channel.create_thread(
            name=f"🟢 {mode} match • {match_id[:8]}",
            type=discord.ChannelType.private_thread,
            invitable=False
        )

        failed_to_add = []
        for user_id in players:
            try:
                await thread.add_user(await guild.fetch_member(user_id))
            except Exception as e:
                failed_to_add.append((user_id, str(e)))

        try:
            drifters_bot = await self.bot.fetch_user(DRIFTERS_BOT_ID)
            await thread.add_user(drifters_bot)
        except Exception:
            pass

        if match_message:
            try:
                await match_message.edit(
                    embed=self.build_match_found_embed(
                        mode=mode,
                        match_id=match_id,
                        team1=team1,
                        team2=team2,
                        status_emoji="🟢",
                        status_label="Active",
                        thread_text=f"[Open Thread]({thread.jump_url})",
                        failed_to_add=failed_to_add if failed_to_add else None,
                    )
                )
            except discord.HTTPException:
                pass

        await matches_col.insert_one({
            "_id": match_id,
            "guild_id": guild.id,
            "mode": mode,
            "players": players,
            "teams": {
                "team1": team1,
                "team2": team2
            },
            "status": "ongoing",

            "created_at": discord.utils.utcnow(),
            "finished_at": None,

            "winner": None,
            "loser": None,

            "score": None,
            "score_submission": None,
            "score_confirmation": {
                "target_team": None,
                "confirmations": [],
                "declines": []
            },
            "cancel_match": {
                "votes": []
            },
            "reports": [],
            "party_entries": selected_entries,

            "thread_id": thread.id,
            "match_message_id": match_message.id if match_message else None,
            "result_message_id": None,
            "confirmation_message_id": None
        })

        result_embed = discord.Embed(
            title=f"⚔️ Match Type • {mode}",
            description="One player submits the score first. Then the opposing team confirms or declines it."
        )
        result_embed.add_field(name="Match ID", value=f"`{match_id}`", inline=False)
        result_embed.add_field(name="Team A", value=", ".join(f"<@{p}>" for p in team1), inline=False)
        result_embed.add_field(name="Team B", value=", ".join(f"<@{p}>" for p in team2), inline=False)

        await thread.send(content=f"<@{DRIFTERS_BOT_ID}>", embed=self.build_match_rules_embed())

        result_message = await thread.send(
            embed=result_embed,
            view=ResultView(self, players, mode, match_id)
        )

        await matches_col.update_one(
            {"_id": match_id},
            {
                "$set": {
                    "result_message_id": result_message.id
                }
            }
        )
        await self.update_match_visuals(guild, match_id)
        return True, None

    async def finalize_confirmed_score(self, guild, match_id, channel):
        match = await matches_col.find_one({"_id": match_id})
        if not match:
            await channel.send("Match not found in database.")
            return

        if match.get("status") != "ongoing":
            return

        mode = match["mode"]
        team1 = match["teams"]["team1"]
        team2 = match["teams"]["team2"]

        submission = match.get("score_submission")
        if not submission:
            await channel.send("No confirmed score found.")
            return

        team1_score = submission["team1_score"]
        team2_score = submission["team2_score"]
        score_diff = abs(team1_score - team2_score)

        if team1_score == team2_score:
            await channel.send("Scores cannot be tied.")
            return

        if team1_score > team2_score:
            winners = team1
            losers = team2
        else:
            winners = team2
            losers = team1

        winner_players = []
        loser_players = []

        for user_id in winners:
            player = await self.get_player(user_id)
            winner_players.append(player)

        for user_id in losers:
            player = await self.get_player(user_id)
            loser_players.append(player)

        winner_enemy_avg_elo = 0
        loser_enemy_avg_elo = 0

        if loser_players:
            winner_enemy_avg_elo = round(sum(player["elo"] for player in loser_players) / len(loser_players))

        if winner_players:
            loser_enemy_avg_elo = round(sum(player["elo"] for player in winner_players) / len(winner_players))

        winner_changes = []
        loser_changes = []
        placement_games = await self.get_placement_games(guild.id)
        current_season = await self.get_current_season_number(guild.id)
        k_factor = await self.get_mode_k_factor(guild.id, mode)

        for user_id, wp in zip(winners, winner_players):
            old_elo = wp["elo"]

            wp["stats"]["global"]["wins"] += 1
            wp["stats"]["global"]["matches"] += 1
            wp["stats"][mode]["wins"] += 1
            wp["stats"][mode]["matches"] += 1

            winner_streaks = self.normalize_player_streaks(wp)
            winner_streaks["global"]["current"] += 1
            winner_streaks["global"]["best"] = max(
                winner_streaks["global"]["best"],
                winner_streaks["global"]["current"]
            )

            winner_streaks[mode]["current"] += 1
            winner_streaks[mode]["best"] = max(
                winner_streaks[mode]["best"],
                winner_streaks[mode]["current"]
            )

            wp["xp"] += 50
            wp["history"].append(match_id)
            wp["history"] = wp["history"][-10:]

            placements = wp.setdefault("placements", {
                "completed": False,
                "matches_played": 0,
                "wins": 0,
                "losses": 0
            })

            was_in_placements = not placements.get("completed", False)

            if was_in_placements:
                placements["matches_played"] += 1
                placements["wins"] += 1

                if placements["matches_played"] >= placement_games:
                    placements["completed"] = True
                    wp["elo"] = calculate_placement_seed(wp, placement_games=placement_games)
            else:
                elo_change = calculate_individual_elo_change(
                    player_elo=old_elo,
                    enemy_avg_elo=winner_enemy_avg_elo,
                    won=True,
                    score_diff=score_diff,
                    k=k_factor
                )
                wp["elo"] += elo_change

            while wp["xp"] >= 100:
                wp["xp"] -= 100
                wp["level"] += 1

            new_elo = wp["elo"]

            winner_changes.append({
                "user_id": user_id,
                "diff": new_elo - old_elo,
                "elo": new_elo,
                "placement_status": wp["placements"]["matches_played"],
                "placement_completed": wp["placements"]["completed"],
            })

            self.append_mmr_history_point(wp, wp["elo"], f"Match win {match_id}")

            season_profile = await self.get_season_profile(guild.id, user_id, current_season)
            season_profile["season_elo"] += max(0, new_elo - old_elo)
            season_profile["peak_elo"] = max(season_profile.get("peak_elo", 0), season_profile["season_elo"])
            season_profile["xp"] += 50

            season_profile["stats"]["global"]["wins"] += 1
            season_profile["stats"]["global"]["matches"] += 1
            season_profile["stats"][mode]["wins"] += 1
            season_profile["stats"][mode]["matches"] += 1
            season_profile["history"].append(match_id)
            season_profile["history"] = season_profile["history"][-20:]

            season_history = season_profile.setdefault("mmr_history", [])
            season_history.append({
                "mmr": season_profile["season_elo"],
                "games_played": season_profile["stats"]["global"]["matches"],
                "reason": f"Match win {match_id}",
                "at": discord.utils.utcnow()
            })
            season_profile["mmr_history"] = season_history[-100:]

            self.add_season_quest_metric(season_profile, "daily", "matches_played", 1)
            self.add_season_quest_metric(season_profile, "weekly", "matches_played", 1)
            self.add_season_quest_metric(season_profile, "monthly", "matches_played", 1)
            self.add_season_quest_metric(season_profile, "seasonal", "matches_played", 1)

            self.add_season_quest_metric(season_profile, "daily", "wins", 1)
            self.add_season_quest_metric(season_profile, "weekly", "wins", 1)
            self.add_season_quest_metric(season_profile, "monthly", "wins", 1)
            self.add_season_quest_metric(season_profile, "seasonal", "wins", 1)

            self.update_season_quest_metric(season_profile, "seasonal", "peak_elo", season_profile["peak_elo"])

            while season_profile["xp"] >= 100:
                season_profile["xp"] -= 100
                season_profile["level"] += 1

            await self.save_season_profile(season_profile)

            if season_profile.get("team_id"):
                await self.apply_team_match_result(
                    guild,
                    season_profile.get("team_id"),
                    user_id,
                    mode,
                    True,
                    new_elo - old_elo
                )

            await players_col.update_one({"_id": user_id}, {"$set": wp})

        for user_id, lp in zip(losers, loser_players):
            old_elo = lp["elo"]

            lp["stats"]["global"]["losses"] += 1
            lp["stats"]["global"]["matches"] += 1
            lp["stats"][mode]["losses"] += 1
            lp["stats"][mode]["matches"] += 1

            loser_streaks = self.normalize_player_streaks(lp)
            loser_streaks["global"]["current"] = 0
            loser_streaks[mode]["current"] = 0

            lp["history"].append(match_id)
            lp["history"] = lp["history"][-10:]

            placements = lp.setdefault("placements", {
                "completed": False,
                "matches_played": 0,
                "wins": 0,
                "losses": 0
            })

            was_in_placements = not placements.get("completed", False)

            if was_in_placements:
                placements["matches_played"] += 1
                placements["losses"] += 1

                if placements["matches_played"] >= placement_games:
                    placements["completed"] = True
                    lp["elo"] = calculate_placement_seed(lp, placement_games=placement_games)
            else:
                elo_change = calculate_individual_elo_change(
                    player_elo=old_elo,
                    enemy_avg_elo=loser_enemy_avg_elo,
                    won=False,
                    score_diff=score_diff,
                    k=k_factor
                )
                lp["elo"] += elo_change

            new_elo = lp["elo"]

            loser_changes.append({
                "user_id": user_id,
                "diff": new_elo - old_elo,
                "elo": new_elo,
                "placement_status": lp["placements"]["matches_played"],
                "placement_completed": lp["placements"]["completed"],
            })

            self.append_mmr_history_point(lp, lp["elo"], f"Match loss {match_id}")

            season_profile = await self.get_season_profile(guild.id, user_id, current_season)
            season_profile["season_elo"] = max(0, season_profile.get("season_elo", 0) + min(0, new_elo - old_elo))
            season_profile["peak_elo"] = max(season_profile.get("peak_elo", 0), season_profile["season_elo"])

            season_profile["stats"]["global"]["losses"] += 1
            season_profile["stats"]["global"]["matches"] += 1
            season_profile["stats"][mode]["losses"] += 1
            season_profile["stats"][mode]["matches"] += 1
            season_profile["history"].append(match_id)
            season_profile["history"] = season_profile["history"][-20:]

            season_history = season_profile.setdefault("mmr_history", [])
            season_history.append({
                "mmr": season_profile["season_elo"],
                "games_played": season_profile["stats"]["global"]["matches"],
                "reason": f"Match loss {match_id}",
                "at": discord.utils.utcnow()
            })
            season_profile["mmr_history"] = season_history[-100:]

            self.add_season_quest_metric(season_profile, "daily", "matches_played", 1)
            self.add_season_quest_metric(season_profile, "weekly", "matches_played", 1)
            self.add_season_quest_metric(season_profile, "monthly", "matches_played", 1)
            self.add_season_quest_metric(season_profile, "seasonal", "matches_played", 1)

            self.update_season_quest_metric(season_profile, "seasonal", "peak_elo", season_profile["peak_elo"])

            await self.save_season_profile(season_profile)

            if season_profile.get("team_id"):
                await self.apply_team_match_result(
                    guild,
                    season_profile.get("team_id"),
                    user_id,
                    mode,
                    False,
                    new_elo - old_elo
                )

            await players_col.update_one({"_id": user_id}, {"$set": lp})

        await matches_col.update_one(
            {"_id": match_id},
            {
                "$set": {
                    "status": "finished",
                    "finished_at": discord.utils.utcnow(),
                    "winner": winners,
                    "loser": losers,
                    "score": f"{team1_score}-{team2_score}",
                    "confirmation_message_id": None
                }
            }
        )

        team_a_changes = winner_changes if winners == team1 else loser_changes
        team_b_changes = winner_changes if winners == team2 else loser_changes

        team_a_lines = []
        for change in team_a_changes:
            diff_text = f"+{change['diff']}" if change['diff'] >= 0 else str(change['diff'])
            team_a_lines.append(
                f"<@{change['user_id']}> • `{diff_text} MMR` • {format_rank_progress(change['elo'])}"
            )

        team_b_lines = []
        for change in team_b_changes:
            diff_text = f"+{change['diff']}" if change['diff'] >= 0 else str(change['diff'])
            team_b_lines.append(
                f"<@{change['user_id']}> • `{diff_text} MMR` • {format_rank_progress(change['elo'])}"
            )

        result_embed = discord.Embed(
            title=f"✅ Match Results • {match_id}",
            description="**Match scores confirmed**",
            color=discord.Color.green()
        )

        result_embed.add_field(
            name="Score",
            value=f"**Team A:** `{team1_score}` • **Team B:** `{team2_score}`",
            inline=False
        )

        result_embed.add_field(
            name="📈 Team A MMR Changes" if winners == team1 else "📉 Team A MMR Changes",
            value="\n".join(team_a_lines) if team_a_lines else "No changes.",
            inline=False
        )

        result_embed.add_field(
            name="📈 Team B MMR Changes" if winners == team2 else "📉 Team B MMR Changes",
            value="\n".join(team_b_lines) if team_b_lines else "No changes.",
            inline=False
        )

        result_embed.add_field(
            name="Team A",
            value=", ".join(f"<@{p}>" for p in team1),
            inline=False
        )

        result_embed.add_field(
            name="Team B",
            value=", ".join(f"<@{p}>" for p in team2),
            inline=False
        )

        result_embed.add_field(
            name="🏆 Winners",
            value="Team A" if winners == team1 else "Team B",
            inline=False
        )

        team_a_placement_lines = []
        for change in team_a_changes:
            if change["placement_completed"]:
                status_text = "Placement complete"
            else:
                status_text = f"Placements: {change['placement_status']}/{placement_games}"

            team_a_placement_lines.append(f"<@{change['user_id']}> • {status_text}")

        team_b_placement_lines = []
        for change in team_b_changes:
            if change["placement_completed"]:
                status_text = "Placement complete"
            else:
                status_text = f"Placements: {change['placement_status']}/{placement_games}"

            team_b_placement_lines.append(f"<@{change['user_id']}> • {status_text}")

        result_embed.add_field(
            name="🧭 Team A Placement Status",
            value="\n".join(team_a_placement_lines) if team_a_placement_lines else "No players.",
            inline=False
        )

        result_embed.add_field(
            name="🧭 Team B Placement Status",
            value="\n".join(team_b_placement_lines) if team_b_placement_lines else "No players.",
            inline=False
        )

        for user_id in winners + losers:
            await self.sync_rank_roles_for_user_id(guild, user_id)

        await channel.send(embed=result_embed)
        await self.update_match_visuals(guild, match_id)
        await self.update_leaderboard(guild)

    # ================= USERS =================

    @commands.command()
    async def stats(self, ctx, user: discord.Member = None):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        target = user or ctx.author
        embed = await self.build_stats_embed(ctx.guild, target.id)
        await ctx.send(embed=embed)

    @commands.command()
    async def rank(self, ctx, user: discord.Member = None):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        target = user or ctx.author
        embed = await self.build_rank_embed(ctx.guild, target.id)
        await ctx.send(embed=embed)

    @commands.command()
    async def history(self, ctx, user: discord.Member = None):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        target = user or ctx.author
        embed = await self.build_history_embed(ctx.guild, target.id)
        await ctx.send(embed=embed)

    @commands.command()
    async def recent(self, ctx, user: discord.Member = None):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        target = user or ctx.author
        embed = await self.build_recent_embed(ctx.guild, target.id)
        await ctx.send(embed=embed)

    @commands.command()
    async def lastmatch(self, ctx, user: discord.Member = None):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        target = user or ctx.author
        embed = await self.build_lastmatch_embed(ctx.guild, target.id)
        await ctx.send(embed=embed)

    @commands.command()
    async def headtohead(self, ctx, user: discord.Member):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        if user.id == ctx.author.id:
            return await ctx.send("You cannot use headtohead on yourself.")

        embed = await self.build_headtohead_embed(ctx.guild, ctx.author.id, user.id)
        await ctx.send(embed=embed)

    @commands.command()
    async def placementinfo(self, ctx):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        embed = await self.build_placement_info_embed(ctx.guild.id)
        await ctx.send(embed=embed)

    @commands.command()
    async def reporthistory(self, ctx, user: discord.Member = None):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        target = user or ctx.author
        embed = await self.build_report_history_embed(ctx.guild, target.id)
        await ctx.send(embed=embed)

    @commands.command()
    async def reportplayer(self, ctx, user: discord.Member, *, reason: str):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        if user.id == ctx.author.id:
            return await ctx.send("You cannot report yourself.")

        report_id = await self.submit_player_report(ctx.guild, ctx.author.id, user.id, reason)
        await ctx.send(f"Player report submitted. Report ID: `{report_id}`")

    @commands.command()
    async def recentreports(self, ctx):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        embed = await self.build_recent_reports_embed(ctx.guild, ctx.author.id)
        await ctx.send(embed=embed)

    @commands.command()
    async def mybans(self, ctx):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        total_pages = await self.get_my_bans_total_pages(ctx.guild.id, ctx.author.id, per_page=5)
        embed = await self.build_my_bans_embed(ctx.guild, ctx.author.id, page=1, per_page=5)
        view = SimplePaginationView(self, self.build_my_bans_embed, 1, total_pages, ctx.guild, ctx.author.id, 5)
        await ctx.send(embed=embed, view=view)

    @commands.command()
    async def pingranked(self, ctx, *, message: str = None):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        active_ban = await self.get_active_ranked_ban(ctx.guild.id, ctx.author.id)
        if active_ban:
            expires_text = self.format_ban_expiry(active_ban.get("expires_at"))
            return await ctx.send(
                f"You are banned from ranked.\nReason: **{active_ban.get('reason', 'No reason')}**\nExpires: **{expires_text}**"
            )

        config = await self.get_config(ctx.guild.id)
        ping_role_id = config.get("ping_role_id")
        if not ping_role_id:
            return await ctx.send("No ranked ping role has been configured.")

        role = ctx.guild.get_role(ping_role_id)
        if not role:
            return await ctx.send("The configured ranked ping role no longer exists.")

        content = f"{role.mention}"
        if message:
            content += f" {message}"

        await ctx.send(content)

        await self.send_ranked_log(
            ctx.guild,
            "📣 Ranked Ping Used",
            f"{ctx.author.mention} used the ranked ping command.",
            color=discord.Color.gold(),
            fields=[
                ("Role", role.mention, True),
                ("Channel", ctx.channel.mention, True),
                ("Message", message or "No extra message", False),
            ]
        )

    @commands.command()
    async def ranklist(self, ctx):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        embed = await self.build_rank_list_embed()
        await ctx.send(embed=embed)

    @commands.command(name="rules", aliases=["matchrules"])
    async def rules(self, ctx):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        await ctx.send(embed=self.build_match_rules_embed())

    @commands.command()
    async def profile(self, ctx, user: discord.Member = None):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        target = user or ctx.author
        embed = await self.build_profile_embed(ctx.guild, target.id)
        await ctx.send(embed=embed)

    @commands.command()
    async def mmrgraph(self, ctx, graph_type: str = "games", user: discord.Member = None):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        allowed_types = {"games", "time", "delta", "movingavg"}
        graph_type = (graph_type or "games").lower()

        if graph_type not in allowed_types:
            return await ctx.send(
                "Invalid graph type. Use one of: `games`, `time`, `delta`, `movingavg`."
            )

        target = user or ctx.author
        file, analytics = await self.build_mmr_graph_file(ctx.guild, target.id, graph_type=graph_type)

        if file is None:
            return await ctx.send("No MMR history available yet for that player.")

        target_obj = await self.resolve_member_or_user(ctx.guild, target.id)
        target_name = target_obj.display_name if isinstance(target_obj, discord.Member) else (
            target_obj.name if target_obj else str(target.id)
        )

        embed = discord.Embed(
            title=f"📊 MMR Graph • {target_name}",
            description=f"Graph type: **{graph_type}**",
            color=discord.Color.blurple()
        )

        if analytics:
            embed.add_field(name="Tracked Points", value=str(analytics["points"]), inline=True)
            embed.add_field(name="Start → End", value=f"`{analytics['start_mmr']}` → `{analytics['end_mmr']}`", inline=True)
            embed.add_field(name="Net Change", value=f"`{analytics['net_change']:+}`", inline=True)

            embed.add_field(name="Peak MMR", value=str(analytics["peak_mmr"]), inline=True)
            embed.add_field(name="Lowest MMR", value=str(analytics["lowest_mmr"]), inline=True)
            embed.add_field(name="Avg Change", value=str(analytics["avg_change"]), inline=True)

            embed.add_field(name="Best Gain", value=f"`{analytics['best_gain']:+}`", inline=True)
            embed.add_field(name="Worst Drop", value=f"`{analytics['worst_drop']}`", inline=True)
            embed.add_field(name="Default View", value="MMR vs Games Played", inline=True)

        await ctx.send(embed=embed, file=file)

    @commands.command()
    async def graphcompare(self, ctx, user: discord.Member):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        if user.id == ctx.author.id:
            return await ctx.send("You cannot compare yourself with yourself.")

        file = await self.build_graph_compare_file(ctx.guild, ctx.author.id, user.id)

        embed = await self.build_graph_compare_embed(ctx.guild, ctx.author.id, user.id)

        if file:
            embed.set_image(url="attachment://graph_compare.png")
            await ctx.send(embed=embed, file=file)
        else:
            await ctx.send(embed=embed)

    @commands.command()
    async def rankprogress(self, ctx, user: discord.Member = None):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        target = user or ctx.author
        embed = await self.build_rank_progress_embed(ctx.guild, target.id)
        await ctx.send(embed=embed)

    @commands.command()
    async def placement(self, ctx, user: discord.Member = None):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        target = user or ctx.author
        embed = await self.build_placement_embed(ctx.guild, target.id)
        await ctx.send(embed=embed)

    @commands.command()
    async def compare(self, ctx, user: discord.Member):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        if user.id == ctx.author.id:
            return await ctx.send("You cannot compare yourself with yourself.")

        embed = await self.build_compare_embed(ctx.guild, ctx.author.id, user.id)
        await ctx.send(embed=embed)

    @commands.command()
    async def rankedinfo(self, ctx):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        embed = await self.build_ranked_info_embed(ctx.guild.id)
        await ctx.send(embed=embed)

    @commands.command()
    async def rankedactivity(self, ctx):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        embed = await self.build_activity_embed(ctx.guild)
        await ctx.send(embed=embed)

    @commands.command()
    async def top(self, ctx, *args):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        mode, metric = self.parse_leaderboard_args(*args)
        if mode is None or metric is None:
            return await ctx.send(
                "Invalid top usage.\n"
                "Examples:\n"
                "`top`\n"
                "`top live`\n"
                "`top best`\n"
                "`top 1v1`\n"
                "`top 1v1 live`\n"
                "`top 1v1 best`"
            )

        total_pages = await self.get_leaderboard_total_pages(mode, metric=metric, per_page=20)
        embed = await self.build_mode_leaderboard_embed(mode, metric=metric, page=1, per_page=20)

        view = LeaderboardPaginationView(self, mode, metric, current_page=1, total_pages=total_pages)
        await ctx.send(embed=embed, view=view)

    @commands.command()
    async def leaderboard(self, ctx, *args):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        mode, metric = self.parse_leaderboard_args(*args)
        if mode is None or metric is None:
            return await ctx.send(
                "Invalid leaderboard usage.\n"
                "Examples:\n"
                "`leaderboard`\n"
                "`leaderboard live`\n"
                "`leaderboard best`\n"
                "`leaderboard 1v1`\n"
                "`leaderboard 1v1 live`\n"
                "`leaderboard 1v1 best`"
            )

        total_pages = await self.get_leaderboard_total_pages(mode, metric=metric, per_page=20)
        embed = await self.build_mode_leaderboard_embed(mode, metric=metric, page=1, per_page=20)

        view = LeaderboardPaginationView(self, mode, metric, current_page=1, total_pages=total_pages)
        await ctx.send(embed=embed, view=view)

    @commands.command()
    async def partycreate(self, ctx):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        existing = await self.get_party_for_user(ctx.guild.id, ctx.author.id)
        if existing:
            return await ctx.send("You are already in a party.")

        party = {
            "_id": uuid.uuid4().hex[:12],
            "guild_id": ctx.guild.id,
            "owner_id": ctx.author.id,
            "members": [ctx.author.id],
            "created_at": discord.utils.utcnow(),
            "last_activity_at": discord.utils.utcnow()
        }

        self.parties[self.build_party_cache_key(ctx.guild.id, ctx.author.id)] = party
        await self.save_party_doc(party)

        await self.send_ranked_log(
            ctx.guild,
            "👥 Party Created",
            f"{ctx.author.mention} created a ranked party.",
            color=discord.Color.blurple(),
            fields=[
                ("Party ID", f"`{party['_id']}`", True),
                ("Owner", ctx.author.mention, True),
            ]
        )

        await ctx.send(f"Party created. Party ID: `{party['_id']}`")

    @commands.command()
    async def partyinvite(self, ctx, user: discord.Member):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        if user.bot:
            return await ctx.send("You cannot invite bots.")

        if user.id == ctx.author.id:
            return await ctx.send("You cannot invite yourself.")

        owner_party = await self.get_party_by_owner(ctx.guild.id, ctx.author.id)
        if not owner_party:
            return await ctx.send("You do not own a party.")

        if user.id in owner_party["members"]:
            return await ctx.send("That player is already in your party.")

        if len(owner_party["members"]) >= 4:
            return await ctx.send("Your party is already full.")

        other_party = await self.get_party_for_user(ctx.guild.id, user.id)
        if other_party:
            return await ctx.send("That player is already in a party.")

        active_ban = await self.get_active_ranked_ban(ctx.guild.id, user.id)
        if active_ban:
            expires_text = self.format_ban_expiry(active_ban.get("expires_at"))
            return await ctx.send(
                f"That player is banned from ranked.\n"
                f"Reason: **{active_ban.get('reason', 'No reason')}**\n"
                f"Expires: **{expires_text}**"
            )

        await self.cleanup_expired_party_invites(owner_party)

        for invite in self.normalize_party_invites(owner_party):
            if invite.get("invited_user_id") == user.id and invite.get("status") == "pending":
                return await ctx.send("That player already has a pending invite to your party.")

        invite = self.create_party_invite_doc(owner_party, user.id)
        self.normalize_party_invites(owner_party).append(invite)
        await self.touch_party(owner_party)

        embed = discord.Embed(
            title="📨 Ranked Party Invite",
            description=f"{user.mention}, you were invited to a ranked party.",
            color=discord.Color.blurple()
        )
        embed.add_field(name="Party ID", value=f"`{owner_party['_id']}`", inline=True)
        embed.add_field(name="Owner", value=ctx.author.mention, inline=True)
        embed.add_field(name="Expires", value=discord.utils.format_dt(invite["expires_at"], style="R"), inline=False)
        embed.add_field(
            name="Members",
            value="\n".join(f"<@{m}>" for m in owner_party["members"]),
            inline=False
        )
        embed.set_footer(text="You can use the buttons below or the partyaccept / partydecline commands.")

        await ctx.send(embed=embed, view=PartyInviteView(self, invite["_id"]))

        await self.send_ranked_log(
            ctx.guild,
            "📨 Party Invite Sent",
            f"{ctx.author.mention} invited {user.mention} to a ranked party.",
            color=discord.Color.blurple(),
            fields=[
                ("Party ID", f"`{owner_party['_id']}`", True),
                ("Invite ID", f"`{invite['_id']}`", True),
            ]
        )

    @commands.command()
    async def partyaccept(self, ctx, invite_id: str = None):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        if invite_id:
            ok, message = await self.accept_party_invite(ctx.guild, ctx.author.id, invite_id)
            return await ctx.send(message)

        party, invite = await self.get_pending_party_invite_for_user(ctx.guild.id, ctx.author.id)
        if not party or not invite:
            return await ctx.send("You do not have any pending party invite.")

        ok, message = await self.accept_party_invite(ctx.guild, ctx.author.id, invite["_id"])
        await ctx.send(message)

    @commands.command()
    async def partydecline(self, ctx, invite_id: str = None):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        if invite_id:
            ok, message = await self.decline_party_invite(ctx.guild, ctx.author.id, invite_id)
            return await ctx.send(message)

        party, invite = await self.get_pending_party_invite_for_user(ctx.guild.id, ctx.author.id)
        if not party or not invite:
            return await ctx.send("You do not have any pending party invite.")

        ok, message = await self.decline_party_invite(ctx.guild, ctx.author.id, invite["_id"])
        await ctx.send(message)

    @commands.command()
    async def partyleave(self, ctx):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        party = await self.get_party_for_user(ctx.guild.id, ctx.author.id)
        if not party:
            return await ctx.send("You are not in a party.")

        if party["owner_id"] == ctx.author.id:
            await self.disband_party(ctx.guild, party, reason="Party disbanded by owner.")
            await self.send_ranked_log(
                ctx.guild,
                "🗑️ Party Disbanded",
                f"{ctx.author.mention} disbanded a ranked party.",
                color=discord.Color.orange(),
                fields=[
                    ("Party ID", f"`{party['_id']}`", True),
                    ("Owner", ctx.author.mention, True),
                ]
            )
            return await ctx.send("Your party was disbanded.")

        party["members"] = [m for m in party["members"] if m != ctx.author.id]
        await self.touch_party(party)
        await self.remove_party_from_all_queues(ctx.guild.id, party["_id"])
        await self.refresh_all_queue_messages(ctx.guild)

        await self.send_ranked_log(
            ctx.guild,
            "↩️ Party Leave",
            f"{ctx.author.mention} left a ranked party.",
            color=discord.Color.orange(),
            fields=[
                ("Party ID", f"`{party['_id']}`", True),
                ("Owner", f"<@{party['owner_id']}>", True),
            ]
        )

        await ctx.send("You left the party.")

    @commands.command()
    async def partydisband(self, ctx):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        party = await self.get_party_by_owner(ctx.guild.id, ctx.author.id)
        if not party:
            return await ctx.send("You do not own a party.")

        await self.disband_party(ctx.guild, party, reason="Party disbanded by owner.")

        await self.send_ranked_log(
            ctx.guild,
            "🗑️ Party Disbanded",
            f"{ctx.author.mention} disbanded a ranked party.",
            color=discord.Color.orange(),
            fields=[
                ("Party ID", f"`{party['_id']}`", True),
                ("Owner", ctx.author.mention, True),
            ]
        )

        await ctx.send("Party disbanded.")

    @commands.command()
    async def partykick(self, ctx, user: discord.Member):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        party = await self.get_party_by_owner(ctx.guild.id, ctx.author.id)
        if not party:
            return await ctx.send("You do not own a party.")

        if user.id == ctx.author.id:
            return await ctx.send("You cannot kick yourself. Use `partydisband` or `partyleave`.")

        if user.id not in party["members"]:
            return await ctx.send("That player is not in your party.")

        party["members"] = [m for m in party["members"] if m != user.id]
        await self.touch_party(party)
        await self.remove_party_from_all_queues(ctx.guild.id, party["_id"])
        await self.refresh_all_queue_messages(ctx.guild)

        await self.send_ranked_log(
            ctx.guild,
            "👢 Party Kick",
            f"{ctx.author.mention} kicked {user.mention} from a ranked party.",
            color=discord.Color.orange(),
            fields=[
                ("Party ID", f"`{party['_id']}`", True),
                ("Owner", ctx.author.mention, True),
            ]
        )

        await ctx.send(f"Kicked {user.mention} from the party.")

    @commands.command()
    async def partytransfer(self, ctx, user: discord.Member):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        party = await self.get_party_by_owner(ctx.guild.id, ctx.author.id)
        if not party:
            return await ctx.send("You do not own a party.")

        if user.id not in party["members"]:
            return await ctx.send("That player is not in your party.")

        if user.id == ctx.author.id:
            return await ctx.send("You already own the party.")

        old_key = self.build_party_cache_key(ctx.guild.id, party["owner_id"])
        self.parties.pop(old_key, None)

        party["owner_id"] = user.id
        await self.touch_party(party)

        new_key = self.build_party_cache_key(ctx.guild.id, party["owner_id"])
        self.parties[new_key] = party

        await self.send_ranked_log(
            ctx.guild,
            "🔁 Party Ownership Transferred",
            f"{ctx.author.mention} transferred party ownership to {user.mention}.",
            color=discord.Color.green(),
            fields=[
                ("Party ID", f"`{party['_id']}`", True),
                ("New Owner", user.mention, True),
            ]
        )

        await ctx.send(f"Transferred party ownership to {user.mention}.")

    @commands.command()
    async def party(self, ctx):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        party = await self.get_party_for_user(ctx.guild.id, ctx.author.id)
        if not party:
            return await ctx.send("You are not in a party.")

        await self.cleanup_expired_party_invites(party)

        pending_invites = [
            invite for invite in self.normalize_party_invites(party)
            if invite.get("status") == "pending" and not self.is_party_invite_expired(invite)
        ]

        embed = discord.Embed(
            title="👥 Ranked Party",
            color=discord.Color.blurple()
        )
        embed.add_field(name="Party ID", value=f"`{party['_id']}`", inline=False)
        embed.add_field(name="Owner", value=f"<@{party['owner_id']}>", inline=False)
        embed.add_field(name="Members", value="\n".join(f"<@{m}>" for m in party["members"]), inline=False)

        last_activity = party.get("last_activity_at")
        if last_activity:
            embed.add_field(name="Last Activity", value=discord.utils.format_dt(last_activity, style="R"), inline=False)

        if pending_invites:
            embed.add_field(
                name="Pending Invites",
                value="\n".join(
                    f"`{invite['_id']}` • <@{invite['invited_user_id']}> • expires {discord.utils.format_dt(invite['expires_at'], style='R')}"
                    for invite in pending_invites[:10]
                ),
                inline=False
            )

        await ctx.send(embed=embed)

    @commands.command()
    async def invites(self, ctx):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        embed = await self.build_pending_invites_embed(ctx.guild, ctx.author.id)
        await ctx.send(embed=embed)

    @commands.command()
    async def partyqueue(self, ctx):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        embed = await self.build_party_queue_embed(ctx.guild, ctx.author.id)
        await ctx.send(embed=embed)

    @commands.command()
    async def partyowner(self, ctx):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        embed = await self.build_party_owner_embed(ctx.guild, ctx.author.id)
        await ctx.send(embed=embed)

    @commands.command()
    async def createteam(self, ctx, *, name: str):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        existing = await self.get_team_for_user(ctx.guild.id, ctx.author.id)
        if existing:
            return await ctx.send("You are already in a team.")

        if len(name.strip()) < 2 or len(name.strip()) > 32:
            return await ctx.send("Team name must be between 2 and 32 characters.")

        duplicate = await teams_col.find_one({
            "guild_id": ctx.guild.id,
            "name": {"$regex": f"^{re.escape(name.strip())}$", "$options": "i"}
        })
        if duplicate:
            return await ctx.send("A team with that name already exists.")

        team = self.build_default_team_doc(ctx.guild.id, ctx.author.id, name.strip())
        current_season = await self.get_current_season_number(ctx.guild.id)
        self.ensure_team_season_state(team, current_season)

        await self.save_team(team)
        await self.sync_team_channel_message(ctx.guild, team)

        await self.send_ranked_log(
            ctx.guild,
            "🛡️ Team Created",
            f"{ctx.author.mention} created a ranked team.",
            color=discord.Color.blurple(),
            fields=[
                ("Team ID", f"`{team['_id']}`", True),
                ("Name", team["name"], True),
            ]
        )

        await ctx.send(f"Team created: **{team['name']}** • ID: `{team['_id']}`")

    @commands.command()
    async def team(self, ctx, team_id: str = None):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        if team_id:
            team = await self.get_team_by_id(team_id)
        else:
            team = await self.get_team_for_user(ctx.guild.id, ctx.author.id)

        if not team or team.get("guild_id") != ctx.guild.id:
            return await ctx.send("Team not found.")

        embed = await self.build_team_embed(ctx.guild, team)
        await ctx.send(embed=embed)

    @commands.command()
    async def disbandteam(self, ctx):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        team = await self.get_team_for_user(ctx.guild.id, ctx.author.id)
        if not team:
            return await ctx.send("You are not in a team.")

        if team["owner_id"] != ctx.author.id:
            return await ctx.send("Only the team owner can disband the team.")

        current_season = await self.get_current_season_number(ctx.guild.id)

        for member_id in team.get("members", []):
            profile = await self.get_season_profile(ctx.guild.id, member_id, current_season)
            profile["team_id"] = None
            profile["team_points"] = 0
            profile["team_contribution"] = {"wins": 0, "matches": 0, "elo_gained": 0}
            await self.save_season_profile(profile)

        await teams_col.delete_one({"_id": team["_id"]})

        await self.send_ranked_log(
            ctx.guild,
            "🗑️ Team Disbanded",
            f"{ctx.author.mention} disbanded a ranked team.",
            color=discord.Color.orange(),
            fields=[
                ("Team ID", f"`{team['_id']}`", True),
                ("Name", team["name"], True),
            ]
        )

        await ctx.send(f"Disbanded team **{team['name']}**.")

    @commands.command()
    async def teaminvites(self, ctx):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        embed = await self.build_pending_team_invites_embed(ctx.guild, ctx.author.id)
        await ctx.send(embed=embed)

    @commands.command()
    async def teamleave(self, ctx):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        team = await self.get_team_for_user(ctx.guild.id, ctx.author.id)
        if not team:
            return await ctx.send("You are not in a team.")

        if team["owner_id"] == ctx.author.id:
            return await ctx.send("You are the team owner. Use `disbandteam` or `teamtransfer` first.")

        team["members"] = [m for m in team.get("members", []) if m != ctx.author.id]
        await self.save_team(team)
        await self.sync_team_channel_message(ctx.guild, team)

        current_season = await self.get_current_season_number(ctx.guild.id)
        profile = await self.get_season_profile(ctx.guild.id, ctx.author.id, current_season)
        profile["team_id"] = None
        profile["team_points"] = 0
        profile["team_contribution"] = {"wins": 0, "matches": 0, "elo_gained": 0}
        await self.save_season_profile(profile)

        await self.send_ranked_log(
            ctx.guild,
            "↩️ Team Leave",
            f"{ctx.author.mention} left a ranked team.",
            color=discord.Color.orange(),
            fields=[
                ("Team ID", f"`{team['_id']}`", True),
                ("Team", team["name"], True),
            ]
        )

        await ctx.send(f"You left **{team['name']}**.")

    @commands.command()
    async def teaminvite(self, ctx, user: discord.Member):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        if user.bot:
            return await ctx.send("You cannot invite bots.")

        if user.id == ctx.author.id:
            return await ctx.send("You cannot invite yourself.")

        team = await self.get_team_for_user(ctx.guild.id, ctx.author.id)
        if not team:
            return await ctx.send("You are not in a team.")

        if team["owner_id"] != ctx.author.id:
            return await ctx.send("Only the team owner can invite members.")

        if user.id in team.get("members", []):
            return await ctx.send("That player is already in your team.")

        if len(team.get("members", [])) >= 8:
            return await ctx.send("Your team is already full.")

        other_team = await self.get_team_for_user(ctx.guild.id, user.id)
        if other_team:
            return await ctx.send("That player is already in a team.")

        active_ban = await self.get_active_ranked_ban(ctx.guild.id, user.id)
        if active_ban:
            expires_text = self.format_ban_expiry(active_ban.get("expires_at"))
            return await ctx.send(
                f"That player is banned from ranked.\n"
                f"Reason: **{active_ban.get('reason', 'No reason')}**\n"
                f"Expires: **{expires_text}**"
            )

        await self.cleanup_expired_team_invites(team)

        for invite in self.normalize_team_invites(team):
            if invite.get("invited_user_id") == user.id and invite.get("status") == "pending":
                return await ctx.send("That player already has a pending team invite.")

        invite = self.create_team_invite_doc(team, user.id)
        self.normalize_team_invites(team).append(invite)
        await self.save_team(team)
        await self.sync_team_channel_message(ctx.guild, team)

        embed = discord.Embed(
            title="📨 Team Invite",
            description=f"{user.mention}, you were invited to join **{team['name']}**.",
            color=discord.Color.blurple()
        )
        embed.add_field(name="Team ID", value=f"`{team['_id']}`", inline=True)
        embed.add_field(name="Owner", value=ctx.author.mention, inline=True)
        embed.add_field(name="Expires", value=discord.utils.format_dt(invite["expires_at"], style="R"), inline=False)
        embed.add_field(
            name="Members",
            value="\n".join(f"<@{m}>" for m in team.get("members", [])),
            inline=False
        )
        embed.set_footer(text="Use the buttons below or the teamaccept / teamdecline commands.")

        await ctx.send(embed=embed, view=TeamInviteView(self, invite["_id"]))

        await self.send_ranked_log(
            ctx.guild,
            "📨 Team Invite Sent",
            f"{ctx.author.mention} invited {user.mention} to a ranked team.",
            color=discord.Color.blurple(),
            fields=[
                ("Team ID", f"`{team['_id']}`", True),
                ("Team", team["name"], True),
                ("Invite ID", f"`{invite['_id']}`", True),
            ]
        )

    @commands.command()
    async def teamaccept(self, ctx, invite_id: str = None):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        if invite_id:
            ok, message = await self.accept_team_invite(ctx.guild, ctx.author.id, invite_id)
            return await ctx.send(message)

        team, invite = await self.get_pending_team_invite_for_user(ctx.guild.id, ctx.author.id)
        if not team or not invite:
            return await ctx.send("You do not have any pending team invite.")

        ok, message = await self.accept_team_invite(ctx.guild, ctx.author.id, invite["_id"])
        await ctx.send(message)

    @commands.command()
    async def teamdecline(self, ctx, invite_id: str = None):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        if invite_id:
            ok, message = await self.decline_team_invite(ctx.guild, ctx.author.id, invite_id)
            return await ctx.send(message)

        team, invite = await self.get_pending_team_invite_for_user(ctx.guild.id, ctx.author.id)
        if not team or not invite:
            return await ctx.send("You do not have any pending team invite.")

        ok, message = await self.decline_team_invite(ctx.guild, ctx.author.id, invite["_id"])
        await ctx.send(message)

    @commands.command()
    async def teamkick(self, ctx, user: discord.Member):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        team = await self.get_team_for_user(ctx.guild.id, ctx.author.id)
        if not team:
            return await ctx.send("You are not in a team.")

        if team["owner_id"] != ctx.author.id:
            return await ctx.send("Only the team owner can kick members.")

        if user.id == ctx.author.id:
            return await ctx.send("You cannot kick yourself.")

        if user.id not in team.get("members", []):
            return await ctx.send("That player is not in your team.")

        team["members"] = [m for m in team["members"] if m != user.id]
        await self.save_team(team)

        current_season = await self.get_current_season_number(ctx.guild.id)
        profile = await self.get_season_profile(ctx.guild.id, user.id, current_season)
        profile["team_id"] = None
        profile["team_points"] = 0
        profile["team_contribution"] = {"wins": 0, "matches": 0, "elo_gained": 0}
        await self.save_season_profile(profile)

        await self.sync_team_channel_message(ctx.guild, team)

        await self.send_ranked_log(
            ctx.guild,
            "👢 Team Kick",
            f"{ctx.author.mention} kicked {user.mention} from a ranked team.",
            color=discord.Color.orange(),
            fields=[
                ("Team ID", f"`{team['_id']}`", True),
                ("Name", team["name"], True),
            ]
        )

        await ctx.send(f"Kicked {user.mention} from **{team['name']}**.")

    @commands.command()
    async def teamtransfer(self, ctx, user: discord.Member):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        team = await self.get_team_for_user(ctx.guild.id, ctx.author.id)
        if not team:
            return await ctx.send("You are not in a team.")

        if team["owner_id"] != ctx.author.id:
            return await ctx.send("Only the team owner can transfer ownership.")

        if user.id not in team.get("members", []):
            return await ctx.send("That player is not in your team.")

        if user.id == ctx.author.id:
            return await ctx.send("You already own the team.")

        team["owner_id"] = user.id
        await self.save_team(team)
        await self.sync_team_channel_message(ctx.guild, team)

        await self.send_ranked_log(
            ctx.guild,
            "🔁 Team Ownership Transferred",
            f"{ctx.author.mention} transferred team ownership to {user.mention}.",
            color=discord.Color.green(),
            fields=[
                ("Team ID", f"`{team['_id']}`", True),
                ("Name", team["name"], True),
            ]
        )

        await ctx.send(f"Transferred ownership of **{team['name']}** to {user.mention}.")

    @commands.command()
    async def teamleaderboard(self, ctx):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        total_pages = await self.get_team_leaderboard_total_pages(ctx.guild.id, per_page=10)
        embed = await self.build_team_leaderboard_embed(ctx.guild, page=1, per_page=10)
        view = SimplePaginationView(
            self,
            self.build_team_leaderboard_embed,
            1,
            total_pages,
            ctx.guild,
            10
        )
        await ctx.send(embed=embed, view=view)

    @commands.command()
    async def teamquests(self, ctx):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        team = await self.get_team_for_user(ctx.guild.id, ctx.author.id)
        if not team:
            return await ctx.send("You are not in a team.")

        embed = await self.build_team_quests_embed(ctx.guild, team)
        await ctx.send(embed=embed)

    @commands.command()
    async def claimteamquest(self, ctx, period: str, quest_id: str):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        period = period.lower()
        if period not in ("weekly", "seasonal"):
            return await ctx.send("Team quest period must be `weekly` or `seasonal`.")

        ok, message = await self.claim_team_quest(ctx.guild, ctx.author.id, period, quest_id)
        await ctx.send(message)

    @commands.command()
    async def claimallteamquests(self, ctx):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        ok, message = await self.claim_all_team_quests(ctx.guild, ctx.author.id)
        await ctx.send(message)

    @commands.command()
    async def peak(self, ctx, user: discord.Member = None):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        target = user or ctx.author
        embed = await self.build_peak_embed(ctx.guild, target.id)
        await ctx.send(embed=embed)

    @commands.command()
    async def queue(self, ctx):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        embed = discord.Embed(
            title="🎮 Ranked Queues",
            color=discord.Color.blurple()
        )

        queue_state = self.get_guild_queue_state(ctx.guild.id)
        for mode in ("1v1", "2v2", "3v3", "4v4"):
            entries = queue_state.get(mode, [])
            players = self.flatten_queue_members(ctx.guild.id, mode)
            required = int(mode[0]) * 2

            value = f"`{len(players)}/{required}` players • `{len(entries)}` group(s)"
            if entries:
                preview = []
                for entry in entries[:5]:
                    members = entry.get("members", [])
                    if entry.get("party_id"):
                        preview.append(f"Party `{entry['party_id']}` • " + ", ".join(f"<@{m}>" for m in members))
                    else:
                        preview.append(", ".join(f"<@{m}>" for m in members))
                value += "\n" + "\n".join(preview)

            embed.add_field(name=mode, value=value, inline=False)

        await ctx.send(embed=embed)

    @commands.command()
    async def whereisqueue(self, ctx, mode: str):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        mode = self.normalize_leaderboard_mode(mode)
        if mode not in {"1v1", "2v2", "3v3", "4v4"}:
            return await ctx.send("Invalid mode. Use 1v1, 2v2, 3v3 or 4v4.")

        config = await self.get_config(ctx.guild.id)
        channel_id = config.get("queue_channels", {}).get(mode)
        message_id = config.get("queue_messages", {}).get(mode)

        if not channel_id:
            return await ctx.send(f"No queue channel is configured for **{mode}**.")

        jump_url = None
        if message_id:
            jump_url = f"https://discord.com/channels/{ctx.guild.id}/{channel_id}/{message_id}"

        embed = discord.Embed(
            title=f"📍 Queue Location • {mode}",
            color=discord.Color.blurple()
        )
        embed.add_field(name="Channel", value=f"<#{channel_id}>", inline=False)
        embed.add_field(name="Message ID", value=str(message_id) if message_id else "Not set", inline=False)
        embed.add_field(name="Panel Link", value=f"[Go to queue]({jump_url})" if jump_url else "Queue message not set", inline=False)

        await ctx.send(embed=embed)

    @commands.command()
    async def myqueue(self, ctx):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        user_id = ctx.author.id

        for mode, entries in self.get_guild_queue_state(ctx.guild.id).items():
            for position, entry in enumerate(entries, start=1):
                if user_id in entry.get("members", []):
                    label = "party/group" if entry.get("party_id") else "entry"
                    await ctx.send(
                        f"You are currently in the **{mode}** queue at {label} position **#{position}**."
                    )
                    return

        await ctx.send("You are not currently in any ranked queue.")

    @commands.command()
    async def leaveallqueues(self, ctx):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        user_id = ctx.author.id
        removed_modes = []
        queue_state = self.get_guild_queue_state(ctx.guild.id)

        for mode, entries in queue_state.items():
            target_entry = None
            for entry in entries:
                if user_id in entry.get("members", []):
                    target_entry = entry
                    break

            if target_entry:
                entries.remove(target_entry)
                removed_modes.append(mode)
                await self.refresh_queue_message(ctx.guild, mode)

        if removed_modes:
            await self.save_queue_state(ctx.guild.id)
            await ctx.send(f"You were removed from: **{', '.join(removed_modes)}**.")
        else:
            await ctx.send("You are not currently in any ranked queue.")

    @commands.command()
    async def mymatch(self, ctx):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        user_id = ctx.author.id

        matches = await matches_col.find({
            "players": user_id
        }).sort("created_at", -1).to_list(length=10)

        if not matches:
            return await ctx.send("You do not have any ranked matches yet.")

        active_match = next((m for m in matches if m.get("status") == "ongoing"), None)
        match = active_match or matches[0]

        mode = match.get("mode", "Unknown")
        match_id = match.get("_id", "Unknown")
        thread = await self.resolve_thread(ctx.guild, match.get("thread_id"))

        if thread:
            thread_value = f"[Open Thread]({thread.jump_url})"
        else:
            thread_value = "Thread unavailable"

        status_data = get_match_status_data(match)

        embed = discord.Embed(
            title="⚔️ Your Match",
            color=discord.Color.blurple()
        )
        embed.add_field(name="Match ID", value=f"`{match_id}`", inline=False)
        embed.add_field(name="Mode", value=mode, inline=True)
        embed.add_field(name="Thread", value=thread_value, inline=True)
        embed.add_field(name="Status", value=f"{status_data['emoji']} {status_data['label']}", inline=True)

        team1 = match.get("teams", {}).get("team1", [])
        team2 = match.get("teams", {}).get("team2", [])

        embed.add_field(name="Team A", value=", ".join(f"<@{p}>" for p in team1) or "Unknown", inline=False)
        embed.add_field(name="Team B", value=", ".join(f"<@{p}>" for p in team2) or "Unknown", inline=False)

        score = match.get("score")
        if score:
            embed.add_field(name="Score", value=f"`{score}`", inline=False)

        await ctx.send(embed=embed)

    @commands.command()
    async def reportmatch(self, ctx, *, reason: str):
        match = await self.find_match_for_context(ctx)

        if not match:
            return await ctx.send("Use this command inside an active match thread.")

        if ctx.author.id not in match.get("players", []):
            return await ctx.send("You are not part of this match.")

        await self.submit_match_report(
            ctx.guild,
            match,
            ctx.author.id,
            reason,
            ctx.channel if isinstance(ctx.channel, discord.Thread) else None
        )

        await ctx.send("Your report has been sent to staff.")


    @commands.command()
    async def cancelvote(self, ctx):
        match = await self.find_match_for_context(ctx)

        if not match:
            return await ctx.send("Use this command inside an active match thread.")

        ok, message, _ = await self.process_cancel_match_vote(
            ctx.guild,
            match,
            ctx.author.id
        )

        await ctx.send(message)

    @commands.command()
    async def season(self, ctx, season_number: int = None):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        if season_number is None:
            season_number = await self.get_current_season_number(ctx.guild.id)

        season_doc = await self.ensure_season_exists(ctx.guild.id, season_number)

        embed = discord.Embed(
            title=f"🌦️ Season {season_number}",
            color=discord.Color.blurple()
        )
        embed.add_field(name="Name", value=season_doc.get("name", f"Season {season_number}"), inline=True)
        embed.add_field(name="Status", value=season_doc.get("status", "unknown"), inline=True)
        embed.add_field(
            name="Started",
            value=discord.utils.format_dt(season_doc["started_at"], style="F") if season_doc.get("started_at") else "Unknown",
            inline=False
        )
        if season_doc.get("ended_at"):
            embed.add_field(
                name="Ended",
                value=discord.utils.format_dt(season_doc["ended_at"], style="F"),
                inline=False
            )
        embed.add_field(name="Team Ready", value="Yes", inline=True)
        await ctx.send(embed=embed)

    @commands.command()
    async def seasoninfo(self, ctx, season_number: int = None):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        if season_number is None:
            season_number = await self.get_current_season_number(ctx.guild.id)

        season_doc = await self.ensure_season_exists(ctx.guild.id, season_number)
        embed = discord.Embed(
            title=f"📚 Season {season_number} Info",
            color=discord.Color.blurple()
        )
        embed.add_field(name="Status", value=season_doc.get("status", "unknown"), inline=True)
        embed.add_field(name="Name", value=season_doc.get("name", f"Season {season_number}"), inline=True)
        embed.add_field(name="Started", value=discord.utils.format_dt(season_doc["started_at"], style="R") if season_doc.get("started_at") else "Unknown", inline=True)
        embed.add_field(
            name="Quest Types",
            value="Daily, Weekly, Monthly, Seasonal",
            inline=False
        )
        embed.add_field(
            name="Archive Support",
            value="Old seasons are preserved with leaderboard snapshots and player season profiles.",
            inline=False
        )
        await ctx.send(embed=embed)

    @commands.command()
    async def seasonprofile(self, ctx, user: discord.Member = None, season_number: int = None):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        target = user or ctx.author
        embed = await self.build_season_profile_embed(ctx.guild, target.id, season_number)
        await ctx.send(embed=embed)

    @commands.command()
    async def seasonquests(self, ctx, season_number: int = None):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        embed = await self.build_season_quests_page_embed(ctx.guild, ctx.author.id, season_number, page=1)
        view = SimplePaginationView(
            self,
            self.build_season_quests_page_embed,
            1,
            4,
            ctx.guild,
            ctx.author.id,
            season_number
        )
        await ctx.send(embed=embed, view=view)

    @commands.command()
    async def claimquest(self, ctx, period: str, quest_id: str, season_number: int = None):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        period = period.lower()
        if period not in ("daily", "weekly", "monthly", "seasonal"):
            return await ctx.send("Period must be one of: `daily`, `weekly`, `monthly`, `seasonal`.")

        ok, message = await self.claim_season_quest(ctx.guild, ctx.author.id, period, quest_id, season_number)
        await ctx.send(message)

    @commands.command()
    async def claimallquests(self, ctx, season_number: int = None):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        ok, message = await self.claim_all_season_quests(ctx.guild, ctx.author.id, season_number)
        await ctx.send(message)

    @commands.command()
    async def seasonrewards(self, ctx, user: discord.Member = None, season_number: int = None):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        target = user or ctx.author
        embed = await self.build_season_rewards_embed(ctx.guild, target.id, season_number)
        await ctx.send(embed=embed)

    @commands.command()
    async def claimseasonreward(self, ctx, reward_level: int, season_number: int = None):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        ok, message = await self.claim_season_reward(ctx.guild, ctx.author.id, reward_level, season_number)
        await ctx.send(message)

    @commands.command()
    async def claimallseasonrewards(self, ctx, season_number: int = None):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        ok, message = await self.claim_all_season_rewards(ctx.guild, ctx.author.id, season_number)
        await ctx.send(message)

    @commands.command()
    async def dailylogin(self, ctx, season_number: int = None):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        ok, message = await self.claim_daily_login_reward(ctx.guild, ctx.author.id, season_number)
        await ctx.send(message)

    @commands.command()
    async def dailyloginstatus(self, ctx, season_number: int = None):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        embed = await self.build_daily_login_embed(ctx.guild, ctx.author.id, season_number)
        await ctx.send(embed=embed)

    @commands.command()
    async def restoretodaystreak(self, ctx, season_number: int = None):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        ok, message = await self.restore_daily_login_streak(ctx.guild, ctx.author.id, season_number)
        await ctx.send(message)

    @commands.command()
    async def dailyrestorestatus(self, ctx, season_number: int = None):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        embed = await self.build_daily_restore_embed(ctx.guild, ctx.author.id, season_number)
        await ctx.send(embed=embed)

    @commands.command()
    async def seasonprogress(self, ctx, user: discord.Member = None, season_number: int = None):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        target = user or ctx.author
        embed = await self.build_season_progress_embed(ctx.guild, target.id, season_number)
        await ctx.send(embed=embed)

    @commands.command()
    async def seasonstats(self, ctx, user: discord.Member = None, season_number: int = None):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        target = user or ctx.author
        embed = await self.build_season_stats_embed(ctx.guild, target.id, season_number)
        await ctx.send(embed=embed)

    @commands.command()
    async def seasongraph(self, ctx, graph_type: str = "games", user: discord.Member = None, season_number: int = None):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        target = user or ctx.author
        file, analytics = await self.build_season_graph_file(ctx.guild, target.id, season_number, graph_type=graph_type)

        if file is None:
            return await ctx.send("No season graph data available yet.")

        embed = discord.Embed(
            title=f"📊 Season Graph • {target.display_name if isinstance(target, discord.Member) else target.name}",
            color=discord.Color.blurple()
        )

        if analytics:
            embed.add_field(name="Tracked Points", value=str(analytics["points"]), inline=True)
            embed.add_field(name="Start → End", value=f"`{analytics['start_mmr']}` → `{analytics['end_mmr']}`", inline=True)
            embed.add_field(name="Peak", value=f"`{analytics['peak_mmr']}`", inline=True)

        embed.set_image(url="attachment://season_graph.png")
        await ctx.send(embed=embed, file=file)

    @commands.command()
    async def seasonleaderboard(self, ctx, season_number: int = None, mode: str = "global"):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        if season_number is None:
            season_number = await self.get_current_season_number(ctx.guild.id)

        total_pages = await self.get_season_leaderboard_total_pages(ctx.guild.id, season_number, mode=mode, per_page=20)
        embed = await self.build_season_leaderboard_embed(ctx.guild, season_number, mode=mode, page=1, per_page=20)
        view = SimplePaginationView(
            self,
            self.build_season_leaderboard_embed,
            1,
            total_pages,
            ctx.guild,
            season_number,
            mode,
            20
        )
        await ctx.send(embed=embed, view=view)

    @commands.command()
    async def seasonteamleaderboard(self, ctx, season_number: int = None):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        if season_number is None:
            season_number = await self.get_current_season_number(ctx.guild.id)

        total_pages = await self.get_season_team_leaderboard_total_pages(ctx.guild.id, season_number, per_page=10)
        embed = await self.build_season_team_leaderboard_embed(ctx.guild, season_number, page=1, per_page=10)
        view = SimplePaginationView(
            self,
            self.build_season_team_leaderboard_embed,
            1,
            total_pages,
            ctx.guild,
            season_number,
            10
        )
        await ctx.send(embed=embed, view=view)

    @commands.command()
    async def seasonhistory(self, ctx, season_number: int = None, user: discord.Member = None):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        target = user or ctx.author
        embed = await self.build_season_history_embed(ctx.guild, target.id, season_number)
        await ctx.send(embed=embed)

    @commands.command()
    async def seasonactivity(self, ctx, season_number: int = None):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        embed = await self.build_season_activity_embed(ctx.guild, season_number)
        await ctx.send(embed=embed)

    @commands.command()
    async def seasonrecap(self, ctx, season_number: int, user: discord.Member = None):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        target = user or ctx.author
        embed = await self.build_season_recap_embed(ctx.guild, target.id, season_number)
        await ctx.send(embed=embed)

    @commands.command()
    async def seasonarchive(self, ctx, season_number: int):
        if not await self.ensure_ranked_channel_ctx(ctx):
            return

        embed = await self.build_season_archive_embed(ctx.guild, season_number)
        await ctx.send(embed=embed)

    # ================= ADMIN =================

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def setelo(self, ctx, user: discord.Member, amount: int):
        player = await self.get_player(user.id)
        player["elo"] = max(0, amount)
        self.append_mmr_history_point(player, player["elo"], f"Admin set by {ctx.author.id}")

        await players_col.update_one({"_id": user.id}, {"$set": player})
        await self.sync_rank_roles_for_user_id(ctx.guild, user.id)

        await self.send_ranked_log(
            ctx.guild,
            "⚙️ MMR Set",
            f"{ctx.author.mention} manually set a player's MMR.",
            color=discord.Color.orange(),
            fields=[
                ("User", user.mention, True),
                ("New MMR", str(player["elo"]), True),
            ]
        )

        await ctx.send(f"Set **{user.display_name}** MMR to `{player['elo']}`.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def addelo(self, ctx, user: discord.Member, amount: int):
        player = await self.get_player(user.id)
        player["elo"] = max(0, player.get("elo", 0) + amount)
        self.append_mmr_history_point(player, player["elo"], f"Admin add by {ctx.author.id}")

        await players_col.update_one({"_id": user.id}, {"$set": player})
        await self.sync_rank_roles_for_user_id(ctx.guild, user.id)

        await self.send_ranked_log(
            ctx.guild,
            "⬆️ MMR Added",
            f"{ctx.author.mention} added MMR to a player.",
            color=discord.Color.orange(),
            fields=[
                ("User", user.mention, True),
                ("Amount", str(amount), True),
                ("New MMR", str(player["elo"]), True),
            ]
        )

        await ctx.send(f"Added `{amount}` MMR to **{user.display_name}**. New MMR: `{player['elo']}`.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def removeelo(self, ctx, user: discord.Member, amount: int):
        player = await self.get_player(user.id)
        player["elo"] = max(0, player.get("elo", 0) - amount)
        self.append_mmr_history_point(player, player["elo"], f"Admin remove by {ctx.author.id}")

        await players_col.update_one({"_id": user.id}, {"$set": player})
        await self.sync_rank_roles_for_user_id(ctx.guild, user.id)

        await self.send_ranked_log(
            ctx.guild,
            "⬇️ MMR Removed",
            f"{ctx.author.mention} removed MMR from a player.",
            color=discord.Color.orange(),
            fields=[
                ("User", user.mention, True),
                ("Amount", str(amount), True),
                ("New MMR", str(player["elo"]), True),
            ]
        )

        await ctx.send(f"Removed `{amount}` MMR from **{user.display_name}**. New MMR: `{player['elo']}`.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def setxp(self, ctx, user: discord.Member, amount: int):
        player = await self.get_player(user.id)
        player["xp"] = max(0, amount)

        await players_col.update_one({"_id": user.id}, {"$set": player})
        await ctx.send(f"Set **{user.display_name}** XP to `{player['xp']}`.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def addxp(self, ctx, user: discord.Member, amount: int):
        player = await self.get_player(user.id)
        player["xp"] = max(0, player.get("xp", 0) + amount)

        await players_col.update_one({"_id": user.id}, {"$set": player})
        await ctx.send(f"Added `{amount}` XP to **{user.display_name}**. New XP: `{player['xp']}`.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def removexp(self, ctx, user: discord.Member, amount: int):
        player = await self.get_player(user.id)
        player["xp"] = max(0, player.get("xp", 0) - amount)

        await players_col.update_one({"_id": user.id}, {"$set": player})
        await ctx.send(f"Removed `{amount}` XP from **{user.display_name}**. New XP: `{player['xp']}`.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def resetstreak(self, ctx, user: discord.Member):
        player = await self.get_player(user.id)
        player["streaks"] = self.build_default_streaks()

        await players_col.update_one({"_id": user.id}, {"$set": player})
        await ctx.send(f"Reset all streaks for **{user.display_name}**.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def setplacementgames(self, ctx, amount: int):
        if amount < 1:
            return await ctx.send("Placement games must be at least 1.")

        config = await self.get_config(ctx.guild.id)
        config["placement_games"] = amount

        await config_col.update_one({"_id": ctx.guild.id}, {"$set": config})
        await ctx.send(f"Set placement games to `{amount}`.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def resetplacements(self, ctx, user: discord.Member):
        player = await self.get_player(user.id)
        player["placements"] = {
            "completed": False,
            "matches_played": 0,
            "wins": 0,
            "losses": 0
        }

        await players_col.update_one({"_id": user.id}, {"$set": player})
        await ctx.send(f"Reset placements for **{user.display_name}**.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def resetplayer(self, ctx, user: discord.Member):
        reset_data = {
            "_id": user.id,
            "elo": 0,
            "xp": 0,
            "level": 1,
            "stats": {
                "global": {"wins": 0, "losses": 0, "matches": 0},
                "1v1": {"wins": 0, "losses": 0, "matches": 0},
                "2v2": {"wins": 0, "losses": 0, "matches": 0},
                "3v3": {"wins": 0, "losses": 0, "matches": 0},
                "4v4": {"wins": 0, "losses": 0, "matches": 0}
            },
            "streaks": self.build_default_streaks(),
            "history": [],
            "placements": {
                "completed": False,
                "matches_played": 0,
                "wins": 0,
                "losses": 0
            }
        }

        await players_col.update_one(
            {"_id": user.id},
            {"$set": reset_data},
            upsert=True
        )

        await ctx.send(f"Reset all ranked data for **{user.display_name}**.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def partyinfo(self, ctx, party_id: str):
        embed = await self.build_party_info_embed(ctx.guild, party_id)
        if embed is None:
            return await ctx.send("Party not found.")
        await ctx.send(embed=embed)

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def disbandparty(self, ctx, party_id: str):
        party = await parties_col.find_one({"_id": party_id, "guild_id": ctx.guild.id})
        if not party:
            return await ctx.send("Party not found.")

        normalized = self.normalize_party_doc(party)
        if not normalized:
            return await ctx.send("Party data is invalid.")

        await self.disband_party(ctx.guild, normalized, reason="Party disbanded by admin.")

        await self.send_ranked_log(
            ctx.guild,
            "🗑️ Party Disbanded By Admin",
            f"{ctx.author.mention} disbanded a ranked party.",
            color=discord.Color.orange(),
            fields=[
                ("Party ID", f"`{normalized['_id']}`", True),
                ("Owner", f"<@{normalized['owner_id']}>", True),
            ]
        )

        await ctx.send(f"Disbanded party `{party_id}`.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def forceleaveparty(self, ctx, user: discord.Member):
        party = await self.get_party_for_user(ctx.guild.id, user.id)
        if not party:
            return await ctx.send("That user is not in a party.")

        if party["owner_id"] == user.id:
            await self.disband_party(ctx.guild, party, reason="Party disbanded by admin force leave.")
            await self.send_ranked_log(
                ctx.guild,
                "🗑️ Party Disbanded By Force Leave",
                f"{ctx.author.mention} force-removed the owner and disbanded the party.",
                color=discord.Color.orange(),
                fields=[
                    ("Party ID", f"`{party['_id']}`", True),
                    ("Owner", user.mention, True),
                ]
            )
            return await ctx.send("The user was the owner, so the party was disbanded.")

        party["members"] = [m for m in party["members"] if m != user.id]
        await self.touch_party(party)
        await self.remove_party_from_all_queues(ctx.guild.id, party["_id"])
        await self.refresh_all_queue_messages(ctx.guild)

        await self.send_ranked_log(
            ctx.guild,
            "👢 Force Leave Party",
            f"{ctx.author.mention} force-removed a user from a party.",
            color=discord.Color.orange(),
            fields=[
                ("Party ID", f"`{party['_id']}`", True),
                ("User", user.mention, True),
            ]
        )

        await ctx.send(f"Removed {user.mention} from the party.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def forcepartytransfer(self, ctx, party_id: str, user: discord.Member):
        party = await parties_col.find_one({"_id": party_id, "guild_id": ctx.guild.id})
        if not party:
            return await ctx.send("Party not found.")

        normalized = self.normalize_party_doc(party)
        if not normalized:
            return await ctx.send("Party data is invalid.")

        if user.id not in normalized["members"]:
            return await ctx.send("That user is not in the target party.")

        old_key = self.build_party_cache_key(ctx.guild.id, normalized["owner_id"])
        self.parties.pop(old_key, None)

        normalized["owner_id"] = user.id
        await self.touch_party(normalized)

        new_key = self.build_party_cache_key(ctx.guild.id, normalized["owner_id"])
        self.parties[new_key] = normalized

        await self.send_ranked_log(
            ctx.guild,
            "🔁 Party Ownership Forced",
            f"{ctx.author.mention} force-transferred party ownership.",
            color=discord.Color.orange(),
            fields=[
                ("Party ID", f"`{normalized['_id']}`", True),
                ("New Owner", user.mention, True),
            ]
        )

        await ctx.send(f"Transferred ownership of party `{party_id}` to {user.mention}.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def partylist(self, ctx):
        total_pages = await self.get_party_list_total_pages(ctx.guild.id, per_page=10)
        embed = await self.build_party_list_embed(ctx.guild, page=1, per_page=10)
        view = SimplePaginationView(self, self.build_party_list_embed, 1, total_pages, ctx.guild, 10)
        await ctx.send(embed=embed, view=view)

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def rankedreportresolve(self, ctx, report_id: str, *, note: str = "Resolved"):
        result = await ranked_reports_col.update_one(
            {"_id": report_id},
            {
                "$set": {
                    "status": "resolved",
                    "resolved_by": ctx.author.id,
                    "resolved_at": discord.utils.utcnow(),
                    "resolution_note": note
                }
            }
        )

        if result.modified_count == 0:
            return await ctx.send("Report not found.")

        await self.send_ranked_log(
            ctx.guild,
            "✅ Ranked Report Resolved",
            f"{ctx.author.mention} resolved a ranked report.",
            color=discord.Color.green(),
            fields=[
                ("Report ID", f"`{report_id}`", True),
                ("Note", note, False),
            ]
        )

        await ctx.send(f"Resolved report `{report_id}`.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def rankedreportdismiss(self, ctx, report_id: str, *, note: str = "Dismissed"):
        result = await ranked_reports_col.update_one(
            {"_id": report_id},
            {
                "$set": {
                    "status": "dismissed",
                    "dismissed_by": ctx.author.id,
                    "dismissed_at": discord.utils.utcnow(),
                    "dismiss_note": note
                }
            }
        )

        if result.modified_count == 0:
            return await ctx.send("Report not found.")

        await self.send_ranked_log(
            ctx.guild,
            "🗑️ Ranked Report Dismissed",
            f"{ctx.author.mention} dismissed a ranked report.",
            color=discord.Color.orange(),
            fields=[
                ("Report ID", f"`{report_id}`", True),
                ("Note", note, False),
            ]
        )

        await ctx.send(f"Dismissed report `{report_id}`.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def rankedreportnote(self, ctx, report_id: str, *, note: str):
        result = await ranked_reports_col.update_one(
            {"_id": report_id},
            {
                "$push": {
                    "notes": {
                        "author_id": ctx.author.id,
                        "note": note,
                        "created_at": discord.utils.utcnow()
                    }
                }
            }
        )

        if result.modified_count == 0:
            return await ctx.send("Report not found.")

        await self.send_ranked_log(
            ctx.guild,
            "📝 Ranked Report Note Added",
            f"{ctx.author.mention} added a note to a ranked report.",
            color=discord.Color.blurple(),
            fields=[
                ("Report ID", f"`{report_id}`", True),
                ("Note", note, False),
            ]
        )

        await ctx.send(f"Added note to report `{report_id}`.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def warn(self, ctx, user: discord.Member, *, reason: str):
        warn_id = uuid.uuid4().hex[:12]
        await ranked_warns_col.insert_one({
            "_id": warn_id,
            "guild_id": ctx.guild.id,
            "user_id": user.id,
            "moderator_id": ctx.author.id,
            "reason": reason,
            "created_at": discord.utils.utcnow()
        })

        await self.send_ranked_log(
            ctx.guild,
            "⚠️ Ranked Warn Issued",
            f"{ctx.author.mention} warned a player.",
            color=discord.Color.orange(),
            fields=[
                ("Warn ID", f"`{warn_id}`", True),
                ("User", user.mention, True),
                ("Reason", reason, False),
            ]
        )

        await ctx.send(f"Warned {user.mention}. Warn ID: `{warn_id}`")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def warns(self, ctx, user: discord.Member):
        embed = await self.build_warns_embed(ctx.guild, user.id)
        await ctx.send(embed=embed)

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def warnlist(self, ctx):
        total_pages = await self.get_warns_leaderboard_total_pages(ctx.guild.id, per_page=15)
        embed = await self.build_warns_leaderboard_embed(ctx.guild, page=1, per_page=15)
        view = SimplePaginationView(self, self.build_warns_leaderboard_embed, 1, total_pages, ctx.guild, 15)
        await ctx.send(embed=embed, view=view)

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def setrankedlogs(self, ctx, channel: discord.TextChannel):
        config = await self.get_config(ctx.guild.id)
        config["ranked_logs_channel"] = channel.id
        await config_col.update_one({"_id": ctx.guild.id}, {"$set": {"ranked_logs_channel": channel.id}})
        await ctx.send(f"Ranked logs channel set to {channel.mention}.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def removerankedlogs(self, ctx):
        await config_col.update_one({"_id": ctx.guild.id}, {"$set": {"ranked_logs_channel": None}})
        await ctx.send("Ranked logs channel removed.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def setteamschannel(self, ctx, channel: discord.TextChannel):
        await config_col.update_one(
            {"_id": ctx.guild.id},
            {"$set": {"teams_channel_id": channel.id}},
            upsert=True
        )
        await ctx.send(f"Teams channel set to {channel.mention}.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def setrankrole(self, ctx, rank_name: str, role: discord.Role):
        normalized = self.normalize_rank_role_key(rank_name)
        valid_ranks = {self.normalize_rank_role_key(name): name for name, _, _ in RANKS}

        if normalized not in valid_ranks:
            return await ctx.send(f"Invalid rank. Valid ranks: {', '.join(name for name, _, _ in RANKS)}")

        config = await self.get_config(ctx.guild.id)
        rank_roles = config.get("rank_roles", {})
        rank_roles[normalized] = role.id

        await config_col.update_one(
            {"_id": ctx.guild.id},
            {"$set": {"rank_roles": rank_roles}}
        )

        await self.send_ranked_log(
            ctx.guild,
            "🏅 Rank Role Set",
            f"{ctx.author.mention} linked a ranked role.",
            color=discord.Color.gold(),
            fields=[
                ("Rank", valid_ranks[normalized], True),
                ("Role", role.mention, True),
            ]
        )

        await ctx.send(f"Set **{valid_ranks[normalized]}** rank role to {role.mention}.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def removerankrole(self, ctx, rank_name: str):
        normalized = self.normalize_rank_role_key(rank_name)
        valid_ranks = {self.normalize_rank_role_key(name): name for name, _, _ in RANKS}

        if normalized not in valid_ranks:
            return await ctx.send(f"Invalid rank. Valid ranks: {', '.join(name for name, _, _ in RANKS)}")

        config = await self.get_config(ctx.guild.id)
        rank_roles = config.get("rank_roles", {})

        if normalized not in rank_roles:
            return await ctx.send("No role is configured for that rank.")

        rank_roles.pop(normalized, None)

        await config_col.update_one(
            {"_id": ctx.guild.id},
            {"$set": {"rank_roles": rank_roles}}
        )

        await self.send_ranked_log(
            ctx.guild,
            "🗑️ Rank Role Removed",
            f"{ctx.author.mention} removed a ranked role mapping.",
            color=discord.Color.orange(),
            fields=[
                ("Rank", valid_ranks[normalized], False),
            ]
        )

        await ctx.send(f"Removed the role mapping for **{valid_ranks[normalized]}**.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def syncrankroles(self, ctx, user: discord.Member = None):
        if user:
            await self.sync_member_rank_role(ctx.guild, user)
            await ctx.send(f"Synced ranked role for {user.mention}.")
            return

        synced = 0
        for member in ctx.guild.members:
            if member.bot:
                continue
            await self.sync_member_rank_role(ctx.guild, member)
            synced += 1

        await self.send_ranked_log(
            ctx.guild,
            "🔄 Rank Roles Synced",
            f"{ctx.author.mention} synced ranked roles.",
            color=discord.Color.green(),
            fields=[
                ("Members Processed", str(synced), False),
            ]
        )

        await ctx.send(f"Synced ranked roles for `{synced}` members.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def setping(self, ctx, role: discord.Role):
        await config_col.update_one({"_id": ctx.guild.id}, {"$set": {"ping_role_id": role.id}})
        await ctx.send(f"Ranked ping role set to {role.mention}.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def removeping(self, ctx):
        await config_col.update_one({"_id": ctx.guild.id}, {"$set": {"ping_role_id": None}})
        await ctx.send("Ranked ping role removed.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def rankedban(self, ctx, user: discord.Member, duration: str, *, reason: str):
        seconds = parse_ranked_duration(duration)

        if seconds is None:
            return await ctx.send("❌ Invalid duration. Use examples like `10m`, `2h`, `3d`, `4w`, `1mo`, or combined like `1mo2w3d4h5m6s`.")

        now = discord.utils.utcnow()
        expires_at = now + timedelta(seconds=seconds)
        pretty_duration = format_ranked_duration(seconds)
        expires_text = self.format_ban_expiry(expires_at)

        await ranked_bans_col.update_one(
            {
                "guild_id": ctx.guild.id,
                "user_id": user.id,
                "active": True
            },
            {
                "$set": {
                    "guild_id": ctx.guild.id,
                    "user_id": user.id,
                    "reason": reason,
                    "created_by": ctx.author.id,
                    "created_at": now,
                    "expires_at": expires_at,
                    "duration_seconds": seconds,
                    "duration_input": duration,
                    "active": True
                }
            },
            upsert=True
        )

        if self.remove_members_from_all_queue_entries(ctx.guild.id, [user.id]):
            await self.save_queue_state(ctx.guild.id)
            await self.refresh_all_queue_messages(ctx.guild)

        await self.send_ranked_log(
            ctx.guild,
            "⛔ Ranked Ban",
            f"{user.mention} was banned from ranked.",
            color=discord.Color.red(),
            fields=[
                ("Moderator", ctx.author.mention, True),
                ("Duration", pretty_duration, True),
                ("Expires", expires_text, True),
                ("Reason", reason, False),
            ]
        )

        await ctx.send(
            f"Banned **{user.display_name}** from ranked.\n"
            f"Duration: **{pretty_duration}**\n"
            f"Expires: **{expires_text}**"
        )

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def rankedunban(self, ctx, user: discord.Member):
        result = await ranked_bans_col.update_many(
            {
                "guild_id": ctx.guild.id,
                "user_id": user.id,
                "active": True
            },
            {
                "$set": {
                    "active": False,
                    "removed_by": ctx.author.id,
                    "removed_at": discord.utils.utcnow()
                }
            }
        )

        if result.modified_count == 0:
            return await ctx.send("That user does not have an active ranked ban.")

        await self.send_ranked_log(
            ctx.guild,
            "✅ Ranked Unban",
            f"{user.mention} was unbanned from ranked.",
            color=discord.Color.green(),
            fields=[
                ("Moderator", ctx.author.mention, True),
                ("User", user.mention, True),
            ]
        )

        await ctx.send(f"Removed ranked ban for **{user.display_name}**.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def rankedbanlist(self, ctx):
        embed = await self.build_ranked_ban_list_embed(ctx.guild)
        await ctx.send(embed=embed)

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def checkrankedban(self, ctx, user: discord.Member):
        ban = await self.get_active_ranked_ban(ctx.guild.id, user.id)
        if not ban:
            return await ctx.send(f"**{user.display_name}** does not have an active ranked ban.")

        expires_at = ban.get("expires_at")
        expires_text = self.format_ban_expiry(expires_at)

        embed = discord.Embed(
            title="⛔ Active Ranked Ban",
            color=discord.Color.red()
        )
        embed.add_field(name="User", value=user.mention, inline=True)
        embed.add_field(name="Moderator", value=f"<@{ban.get('created_by')}>" if ban.get("created_by") else "Unknown", inline=True)
        embed.add_field(name="Expires", value=expires_text, inline=False)
        embed.add_field(name="Reason", value=ban.get("reason", "No reason"), inline=False)

        await ctx.send(embed=embed)

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def playerreporthistory(self, ctx, user: discord.Member):
        embed = await self.build_player_report_history_embed(ctx.guild, user.id)
        await ctx.send(embed=embed)

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def debugmatch(self, ctx, match_id: str):
        embed = await self.build_debug_match_embed(ctx.guild, match_id)
        if embed is None:
            return await ctx.send("Match not found.")

        await ctx.send(embed=embed)

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def refreshmatch(self, ctx, match_id: str):
        match = await matches_col.find_one({"_id": match_id, "guild_id": ctx.guild.id})
        if not match:
            return await ctx.send("Match not found.")

        await self.update_match_visuals(ctx.guild, match_id)
        await self.refresh_match_panels(ctx.guild, match_id)

        await self.send_ranked_log(
            ctx.guild,
            "🛠️ Match Refreshed",
            f"{ctx.author.mention} refreshed match visuals and panels.",
            color=discord.Color.orange(),
            fields=[
                ("Match ID", f"`{match_id}`", False),
            ]
        )

        await ctx.send(f"Refreshed match visuals and live panels for `{match_id}`.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def rebuildmatchpanel(self, ctx, match_id: str):
        ok, message = await self.rebuild_match_panel_core(ctx.guild, match_id)
        if not ok:
            return await ctx.send(message)

        await self.update_match_visuals(ctx.guild, match_id)

        await self.send_ranked_log(
            ctx.guild,
            "🧩 Match Panel Rebuilt",
            f"{ctx.author.mention} rebuilt a match panel.",
            color=discord.Color.orange(),
            fields=[
                ("Match ID", f"`{match_id}`", False),
            ]
        )

        await ctx.send(f"Rebuilt match panel for `{match_id}`.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def resetmatchvotes(self, ctx, match_id: str):
        ok, message = await self.reset_match_votes_core(ctx.guild, match_id)
        if not ok:
            return await ctx.send(message)

        await self.send_ranked_log(
            ctx.guild,
            "🔄 Match Votes Reset",
            f"{ctx.author.mention} reset match votes.",
            color=discord.Color.orange(),
            fields=[
                ("Match ID", f"`{match_id}`", False),
            ]
        )

        await ctx.send(f"Reset match votes for `{match_id}`.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def reopenmatch(self, ctx, match_id: str):
        ok, message = await self.reopen_match_core(ctx.guild, match_id)
        if not ok:
            return await ctx.send(message)

        await self.send_ranked_log(
            ctx.guild,
            "✅ Match Reopened",
            f"{ctx.author.mention} reopened a cancelled match.",
            color=discord.Color.green(),
            fields=[
                ("Match ID", f"`{match_id}`", False),
            ]
        )

        await ctx.send(f"Reopened match `{match_id}`.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def forceresetscore(self, ctx, match_id: str):
        match = await matches_col.find_one({"_id": match_id})
        if not match:
            return await ctx.send("Match not found.")

        if match.get("status") != "ongoing":
            return await ctx.send("That match is not ongoing.")

        ok = await self.reset_match_score_state(ctx.guild, match_id)
        if not ok:
            return await ctx.send("Failed to reset score state.")

        await self.send_ranked_log(
            ctx.guild,
            "🔄 Score Reset Forced",
            f"{ctx.author.mention} reset a match score submission.",
            color=discord.Color.orange(),
            fields=[
                ("Match ID", f"`{match_id}`", False),
            ]
        )

        await ctx.send(f"Reset score submission for match `{match_id}`.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def cancelmatch(self, ctx, match_id: str):
        match = await matches_col.find_one({"_id": match_id})
        if not match:
            return await ctx.send("Match not found.")

        if match.get("status") != "ongoing":
            return await ctx.send("That match is not ongoing.")

        await matches_col.update_one(
            {"_id": match_id},
            {
                "$set": {
                    "status": "cancelled",
                    "finished_at": discord.utils.utcnow(),
                    "score_submission": None,
                    "score_confirmation": {
                        "target_team": None,
                        "confirmations": [],
                        "declines": []
                    },
                    "confirmation_message_id": None,
                    "score": "CANCELLED"
                }
            }
        )

        thread = await self.resolve_thread(ctx.guild, match.get("thread_id"))
        if thread:
            cancel_embed = discord.Embed(
                title="⚫ Match Cancelled",
                description=f"This match was cancelled by {ctx.author.mention}.",
                color=discord.Color.dark_grey()
            )
            cancel_embed.add_field(name="Match ID", value=f"`{match_id}`", inline=False)
            try:
                await thread.send(embed=cancel_embed)
            except discord.HTTPException:
                pass

        await self.update_match_visuals(ctx.guild, match_id)

        await self.send_ranked_log(
            ctx.guild,
            "⚫ Match Cancelled",
            f"{ctx.author.mention} cancelled a match.",
            color=discord.Color.dark_grey(),
            fields=[
                ("Match ID", f"`{match_id}`", False),
            ]
        )

        await ctx.send(f"Cancelled match `{match_id}`.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def forcewin(self, ctx, match_id: str, team: str):
        team = team.lower().strip()

        aliases = {
            "a": "team1",
            "teama": "team1",
            "team_a": "team1",
            "team1": "team1",
            "1": "team1",

            "b": "team2",
            "teamb": "team2",
            "team_b": "team2",
            "team2": "team2",
            "2": "team2",
        }

        winning_team_key = aliases.get(team)
        if winning_team_key is None:
            return await ctx.send("Invalid team. Use `team1`, `team2`, `a`, or `b`.")

        ok, error = await self.force_finalize_match(ctx.guild, match_id, winning_team_key)
        if not ok:
            return await ctx.send(error)

        await self.send_ranked_log(
            ctx.guild,
            "🏁 Match Force Finished",
            f"{ctx.author.mention} force-finished a match.",
            color=discord.Color.orange(),
            fields=[
                ("Match ID", f"`{match_id}`", False),
                ("Winning Team", winning_team_key, False),
            ]
        )

        await ctx.send(f"Force-finished match `{match_id}` with **{winning_team_key}** as the winner.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def setqueue(self, ctx, mode: str, channel: discord.TextChannel):
        if mode not in {"1v1", "2v2", "3v3", "4v4"}:
            return await ctx.send("Invalid mode. Use 1v1, 2v2, 3v3 or 4v4.")
            
        config = await self.get_config(ctx.guild.id)

        old_channel_id = config.get("queue_channels", {}).get(mode)
        old_message_id = config.get("queue_messages", {}).get(mode)

        if old_channel_id and old_message_id:
            old_channel = ctx.guild.get_channel(old_channel_id)
            if old_channel:
                try:
                    old_message = await old_channel.fetch_message(old_message_id)
                    await old_message.delete()
                except discord.NotFound:
                    pass
                except discord.HTTPException:
                    pass

        config["queue_channels"][mode] = channel.id
        await config_col.update_one({"_id": ctx.guild.id}, {"$set": config})

        await self.recreate_queue_message(ctx.guild, mode)
        await ctx.send(f"Queue setup for {mode}.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def setmatches(self, ctx, mode: str, channel: discord.TextChannel):
        if mode not in {"1v1", "2v2", "3v3", "4v4"}:
            return await ctx.send("Invalid mode. Use 1v1, 2v2, 3v3 or 4v4.")

        config = await self.get_config(ctx.guild.id)
        config.setdefault("match_channels", {})[mode] = channel.id

        await config_col.update_one({"_id": ctx.guild.id}, {"$set": config})
        await ctx.send(f"Match channel set for {mode}.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def setkfactor(self, ctx, value: int):
        if value < 1 or value > 100:
            return await ctx.send("K-factor must be between `1` and `100`.")

        await config_col.update_one(
            {"_id": ctx.guild.id},
            {"$set": {"k_factor": int(value)}},
            upsert=True
        )

        await self.send_ranked_log(
            ctx.guild,
            "⚙️ Global K-Factor Updated",
            f"{ctx.author.mention} changed the ranked K-factor.",
            color=discord.Color.orange(),
            fields=[
                ("New K-Factor", f"`{value}`", False),
            ]
        )

        await ctx.send(f"Set global fallback K-factor to `{value}`.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def showkfactor(self, ctx):
        config = await self.get_config(ctx.guild.id)
        await ctx.send(f"Current global ranked K-factor: `{int(config.get('k_factor', 40))}`")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def setmodekfactor(self, ctx, mode: str, value: int):
        mode = mode.lower()
        if mode not in {"1v1", "2v2", "3v3", "4v4"}:
            return await ctx.send("Invalid mode. Use `1v1`, `2v2`, `3v3`, or `4v4`.")

        if value < 1 or value > 100:
            return await ctx.send("Mode K-factor must be between `1` and `100`.")

        config = await self.get_config(ctx.guild.id)
        mode_k_factors = config.get("mode_k_factors", {
            "1v1": config.get("k_factor", 40),
            "2v2": config.get("k_factor", 40),
            "3v3": config.get("k_factor", 40),
            "4v4": config.get("k_factor", 40),
        })

        mode_k_factors[mode] = int(value)

        await config_col.update_one(
            {"_id": ctx.guild.id},
            {"$set": {"mode_k_factors": mode_k_factors}},
            upsert=True
        )

        await self.send_ranked_log(
            ctx.guild,
            "⚙️ Mode K-Factor Updated",
            f"{ctx.author.mention} changed a mode-specific K-factor.",
            color=discord.Color.orange(),
            fields=[
                ("Mode", mode, True),
                ("New K-Factor", f"`{value}`", True),
            ]
        )

        await ctx.send(f"Set **{mode}** K-factor to `{value}`.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def showmodekfactor(self, ctx):
        config = await self.get_config(ctx.guild.id)
        global_k = int(config.get("k_factor", 40))
        mode_k_factors = config.get("mode_k_factors", {})

        embed = discord.Embed(
            title="⚙️ Ranked K-Factors",
            color=discord.Color.blurple()
        )
        embed.add_field(name="Global Fallback", value=f"`{global_k}`", inline=False)

        for mode in ("1v1", "2v2", "3v3", "4v4"):
            value = mode_k_factors.get(mode, global_k)
            embed.add_field(name=mode, value=f"`{int(value)}`", inline=True)

        await ctx.send(embed=embed)

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def simulateelo(self, ctx, mode: str, player_elo: int, enemy_avg_elo: int, result: str, score_diff: int, k_override: int = None):
        mode = mode.lower()
        if mode not in {"1v1", "2v2", "3v3", "4v4"}:
            return await ctx.send("Invalid mode. Use `1v1`, `2v2`, `3v3`, or `4v4`.")

        result = result.lower()
        if result not in {"win", "won", "loss", "lose", "lost"}:
            return await ctx.send("Result must be `win` or `loss`.")

        if player_elo < 0 or enemy_avg_elo < 0:
            return await ctx.send("ELO values cannot be negative.")

        if score_diff < 1:
            return await ctx.send("Score difference must be at least `1`.")

        won = result in {"win", "won"}

        if k_override is not None:
            if k_override < 1 or k_override > 100:
                return await ctx.send("Override K-factor must be between `1` and `100`.")
            k_value = int(k_override)
        else:
            k_value = await self.get_mode_k_factor(ctx.guild.id, mode)

        expected = expected_score(player_elo, enemy_avg_elo)
        score_multiplier = get_score_margin_multiplier(score_diff)
        upset_multiplier = get_upset_bonus_multiplier(player_elo, enemy_avg_elo, won)
        favorite_multiplier = get_favorite_win_reduction_multiplier(player_elo, enemy_avg_elo, won)

        change = calculate_individual_elo_change(
            player_elo=player_elo,
            enemy_avg_elo=enemy_avg_elo,
            won=won,
            score_diff=score_diff,
            k=k_value
        )

        new_elo = player_elo + change

        embed = discord.Embed(
            title="🧪 ELO Simulation",
            color=discord.Color.blurple()
        )
        embed.add_field(name="Mode", value=mode, inline=True)
        embed.add_field(name="Result", value="Win" if won else "Loss", inline=True)
        embed.add_field(name="Score Diff", value=f"`{score_diff}`", inline=True)

        embed.add_field(name="Player ELO", value=f"`{player_elo}`", inline=True)
        embed.add_field(name="Enemy Avg ELO", value=f"`{enemy_avg_elo}`", inline=True)
        embed.add_field(name="K-Factor", value=f"`{k_value}`", inline=True)

        embed.add_field(name="Expected Score", value=f"`{expected:.4f}`", inline=True)
        embed.add_field(name="Score Multiplier", value=f"`{score_multiplier:.2f}`", inline=True)
        embed.add_field(name="Upset Multiplier", value=f"`{upset_multiplier:.2f}`", inline=True)
        embed.add_field(name="Favorite Multiplier", value=f"`{favorite_multiplier:.2f}`", inline=True)

        embed.add_field(name="ELO Change", value=f"`{change:+}`", inline=True)
        embed.add_field(name="New ELO", value=f"`{new_elo}`", inline=True)
        embed.add_field(name="Rank After", value=format_rank_display(get_rank_info(new_elo)["name"]), inline=False)

        embed.set_footer(text="Use the optional last argument to override the mode K-factor for testing.")
        await ctx.send(embed=embed)

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def removematches(self, ctx, mode: str):
        config = await self.get_config(ctx.guild.id)

        if "match_channels" in config:
            config["match_channels"].pop(mode, None)

        await config_col.update_one({"_id": ctx.guild.id}, {"$set": config})
        await ctx.send(f"Match channel removed for {mode}.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def setmatchlogs(self, ctx, mode: str, channel: discord.TextChannel):
        if mode not in {"1v1", "2v2", "3v3", "4v4"}:
            return await ctx.send("Invalid mode. Use 1v1, 2v2, 3v3 or 4v4.")

        config = await self.get_config(ctx.guild.id)
        config.setdefault("match_log_channels", {})[mode] = channel.id

        await config_col.update_one({"_id": ctx.guild.id}, {"$set": config})
        await ctx.send(f"Match log channel set for {mode}.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def removematchlogs(self, ctx, mode: str):
        config = await self.get_config(ctx.guild.id)

        if "match_log_channels" in config:
            config["match_log_channels"].pop(mode, None)

        await config_col.update_one({"_id": ctx.guild.id}, {"$set": config})
        await ctx.send(f"Match log channel removed for {mode}.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def setleaderboard(self, ctx, channel: discord.TextChannel, name: str, webhook_url: str = None):
        config = await self.get_config(ctx.guild.id)

        if webhook_url:
            webhook = discord.Webhook.from_url(webhook_url, session=self.bot.http._HTTPClient__session)
        else:
            webhook = await channel.create_webhook(name=name)

        embed = await self.build_leaderboard()
        msg = await webhook.send(embed=embed, wait=True)

        config["leaderboard"] = {
            "webhook_url": webhook.url,
            "message_id": msg.id
        }

        await config_col.update_one({"_id": ctx.guild.id}, {"$set": config})
        await ctx.send("Leaderboard setup complete.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def removeleaderboard(self, ctx):
        config = await self.get_config(ctx.guild.id)
        config["leaderboard"] = {}

        await config_col.update_one({"_id": ctx.guild.id}, {"$set": config})
        await ctx.send("Leaderboard removed.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def setrankedchannel(self, ctx):
        config = await self.get_config(ctx.guild.id)
        if ctx.channel.id not in config["allowed_channels"]:
            config["allowed_channels"].append(ctx.channel.id)
        await config_col.update_one({"_id": ctx.guild.id}, {"$set": config})
        await ctx.send("Channel added.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def removerankedchannel(self, ctx):
        config = await self.get_config(ctx.guild.id)
        if ctx.channel.id in config["allowed_channels"]:
            config["allowed_channels"].remove(ctx.channel.id)
        await config_col.update_one({"_id": ctx.guild.id}, {"$set": config})
        await ctx.send("Channel removed.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def refreshqueues(self, ctx):
        rebuilt = []
        failed = []

        for mode in ("1v1", "2v2", "3v3", "4v4"):
            try:
                ok = await self.recreate_queue_message(ctx.guild, mode)
                if ok:
                    rebuilt.append(mode)
                else:
                    failed.append(mode)
            except Exception as e:
                failed.append(f"{mode} ({e})")

        parts = []
        if rebuilt:
            parts.append(f"Rebuilt: **{', '.join(rebuilt)}**")
        if failed:
            parts.append(f"Failed / not configured: **{', '.join(failed)}**")

        await ctx.send("\n".join(parts) if parts else "No queue messages were refreshed.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def rebuildpartyqueues(self, ctx):
        seen_members = set()
        queue_state = self.get_guild_queue_state(ctx.guild.id)

        for mode, entries in queue_state.items():
            cleaned_entries = []

            for entry in entries:
                members = entry.get("members", [])
                if not isinstance(members, list):
                    continue

                normalized_members = []
                for member_id in members:
                    try:
                        member_id = int(member_id)
                    except (TypeError, ValueError):
                        continue

                    if member_id in seen_members:
                        continue

                    seen_members.add(member_id)
                    normalized_members.append(member_id)

                if not normalized_members:
                    continue

                cleaned_entries.append({
                    "party_id": entry.get("party_id"),
                    "members": normalized_members
                })

            queue_state[mode] = cleaned_entries

        await self.save_queue_state(ctx.guild.id)
        await self.refresh_all_queue_messages(ctx.guild)
        await ctx.send("Party queue state rebuilt and queue panels refreshed.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def seasonstart(self, ctx, season_number: int = None, *, name: str = None):
        current = await self.get_current_season_number(ctx.guild.id)

        if season_number is None:
            season_number = current + 1

        embed = discord.Embed(
            title="⚠️ Confirm Season Start",
            description=(
                f"You are about to start **Season {season_number}**.\n"
                f"This will archive the current active season if needed and make the new season live."
            ),
            color=discord.Color.orange()
        )
        embed.add_field(name="New Season", value=f"`{season_number}`", inline=True)
        embed.add_field(name="Name", value=name or f"Season {season_number}", inline=True)

        view = SeasonDangerConfirmView(
            self,
            "seasonstart",
            ctx.guild.id,
            ctx.author.id,
            {
                "season_number": season_number,
                "name": name
            }
        )

        await ctx.send(embed=embed, view=view)

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def seasonend(self, ctx, season_number: int = None, *, notes: str = None):
        if season_number is None:
            season_number = await self.get_current_season_number(ctx.guild.id)

        embed = discord.Embed(
            title="⚠️ Confirm Season Archive",
            description=(
                f"You are about to archive **Season {season_number}**.\n"
                f"This will save the season leaderboard and mark the season as archived."
            ),
            color=discord.Color.orange()
        )
        embed.add_field(name="Season", value=f"`{season_number}`", inline=True)
        embed.add_field(name="Notes", value=notes or "No notes", inline=False)

        view = SeasonDangerConfirmView(
            self,
            "seasonend",
            ctx.guild.id,
            ctx.author.id,
            {
                "season_number": season_number,
                "notes": notes
            }
        )

        await ctx.send(embed=embed, view=view)

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def setseason(self, ctx, season_number: int):
        embed = discord.Embed(
            title="⚠️ Confirm Active Season Change",
            description=(
                f"You are about to set the current active season to **Season {season_number}**.\n"
                f"This changes which season all live season commands use."
            ),
            color=discord.Color.orange()
        )
        embed.add_field(name="Target Season", value=f"`{season_number}`", inline=False)

        view = SeasonDangerConfirmView(
            self,
            "setseason",
            ctx.guild.id,
            ctx.author.id,
            {
                "season_number": season_number
            }
        )

        await ctx.send(embed=embed, view=view)

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def seasonsetname(self, ctx, season_number: int, *, name: str):
        await self.ensure_season_exists(ctx.guild.id, season_number)
        await ranked_seasons_col.update_one(
            {"guild_id": ctx.guild.id, "season_number": int(season_number)},
            {"$set": {"name": name}}
        )
        await ctx.send(f"Set Season {season_number} name to **{name}**.")

    @commands.command()
    @commands.has_permissions(administrator=True)
    async def seasonsnapshot(self, ctx, season_number: int = None, *, notes: str = None):
        if season_number is None:
            season_number = await self.get_current_season_number(ctx.guild.id)

        embed = discord.Embed(
            title="⚠️ Confirm Season Snapshot",
            description=(
                f"You are about to save a snapshot for **Season {season_number}**.\n"
                f"This keeps an archive copy of the current season standings."
            ),
            color=discord.Color.orange()
        )
        embed.add_field(name="Season", value=f"`{season_number}`", inline=True)
        embed.add_field(name="Notes", value=notes or "No notes", inline=False)

        view = SeasonDangerConfirmView(
            self,
            "seasonsnapshot",
            ctx.guild.id,
            ctx.author.id,
            {
                "season_number": season_number,
                "notes": notes
            }
        )

        await ctx.send(embed=embed, view=view)

    # ================= LEADERBOARD =================

    async def build_leaderboard(self):
        return await self.build_leaderboard_embed()

    async def update_leaderboard(self, guild, force=False):
        config = await self.get_config(guild.id)
        lb = config.get("leaderboard")

        if not lb:
            return

        hash_lines = []
        rank = 1

        async for player in players_col.find().sort("elo", -1).limit(15):
            hash_lines.append(f"{rank}:{player['_id']}:{player['elo']}")
            rank += 1

        current_hash = hashlib.md5("|".join(hash_lines).encode()).hexdigest()

        if not force and self.lb_cache.get(guild.id) == current_hash:
            return

        self.lb_cache[guild.id] = current_hash

        webhook = discord.Webhook.from_url(
            lb["webhook_url"],
            session=self.bot.http._HTTPClient__session
        )

        embed = await self.build_leaderboard()

        try:
            await webhook.edit_message(lb["message_id"], embed=embed)
        except:
            msg = await webhook.send(embed=embed, wait=True)
            lb["message_id"] = msg.id
            await config_col.update_one({"_id": guild.id}, {"$set": config})

    @tasks.loop(minutes=15)
    async def leaderboard_loop(self):
        for guild in self.bot.guilds:
            await self.update_leaderboard(guild)

    @tasks.loop(minutes=10)
    async def party_cleanup_loop(self):
        now = discord.utils.utcnow()
        cutoff = now - timedelta(hours=6)

        expired = []
        for party in self.parties.values():
            await self.cleanup_expired_party_invites(party)

            last_activity = party.get("last_activity_at") or party.get("created_at")
            if last_activity is None:
                continue

            if hasattr(last_activity, "tzinfo") and last_activity.tzinfo is None:
                last_activity = last_activity.replace(tzinfo=timezone.utc)

            if last_activity <= cutoff:
                expired.append(party)

        for party in expired:
            guild = self.bot.get_guild(party["guild_id"])
            await self.disband_party(guild, party, reason="Party expired from inactivity.")

    @party_cleanup_loop.before_loop
    async def before_party_cleanup_loop(self):
        await self.bot.wait_until_ready()

    @leaderboard_loop.before_loop
    async def before_lb_loop(self):
        await self.bot.wait_until_ready()


async def setup(bot):
    await bot.add_cog(Ranked(bot))
