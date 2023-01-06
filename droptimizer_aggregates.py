import requests
import csv
import os
import pandas as pd
from dotenv import load_dotenv
from blizzardapi import BlizzardApi

import gspread as gs
import gspread_dataframe as gd

load_dotenv()

client_id = os.getenv('BLIZZ_CLIENT')
client_secret = os.getenv('BLIZZ_SECRET')
api_client = BlizzardApi(client_id, client_secret)

spreadsheet_name = os.getenv('SPREADSHEET_NAME')
gc = gs.service_account(filename='creds.json')

# DK, DH, Lock
DREADFUL_HELM = '196590'
DREADFUL_SHOULDERS = '196589'
DREADFUL_CHEST = '196586'
DREADFUL_GLOVES = '196587'
DREADFUL_PANTS = '196588'

# Druid, Hunter, Mage
MYSTIC_HELM = '196600'
MYSTIC_SHOULDERS = '196599'
MYSTIC_CHEST = '196596'
MYSTIC_GLOVES = '196597'
MYSTIC_PANTS = '196598'

# Paladin, Priest, Shaman
VENERATED_HELM = '196605'
VENERATED_SHOULDERS = '196604'
VENERATED_CHEST = '196601'
VENERATED_GLOVES = '196602'
VENERATED_PANTS = '196603'

# Evoker, Monk, Rogue, Warrior
ZENITH_HELM = '196595'
ZENITH_SHOULDERS = '196594'
ZENITH_CHEST = '196591'
ZENITH_GLOVES = '196592'
ZENITH_PANTS = '196593'

tier_list = {
    # Death Knight
    '200405': DREADFUL_CHEST,
    '200407': DREADFUL_GLOVES,
    '200408': DREADFUL_HELM,
    '200409': DREADFUL_PANTS,
    '200410': DREADFUL_SHOULDERS,

    # Demon Hunter
    '200342': DREADFUL_CHEST,
    '200344': DREADFUL_GLOVES,
    '200345': DREADFUL_HELM,
    '200346': DREADFUL_PANTS,
    '200347': DREADFUL_SHOULDERS,

    # Druid
    '200351': MYSTIC_CHEST,
    '200353': MYSTIC_GLOVES,
    '200354': MYSTIC_HELM,
    '200355': MYSTIC_PANTS,
    '200356': MYSTIC_SHOULDERS,

    # Evoker
    '200378': ZENITH_CHEST,
    '200380': ZENITH_GLOVES,
    '200381': ZENITH_HELM,
    '200382': ZENITH_PANTS,
    '200383': ZENITH_SHOULDERS,

    # Hunter
    '200351': MYSTIC_CHEST,
    '200353': MYSTIC_GLOVES,
    '200354': MYSTIC_HELM,
    '200355': MYSTIC_PANTS,
    '200356': MYSTIC_SHOULDERS,

    # Mage
    '200315': MYSTIC_CHEST,
    '200317': MYSTIC_GLOVES,
    '200318': MYSTIC_HELM,
    '200319': MYSTIC_PANTS,
    '200320': MYSTIC_SHOULDERS,

    # Monk
    '200360': ZENITH_CHEST,
    '200362': ZENITH_GLOVES,
    '200363': ZENITH_HELM,
    '200364': ZENITH_PANTS,
    '200365': ZENITH_SHOULDERS,

    # Paladin
    '200414': VENERATED_CHEST,
    '200416': VENERATED_GLOVES,
    '200417': VENERATED_HELM,
    '200418': VENERATED_PANTS,
    '200419': VENERATED_SHOULDERS,

    # Priest
    '200324': VENERATED_CHEST,
    '200326': VENERATED_GLOVES,
    '200327': VENERATED_HELM,
    '200328': VENERATED_PANTS,
    '200329': VENERATED_SHOULDERS,

    # Rogue
    '200360': ZENITH_CHEST,
    '200362': ZENITH_GLOVES,
    '200363': ZENITH_HELM,
    '200364': ZENITH_PANTS,
    '200365': ZENITH_SHOULDERS,

    # Shaman
    '200396': VENERATED_CHEST,
    '200398': VENERATED_GLOVES,
    '200399': VENERATED_HELM,
    '200400': VENERATED_PANTS,
    '200401': VENERATED_SHOULDERS,

    # Warlock
    '200333': DREADFUL_CHEST,
    '200335': DREADFUL_GLOVES,
    '200336': DREADFUL_HELM,
    '200337': DREADFUL_PANTS,
    '200338': DREADFUL_SHOULDERS,

    # Warrior
    '200423': ZENITH_CHEST,
    '200425': ZENITH_GLOVES,
    '200426': ZENITH_HELM,
    '200427': ZENITH_PANTS,
    '200428': ZENITH_SHOULDERS,
}

# Item List from Blizz API for Caching
item_list = {}
def get_item(id):
    '''
    Retrieves item information from the Blizzard WoW GameData API using the item id.
    '''
    if id in tier_list:
        id = tier_list[id]
    if id not in item_list:
        item = api_client.wow.game_data.get_item('us', 'en_US', id)
        item_list[id] = item['name']
    return item_list[id]


# Boss List from Blizz API for Caching
boss_list = {
    '-44': 'Trash',
}
def get_boss(id):
    '''
    Retrieves boss information from the Blizzard WoW GameData API using the encounter id.
    '''
    if id not in boss_list:
        boss = api_client.wow.game_data.get_journal_encounter('us', 'en_US', id)
        boss_list[id] = boss['name']
    return boss_list[id]


def parse_report(data):
    '''
    Parses the data in a raidbots data.csv to find the % increase for each sim reported
    '''
    player = data[1][0]
    print('Parsing report for player ' + player + '.')

    report_data = {}

    # Get the Report Data
    baseline_dps = float(data[1][1])
    for sim in data[2:]:
        # Calculate the Sim Info
        sim_dps = float(sim[1])
        sim_diff = (sim_dps - baseline_dps)

        # Get the Item Name
        sim_name_list = sim[0].split('/')
        item_name = get_boss(sim_name_list[1]) + ' - ' + get_item(sim_name_list[3])

        # Add to data dictionary, choosing the highest sim if the item appears multiple times
        if item_name in report_data:
            report_data[item_name] = max(sim_diff, report_data[item_name])
        else:
            report_data[item_name] = sim_diff

    return player, report_data


def get_report(report_link):
    '''
    Downloads the Report Data from Raidbots using the Simple CSV data endpoint.
    '''
    report_link = report_link + '/data.csv'
    raw_data = None
    with requests.Session() as s:
        raw_data = s.get(report_link)
    decoded_content = raw_data.content.decode('utf-8')
    csv_data = csv.reader(decoded_content.splitlines(), delimiter=',')
    data_list = list(csv_data)
    return data_list


def write_dataframe_to_sheet(data, sheet):
    '''
    Writes a dataframe to the specified sheet
    '''
    sheet.clear()
    gd.set_with_dataframe(worksheet=sheet,dataframe=data,include_index=True,include_column_header=True,resize=True)


def get_boss_summary(data: dict):
    '''
    Grabs relevant statistics for each boss
    '''
    boss_data = {

    }
    for player in data:
        player_data = {}
        
        # Get the highest upgrade for each Encounter
        for item in data[player]:
            item_list = item.split('-')
            boss_name = item_list[0].strip()
            item_name = item_list[1].strip()
            if boss_name not in player_data:
                player_data[boss_name] = 0
            upgrade_value = max(0, data[player][item])
            player_data[boss_name] = max(upgrade_value, player_data[boss_name])
        
        # Add player data to for boss
        for boss in player_data:
            # If the boss is not in the data, add it and initialize its value
            if boss not in boss_data:
                boss_data[boss] = {
                    'count': 0,
                    'total': 0,
                    'max': 0
                }
            upgrade_value = player_data[boss]
            if upgrade_value > 100:
                boss_data[boss]['count'] += 1
                boss_data[boss]['total'] += upgrade_value
                boss_data[boss]['max'] = max(boss_data[boss]['max'], upgrade_value)

    return boss_data


def main():
    # Open the main spreadsheet
    spreadsheet = gc.open(os.getenv('SPREADSHEET_NAME'))

    # Retrieve list of reports
    links_sheet = spreadsheet.worksheet('Links')
    mythic_reports_list = links_sheet.col_values(2)[1:]
    heroic_reports_list = links_sheet.col_values(3)[1:]
    normal_reports_list = links_sheet.col_values(4)[1:]

    # Run Mythic Parses
    mythic_dataframe = None
    mythic_data = {}
    if len(mythic_reports_list) > 0:
        for report_link in mythic_reports_list:
            # Download report data
            report_data = get_report(report_link)
            player, data = parse_report(report_data)
            mythic_data[player] = data
        mythic_dataframe = pd.DataFrame(data=mythic_data)
        mythic_sheet = spreadsheet.worksheet('Mythic')
        write_dataframe_to_sheet(mythic_dataframe, mythic_sheet)

    # Run Heroic Parses
    heroic_dataframe = None
    heroic_data = {}
    if len(heroic_reports_list) > 0:
        for report_link in heroic_reports_list:
            # Download report data
            report_data = get_report(report_link)
            player, data = parse_report(report_data)
            heroic_data[player] = data
        heroic_dataframe = pd.DataFrame(data=heroic_data)
        heroic_sheet = spreadsheet.worksheet('Heroic')
        write_dataframe_to_sheet(heroic_dataframe, heroic_sheet)

    # Run Normal Parses
    normal_dataframe = None
    normal_data = {}
    if len(normal_reports_list) > 0:
        for report_link in normal_reports_list:
            # Download report data
            report_data = get_report(report_link)
            player, data = parse_report(report_data)
            normal_data[player] = data
        normal_dataframe = pd.DataFrame(data=normal_data)
        normal_sheet = spreadsheet.worksheet('Normal')
        write_dataframe_to_sheet(normal_dataframe, normal_sheet)

    # Get boss summaries
    summary_sheet = spreadsheet.worksheet('Summary')
    if mythic_dataframe is not None:
        mythic_summary = get_boss_summary(mythic_data)
        summary_dataframe = pd.DataFrame(data=mythic_summary).transpose().sort_index()
        gd.set_with_dataframe(worksheet=summary_sheet, dataframe=summary_dataframe,
            row=3, col=2, include_index=False, include_column_header=False)
    if heroic_dataframe is not None:
        heroic_summary = get_boss_summary(heroic_data)
        summary_dataframe = pd.DataFrame(data=heroic_summary).transpose().sort_index()
        gd.set_with_dataframe(worksheet=summary_sheet, dataframe=summary_dataframe,
            row=3, col=5, include_index=False, include_column_header=False)
    if normal_dataframe is not None:
        normal_summary = get_boss_summary(normal_data)
        summary_dataframe = pd.DataFrame(data=normal_summary).transpose().sort_index()
        gd.set_with_dataframe(worksheet=summary_sheet, dataframe=summary_dataframe,
            row=3, col=8, include_index=False, include_column_header=False)




# Let's do this thing.
if __name__ == '__main__':
    main()
