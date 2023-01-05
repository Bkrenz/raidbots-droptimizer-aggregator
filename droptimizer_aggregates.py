import requests
import csv
import os
import pandas as pd
from blizzardapi import BlizzardApi

import gspread as gs
import gspread_dataframe as gd

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


def import_reports():
    '''
    Downloads each report listed in the reports.txt file and writes to a csv file
    '''
    with open('data/reports.txt', 'r') as reports:
        for report in reports:
            report = report.strip()
            print('Downloading report ' + report + '.')
            # Download report csv
            download = None
            with requests.Session() as s:
                download = s.get(report + '/data.csv')
            decoded_content = download.content.decode('utf-8')

            # Find the player name
            cr = csv.reader(decoded_content.splitlines(), delimiter=',')
            my_list = list(cr)
            report_name = my_list[1][0]

            # Write to File
            print('Writing report ' + report + ' to file ' + report_name + '.csv')
            with open('data/' + report_name + '.csv', 'w') as file:
                file.write(decoded_content)


item_list = {}
def get_item(id):
    if id in tier_list:
        id = tier_list[id]
    if id not in item_list:
        item = api_client.wow.game_data.get_item('us', 'en_US', id)
        item_list[id] = item['name']
    return item_list[id]


boss_list = {
    '-44': 'Trash',
}
def get_boss(id):
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
        sim_name = sim[0]
        name_list = sim_name.split('/')
        sim_dps = float(sim[1])
        sim_diff = (sim_dps - baseline_dps)

        # Get the Boss Name
        boss_id = name_list[1]
        boss_name = get_boss(boss_id)
        
        # Get the Sim Item
        item_id = name_list[3]
        item_name = boss_name + ' - ' + get_item(item_id)

        # Add to dictionary
        if item_name in report_data:
            report_data[item_name] = max(sim_diff, report_data[item_name])
        else:
            report_data[item_name] = sim_diff

    return player, report_data


def parse_reports():
    '''
    Iterates through the reports for parsing
    '''
    print('Parsing reports...')
    reports = [x for x in os.listdir(os.curdir + '/data') if x.endswith('.csv')]
    all_data = {}
    for report in reports:
        with open('data/' + report) as csvfile:
            cr = csv.reader(csvfile, delimiter=',')
            player, data = parse_report(list(cr))
            all_data[player] = data
    dataframe = pd.DataFrame(data=all_data)

    ws = gc.open(spreadsheet_name).worksheet("windbridge")
    ws.clear()
    gd.set_with_dataframe(worksheet=ws,dataframe=dataframe,include_index=True,include_column_header=True,resize=True)


# Let's do this thing.
if __name__ == '__main__':
    import_reports()
    parse_reports()
