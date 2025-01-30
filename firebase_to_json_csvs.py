import firebase_admin
from firebase_admin import credentials, firestore
import json
import os
import json
import csv
import json
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
import json
from datetime import datetime
import json
from datetime import datetime
import pandas as pd
import json


def main():
    # Initialize Firebase Admin SDK
    firebase_cred_path = "perkcup-689f9-firebase-adminsdk-yuf7v-c17798a18e.json"  # Replace with your Firebase Admin JSON
    if not firebase_admin._apps:
        cred = credentials.Certificate(firebase_cred_path)
        firebase_admin.initialize_app(cred)

    # Firestore client
    db = firestore.client()

    def fetch_all_documents(collection_ref):
        """Recursively fetch all documents and their subcollections."""
        data = {}
        try:
            for doc in collection_ref.stream():
                doc_id = doc.id
                print('doc :' , doc_id)
                doc_data = doc.to_dict()
                data[doc_id] = doc_data

                # Check for subcollections
                subcollections = db.collection(collection_ref.id).document(doc_id).collections()
                for subcollection in subcollections:
                    subcollection_name = subcollection.id
                    data[doc_id][subcollection_name] = fetch_all_documents(subcollection)

            return data
        except Exception as e:
            print(f"Error fetching documents: {e}")
            return {}

    def fetch_full_database():
        """Fetch the full Firestore database content."""
        database_content = {}
        try:
            # Fetch all top-level collections
            collections = db.collections()
            for collection in collections:
                collection_name = collection.id
                print(collection_name)
                database_content[collection_name] = fetch_all_documents(collection)
            return database_content
        except Exception as e:
            print(f"Error fetching database: {e}")
            return {}



    def save_to_file(data, filename):
        """Save the fetched data to a JSON file."""
        def default_serializer(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()  # Convert datetime to ISO 8601 string
            if hasattr(obj, '__dict__'):
                return obj.__dict__  # Serialize objects with `__dict__` attribute
            return str(obj)  # Fallback to string representation

        try:
            with open(filename, 'w') as file:
                json.dump(data, file, indent=4, default=default_serializer)
            print(f"Database content saved to {filename}")
        except Exception as e:
        
            print(f"Error saving to file: {e}")

    print("Fetching full Firestore database content...")
    full_database_content = fetch_full_database()
    print("Saving...")
    # Save the fetched data to a JSON file
    save_to_file(full_database_content, "firestore_full_database.json")





    # Initialize Firebase Admin SDK
    firebase_cred_path = "perkcup-689f9-firebase-adminsdk-yuf7v-c17798a18e.json"  # Replace with your Firebase Admin JSON
    if not firebase_admin._apps:
        cred = credentials.Certificate(firebase_cred_path)
        firebase_admin.initialize_app(cred)

    # Firestore client
    db = firestore.client()

    def fetch_blocks_collection():
        """Fetch all documents in the 'blocks' collection."""
        blocks_data = {}
        try:
            blocks_collection = db.collection('blocks')
            for doc in blocks_collection.stream():
                doc_id = doc.id
                print(f"Fetching document: {doc_id}")
                try:
                    doc_data = doc.to_dict()
                    blocks_data[doc_id] = doc_data
                    
                    # Fetch subcollections for each document
                    subcollections = db.collection('blocks').document(doc_id).collections()
                    for subcollection in subcollections:
                        subcollection_name = subcollection.id
                        print(f"Fetching subcollection: {subcollection_name}")
                        blocks_data[doc_id][subcollection_name] = {
                            sub_doc.id: sub_doc.to_dict() for sub_doc in subcollection.stream()
                        }
                except Exception as e:
                    print(f"Error processing document {doc_id}: {e}")

            return blocks_data
        except Exception as e:
            print(f"Error fetching 'blocks' collection: {e}")
            return {}

    def save_to_file(data, filename):
        """Save the fetched data to a JSON file."""
        def default_serializer(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()  # Convert datetime to ISO 8601 string
            return str(obj)  # Fallback to string representation

        try:
            with open(filename, 'w') as file:
                json.dump(data, file, indent=4, default=default_serializer)
            print(f"Database content saved to {filename}")
        except Exception as e:
            print(f"Error saving to file: {e}")

    print("Fetching 'blocks' collection content...")
    blocks_content = fetch_blocks_collection()
    print("Saving...")
    save_to_file(blocks_content, "blocks_collection.json")



    import pandas as pd
    import json

    def load_json(file_path):
        """
        Load JSON data from a file.
        """
        with open(file_path, 'r') as file:
            return json.load(file)

    def extract_tournaments_data(data):
        """
        Extract tournaments data from the loaded JSON, including details about players and other event information.
        """
        tournaments_data = []
        for year, tours in data.get('tournaments', {}).items():
            for tour, events in tours.items():
                for event_id, event_details in events.items():
                    # General tournament details
                    tournament_info = {
                        'year': year,
                        'tour': tour,
                        'event_name': event_details.get('event_name'),
                        'event_id': event_id,
                        'isUpcoming': event_details.get('isUpcoming'),
                        'finish_date': event_details.get('finish_date'),
                        'last_fetched': event_details.get('last_fetched')
                    }
                    
                    # Include players' data if available
                    players = event_details.get('website_info', {}).get('players', [])
                    for player in players:
                        player_info = {
                            'player_name': player.get('Name'),
                            'finish_position': player.get('Finish_Position'),
                            'perkcup_points': player.get('PerkCup_Value'),
                            'sg_total': player.get('SG_Total'),
                            'sg_app': player.get('SG_APP'),
                            'sg_ott': player.get('SG_OTT'),
                            'sg_putt': player.get('SG_PUTT'),
                            'sg_arg': player.get('SG_ARG'),
                            'field': player.get('Field'),
                            'comment': player.get('Comment')
                        }
                        # Combine tournament info with player info
                        tournaments_data.append({**tournament_info, **player_info})
                        
                    # If no players, just append the tournament info
                    if not players:
                        tournaments_data.append(tournament_info)

        return pd.DataFrame(tournaments_data)

    def extract_players_data(data):
        """
        Extract players data from the loaded JSON.
        """
        players_data = []
        for year, tours in data.get('tournaments', {}).items():
            for tour, events in tours.items():
                for event_id, event_details in events.items():
                    for player in event_details.get('Results', {}).get('scores', []):
                        player_data = {
                            'event_id': event_details.get('event_id'),
                            'player_name': player.get('player_name'),
                            'dg_id': player.get('dg_id'),
                            'final_position': player.get('fin_text')
                        }

                        # Add round details
                        for round_num in range(1, 5):
                            round_key = f'round_{round_num}'
                            round_details = player.get(round_key, {})
                            player_data.update({
                                f'round_{round_num}_score': round_details.get('score'),
                                f'round_{round_num}_sg_app': round_details.get('sg_app'),
                                f'round_{round_num}_course_par': round_details.get('course_par'),
                                f'round_{round_num}_driving_acc': round_details.get('driving_acc'),
                                f'round_{round_num}_teetime': round_details.get('teetime'),
                                f'round_{round_num}_sg_total': round_details.get('sg_total'),
                                f'round_{round_num}_sg_arg': round_details.get('sg_arg'),
                                f'round_{round_num}_gir': round_details.get('gir'),
                                f'round_{round_num}_sg_putt': round_details.get('sg_putt'),
                                f'round_{round_num}_prox_fw': round_details.get('prox_fw'),
                                f'round_{round_num}_scrambling': round_details.get('scrambling'),
                                f'round_{round_num}_course_num': round_details.get('course_num'),
                                f'round_{round_num}_great_shots': round_details.get('great_shots'),
                                f'round_{round_num}_course_name': round_details.get('course_name'),
                                f'round_{round_num}_sg_ott': round_details.get('sg_ott'),
                                f'round_{round_num}_start_hole': round_details.get('start_hole'),
                                f'round_{round_num}_prox_rgh': round_details.get('prox_rgh'),
                                f'round_{round_num}_poor_shots': round_details.get('poor_shots'),
                                f'round_{round_num}_driving_dist': round_details.get('driving_dist'),
                                f'round_{round_num}_sg_t2g': round_details.get('sg_t2g')
                            })
                        players_data.append(player_data)
        return pd.DataFrame(players_data)

    def extract_players_info(data):
        """
        Extract players information from the loaded JSON.
        """
        players_data = data.get("players", {})
        return pd.DataFrame.from_dict(players_data, orient='index')

    def save_to_csv(df, file_name):
        """
        Save a DataFrame to a CSV file.
        """
        df.to_csv(file_name, index=False)
        print(f"CSV file '{file_name}' has been created.")

    def main1():
        # Load the JSON data
        data = load_json('firestore_full_database.json')

        # Extract tournaments and players data
        tournaments_df = extract_tournaments_data(data)
        #players_df = extract_players_data(data)
        players_info_df = extract_players_info(data)

        # Save to CSV
        # Define the required columns
        required_columns = [
            "year", "tour","finish_date", "event_name", "event_id", "last_fetched", 
            "player_name", "finish_position", "perkcup_points", 
            "sg_total", "sg_arg"
        ]

        # Filter the dataframe
        tournaments_df = tournaments_df[required_columns]  # Keep only required columns
        tournaments_df = tournaments_df[tournaments_df["tour"] == "pga"]  # Keep only rows where tour == 'pga'

        save_to_csv(tournaments_df, 'functions/CSV DATA/tournaments.csv')
        #save_to_csv(players_df, 'events.csv')
        save_to_csv(players_info_df, 'functions/CSV DATA/players_info.csv')


    main1()
    import json
    import csv

    # Load JSON data from file
    with open("firestore_full_database.json", "r") as file:
        data = json.load(file)

    # Extract relevant tournament data
    tournaments = data.get("2025_tournaments", {})

    tournament_rows = []

    for tournament_id, tournament_data in tournaments.items():
        players = tournament_data.get("players", {})
        for player_id, player_data in players.items():
            tournament_rows.append([
                tournament_id,
                2025,
                player_data.get("player_name", ""),
                player_data.get("sg_total", 0),
                player_data.get("perkcup_points", 0),
                player_data.get("fin_text", "")
            ])

    # Save to CSV
    csv_filename = "functions/CSV DATA/2025_tournaments_data.csv"
    with open(csv_filename, "w", newline="") as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(["event_id", "year","player_name", "sg_total", "perkcup_points", "finish_position"])
        csv_writer.writerows(tournament_rows)

    print(f"CSV file '{csv_filename}' created successfully.")
    # Load both CSV files
    df1 = pd.read_csv('functions/CSV DATA/2025_tournaments_data.csv')
    df2 = pd.read_csv('functions/CSV DATA/tournaments.csv')
    # Concatenate while keeping all columns from both DataFrames

    combined_df = pd.concat([df1, df2], axis=0, ignore_index=True)
    id_to_name = {
        6: "Sony Open in Hawaii",
        2: "The American Express",
        16: "The Sentry",
        4 : "Farmers Insurance Open"
    }

    # Fill missing event_name values based on event_id
    combined_df['event_name'] = combined_df.apply(lambda row: id_to_name.get(row['event_id'], row['event_name']), axis=1)

    # Save the merged DataFrame to a new CSV file
    combined_df.to_csv('functions/CSV DATA/tournaments.csv', index=False)


    import json
    import pandas as pd

    # Function to load JSON data
    def load_json(file_path):
        """
        Load JSON data from a file.
        """
        with open(file_path, 'r') as file:
            return json.load(file)

    # Load JSON file
    file_path = "firestore_full_database.json"  # Replace with your file path
    data = load_json(file_path)

    # Extract scores data
    scores = data.get("scores", {})

    # Process all events
    rows = []
    for event_id, event_data in scores.items():
        years = event_data.get("years", {})
        for year, year_details in years.items():
            event_name = year_details.get("event_name", "N/A")
            tour = year_details.get("tour", "N/A")
            tournament_id = year_details.get("tournament_id", "N/A")
            date = year_details.get("date", "N/A")
            if date == "N/A":
                print(f"Missing date for event ID: {event_id}, year: {year}")
            if date != "N/A":
                print(f"Event ID: {event_id}, year: {year}, date: {date}")
            actual_results = year_details.get("actual_results", [])

            for player in actual_results:
                player_name = player.get("name", "N/A")
                total_score = player.get("total_score", 0)
                fin_text = player.get("fin_text", "N/A")
                strokes_gained = player.get("strokes_gained", 0)
                percup_value = player.get("perkcup_value", 0)  # Include `perkcup_value` with default 0

                # Add player details to rows
                rows.append({
                    "Event ID": event_id,
                    "Year": year,
                    "Event Name": event_name,
                    "Tour": tour,
                    "Tournament ID": tournament_id,
                    "Date": date,
                    "Player Name": player_name,
                    "Total Score": total_score,
                    "Final Position": fin_text,
                    "Strokes Gained": strokes_gained,
                    "Perkcup Value": percup_value,
                })

    # Create a DataFrame and save it as a CSV
    output_file = "functions/CSV DATA/all_event_scores.csv"
    df = pd.DataFrame(rows)
    df.to_csv(output_file, index=False)
    print(f"All event scores data saved to {output_file}")
    import json
    import csv

    def load_json(file_path):
        """
        Load JSON data from a file.
        """
        with open(file_path, 'r') as file:
            return json.load(file)

    import pandas as pd
    import json

    def save_blocks_with_names(blocks, output_file):
        # Extract PerkCup Points data
        perkcuppoints_data = []
        for block_key, block_data in blocks.items():
            for id_key, player_data in block_data.get("perkcup_points", {}).items():
                for year, points in player_data.items():
                    year_cleaned = year  # Keep the year as-is
                    perkcuppoints_data.append({"Block": block_key, "PlayerID": id_key, "Year": year_cleaned, "PerkCupPoints": points})
        
        # Convert to DataFrame and pivot years into columns
        perkcuppoints_df = pd.DataFrame(perkcuppoints_data)
        perkcuppoints_pivot = perkcuppoints_df.pivot(index=["Block", "PlayerID"], columns="Year", values="PerkCupPoints").reset_index()

        # Extract Players' Historical Finish Positions
        players_data = []
        for block_key, block_data in blocks.items():
            for player_name, player_details in block_data.get("players", {}).items():
                for year_tournament, position in player_details.get("historicalFinishPositions", {}).items():
                    year, tournament = map(str, year_tournament.split(","))
                    players_data.append({
                        "Block": block_key,
                        "PlayerName": player_name,
                        "Year": year,
                        "Tournament": tournament,
                        "Position": position,
                        "Participations": player_details.get("participations", 0),
                        "TotalFieldPoints": round(player_details.get("currentblocktotalfieldpoints", 0), 2)  # Format to 2 decimal places
                    })

        # Convert to DataFrame
        players_df = pd.DataFrame(players_data)

        # Load Firestore player data for names and IDs
        firestore_file_path = 'firestore_full_database.json'
        with open(firestore_file_path, 'r') as f:
            firestore_data = json.load(f)
        
        # Extract player IDs and names
        players_ids_data = []
        for player_id, player_info in firestore_data.get("players", {}).items():
            players_ids_data.append({
                "PlayerID": player_id,
                "PlayerName": player_info.get("player_name", "Unknown")
            })
        players_ids_df = pd.DataFrame(players_ids_data)

        # Merge PerkCup Points with player IDs and names
        combined_perkcuppoints_df = pd.merge(perkcuppoints_pivot, players_ids_df, on="PlayerID", how="left")

        # Merge the combined PerkCup Points with players' data on PlayerName
        final_combined_df = pd.merge(players_df, combined_perkcuppoints_df, on="PlayerName", how="left")

        # Save the combined data to the specified output file
        final_combined_df.to_csv(output_file, index=False)
        print(f"Combined data saved to {output_file}")

    # Example usage
    # Load blocks data
    blocks_file_path = 'blocks_collection.json'
    with open(blocks_file_path, 'r') as f:
        blocks_data = json.load(f)

    # Save combined CSV
    output_file = "functions/CSV DATA/blocks_with_names.csv"
    save_blocks_with_names(blocks_data, output_file)

    import os
    import pandas as pd

    # Path to your CSV data directory
    directory = 'functions/CSV DATA'

    # Iterate through all files in the directory
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            file_path = os.path.join(directory, filename)
            
            # Load the CSV file
            df = pd.read_csv(file_path)
            
            # Rename columns: replace spaces with underscores
            df.columns = [col.replace(' ', '_') for col in df.columns]
            
            # Save the modified DataFrame back to the same file
            df.to_csv(file_path, index=False)
            print(f"Processed: {filename}")
    import pandas as pd

    # Load the CSV files
    events_scores_path = 'functions/CSV DATA/all_event_scores.csv'
    tournaments_path = 'functions/CSV DATA/tournaments.csv'
    # Read the data
    events_scores_df = pd.read_csv(events_scores_path)
    tournaments_df = pd.read_csv(tournaments_path)
    print(tournaments_df[tournaments_df["year"]==2025])

    # Normalize column names for joining
    tournaments_df.rename(columns={
        'event_name': 'Event_Name',
        'tour': 'Tour',
        'year': 'Year',
        'event_id': 'Event_ID',
        'player_name': 'Player_Name'
    }, inplace=True)

    # Perform the merge (inner join based on Event_ID and Player_Name)
    merged_df = pd.merge(
        tournaments_df,
        events_scores_df,

        on=['Event_ID', 'Player_Name'],
        how='inner',
        suffixes=('', '_duplicate')  # Avoid suffixes where possible
    )

    # Remove duplicated columns explicitly
    columns_to_keep = [col for col in merged_df.columns if not col.endswith('_duplicate')]
    merged_df = merged_df[columns_to_keep]
    columns_to_keep
    # Drop specified columns
    merged_df.drop(columns=[ 'last_fetched', 'Final_Position'], inplace=True)
    # Extract 'Month' and 'Day' from 'Date' column
    merged_df['Month'] = pd.to_datetime(merged_df['Date']).dt.month
    merged_df['Day'] = pd.to_datetime(merged_df['Date']).dt.day
    merged_df.drop(columns=['Date'], inplace=True)

    # Rename ambiguous columns to make them more descriptive (if needed)
    merged_df.rename(columns={
        'Year': 'Event_Year',
        'Tour': 'Event_Tour',
        'Event_Name': 'Event_Name_Final'
    }, inplace=True)

    # Reorder columns to place 'Month' and 'Day' after 'Event_Year'
    columns_order = ['Event_ID', 'Player_Name', 'Event_Year', 'Month', 'Day'] + \
        [col for col in merged_df.columns if col not in ['Event_ID', 'Player_Name', 'Event_Year', 'Month', 'Day']]
    merged_df = merged_df[columns_order]

    # Save the cleaned dataframe
    merged_df.to_csv("functions/CSV DATA/tours_events.csv", index=False)

    import pandas as pd

    # Load the datasets
    players_info_path = 'functions/CSV DATA/players_info.csv'
    tours_events_path = 'functions/CSV DATA/tours_events.csv'

    players_info_df = pd.read_csv(players_info_path)
    tours_events_df = pd.read_csv(tours_events_path)

    # Harmonize column names for clarity
    clear_column_names = {
        "sample_size": "SampleSize",
        "firstName": "FirstName",
        "lastName": "LastName",
        "countryCode": "CountryCode",
        "primary_tour": "PrimaryTour",
        "player_name": "PlayerName"
        # Add other renaming rules if needed
    }

    players_info_df.rename(columns=clear_column_names, inplace=True)

    # Join the datasets on Player_Name/PlayerName
    combined_df = pd.merge(
        tours_events_df,
        players_info_df,
        left_on="Player_Name",
        right_on="PlayerName",
        how="left"
    )

    # Save the combined dataframe to a CSV file
    combined_df.to_csv("functions/CSV DATA/tours_events_players.csv", index=False)
    file_path = "functions/CSV DATA/tours_events_players.csv"
    df = pd.read_csv(file_path)

    # Clean up column names
    df.columns = [
        col.strip().replace(" ", "_").lower()  # Normalize column names
        for col in df.columns
    ]

    # Save the cleaned DataFrame back to a CSV file if needed
    # Drop columns starting with specific prefixes
    prefixes_to_drop = ('100', '150', '50', 'under', 'over' , "playerskillratings")
    columns_to_drop = [col for col in df.columns if col.startswith(prefixes_to_drop)]
    df = df.drop(columns=columns_to_drop)

    # Drop duplicates based on the specified columns
    df = df.drop_duplicates(subset=["event_id", "player_name", "event_year", "month", "day", "event_name_final", "event_tour"], keep="first")

    df['finish_date'] = pd.to_datetime(df['finish_date'], format="%d/%m/%Y", errors='coerce')

    # Fill 'month' column with month extracted from 'finish_date'
    df['month'] = df['finish_date'].dt.month

    # Save the cleaned DataFrame
    cleaned_file_path = "functions/CSV DATA/tours_events_players.csv"
    df.to_csv(cleaned_file_path, index=False)

    print(f"Cleaned CSV saved to {cleaned_file_path}")
    print("combined tours_events_players is saved ")
    df[df["event_year"]==2025]




if __name__ == "__main__":
    print("Fetching collections and Saving to Csvs...")
    main()