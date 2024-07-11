import requests
import time
import logging
from sqlalchemy import create_engine, text
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from uuid import UUID

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# API configuration
API_BASE_URL = "https://pokeapi.co/api/v2/pokemon/"
TOTAL_POKEMON = 1025  # ALL pokemons (until the 9th generation)
DB_CONNECTION_STRING = 'postgresql://postgres:Doggui3hous3@127.0.0.1/pokedex'  # Connecting to SQLAlchemy


# Connection to db
def get_db_connection():
    try:
        engine = create_engine(DB_CONNECTION_STRING)
        return engine
    except Exception as e:
        logging.error(f"Error connecting to db: {e}")
        return None


# Function to fetching all pokemons
def fetch_pokemon(pokemon_id):
    response = requests.get(f"{API_BASE_URL}{pokemon_id}")
    if response.status_code == 200:
        return response.json()
    else:
        logging.error(f"Error fetching Pokemon {pokemon_id}: {response.status_code}")
        return None


def optimize_tables(engine):
    with engine.connect() as conn:
        conn.execute(text("CREATE INDEX ID NOT EXISTS idx_pokemon_id ON pokemon (id)"))
        conn.execute(text("ANALYZE pokemon"))


# Function for create tables if they are not exist
def create_tables(engine):
    with engine.connect() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS pgcrypto"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS pokemon (
                serial_id BIGSERIAL PRIMARY KEY,
                random_id BIGINT UNIQUE NOT NULL,
                pokedex_number INTEGER UNIQUE NOT NULL,
                name VARCHAR(100) NOT NULL,
                height DECIMAL(5,2),
                weight DECIMAL(5,2),
                hp INTEGER,
                attack INTEGER,
                defense INTEGER,
                special_attack INTEGER,
                special_defense INTEGER,
                speed INTEGER
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS types (
                id SERIAL PRIMARY KEY,
                name VARCHAR(20) UNIQUE NOT NULL
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS pokemon_types (
                pokemon_id UUID REFERENCES pokemon(id),
                type_id INTEGER REFERENCES types(id),
                PRIMARY KEY (pokemon_id, type_id)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS abilities (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50) UNIQUE NOT NULL
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS pokemon_abilities (
                pokemon_id UUID REFERENCES pokemon(id),
                ability_id INTEGER REFERENCES abilities(id),
                is_hidden BOOLEAN NOT NULL,
                PRIMARY KEY (pokemon_id, ability_id)
            )
        """))


# Function for insert all pokemons into the tables
def insert_pokemon(engine, pokemon_data):
    with engine.connect() as conn:
        # Query for insert all info into the table 'pokemon'
        # If the pokemon exist already only update the missing columns or ignore the row if the info are full
        result = conn.execute(text("""
            INSERT INTO pokemon (random_id, pokedex_number, name, height, weight, hp, attack, defense, special_attack, special_defense, speed)
            VALUES (generate_random_id(), :pokedex_number, :name, :height, :weight, :hp, :attack, :defense, :special_attack, :special_defense, :speed)
            ON CONFLICT (pokedex_number) DO UPDATE SET
                random_id = generate_random_id(),
                name = EXCLUDED.name,
                height = EXCLUDED.height,
                weight = EXCLUDED.weight,
                hp = EXCLUDED.hp,
                attack = EXCLUDED.attack,
                defense = EXCLUDED.defense,
                special_attack = EXCLUDED.special_attack,
                special_defense = EXCLUDED.special_defense,
                speed = EXCLUDED.speed
            RETURNING id
        """), {
            'pokedex_number': pokemon_data['id'],
            'name': pokemon_data['name'],
            'height': pokemon_data['height'] / 10,
            'weight': pokemon_data['weight'] / 10,
            'hp': next(stat['base_stat'] for stat in pokemon_data['stats'] if stat['stat']['name'] == 'hp'),
            'attack': next(stat['base_stat'] for stat in pokemon_data['stats'] if stat['stat']['name'] == 'attack'),
            'defense': next(stat['base_stat'] for stat in pokemon_data['stats'] if stat['stat']['name'] == 'defense'),
            'special_attack': next(stat['base_stat'] for stat in pokemon_data['stats'] if stat['stat']['name'] == 'special-attack'),
            'special_defense': next(stat['base_stat'] for stat in pokemon_data['stats'] if stat['stat']['name'] == 'special-defense'),
            'speed': next(stat['base_stat'] for stat in pokemon_data['stats'] if stat['stat']['name'] == 'speed')
        })
        pokemon_id = result.fetchone()[0]

        if isinstance(pokemon_id, str):
            pokemon_id = UUID(pokemon_id)

        # Insert the types of each pokemon in the table 'types'
        for type_data in pokemon_data['types']:
            conn.execute(text("INSERT INTO types (name) VALUES (:name) ON CONFLICT (name) DO NOTHING"), {'name': type_data['type']['name']})
            result = conn.execute(text("SELECT id FROM types WHERE name = :name"), {'name': type_data['type']['name']})
            type_id = result.fetchone()[0]
            conn.execute(text("INSERT INTO pokemon_types (pokemon_id, type_id) VALUES (:pokemon_id, :type_id) ON CONFLICT DO NOTHING"), 
                         {'pokemon_id': pokemon_id, 'type_id': type_id})

        # Insert the abilities for each pokemon in the table 'abilities'
        for ability_data in pokemon_data['abilities']:
            conn.execute(text("INSERT INTO abilities (name) VALUES (:name) ON CONFLICT (name) DO NOTHING"), 
                         {'name': ability_data['ability']['name']})
            result = conn.execute(text("SELECT id FROM abilities WHERE name = :name"), {'name': ability_data['ability']['name']})
            ability_id = result.fetchone()[0]
            conn.execute(text("""
                INSERT INTO pokemon_abilities (pokemon_id, ability_id, is_hidden)
                VALUES (:pokemon_id, :ability_id, :is_hidden)
                ON CONFLICT DO NOTHING
            """), {'pokemon_id': pokemon_id, 'ability_id': ability_id, 'is_hidden': ability_data['is_hidden']})


# Function for collect all the data and create the tables for the images
def collect_pokemon_data(engine):
    create_tables(engine)
    # Searching every index of each pokemon in the API (TOTAL_POKEMON=1025)
    for pokemon_id in range(1, TOTAL_POKEMON + 1):
        pokemon_data = fetch_pokemon(pokemon_id)
        if pokemon_data:
            insert_pokemon(engine, pokemon_data)
            logging.info(f"Processed {pokemon_id}: {pokemon_data['name']}")
        else:
            logging.warning(f"Failed to fetch Pokemon {pokemon_id}")
        time.sleep(0.3)  # Avoiding overload in the API


# Function for load all the data of each pokemon
def load_pokemon_data(engine):
    query = """
    SELECT p.*,
           string_agg(DISTINCT t.name, ', ' ORDER BY t.name) as types
    FROM pokemon p
    LEFT JOIN pokemon_types pt ON p.id = pt.pokemon_id
    LEFT JOIN types t ON pt.type_id = t.id
    GROUP BY p.id
    """
    return pd.read_sql_query(query, engine)


# Function to print the distribution of the info of the pokemons (attack, defense, etc)
def plot_distributions(df):
    fig, axs = plt.subplots(2, 4, figsize=(20, 10))
    axs = axs.flatten()

    for i, col in enumerate(['weight', 'height', 'attack', 'speed', 'hp', 'special_attack', 'special_defense', 'defense']):
        sns.histplot(df[col], ax=axs[i], kde=True)
        axs[i].set_title(f'Distribution of {col.capitalize()}')

    plt.tight_layout()
    plt.savefig('pokemon_distributions.png')
    plt.close()


# Function for print the all combinatiod of types for the pokemons
def plot_type_combinations(df):
    type_counts = df['types'].value_counts().head(20)
    plt.figure(figsize=(12, 8))
    sns.barplot(x=type_counts.values, y=type_counts.index)
    plt.title('Top 20 Type Combinations')
    plt.xlabel('Count')
    plt.ylabel('Type Combination')
    plt.tight_layout()
    plt.savefig('type_combinations.png')
    plt.close()


# Function for made the analysis for the correlation between HP and Height and Weight
def correlation_analysis(df):
    corr_hp_height = df['hp'].corr(df['height'])
    corr_hp_weight = df['hp'].corr(df['weight'])

    print(f"Correlation between HP and Height: {corr_hp_height}")
    print(f"Correlation between HP and Weight: {corr_hp_weight}")

    plt.figure(figsize=(12, 5))

    plt.subplot(1, 2, 1)
    plt.scatter(df['height'], df['hp'])
    plt.xlabel('Height')
    plt.ylabel('HP')
    plt.title('HP vs Height')

    plt.subplot(1, 2, 2)
    plt.scatter(df['weight'], df['hp'])
    plt.xlabel('Weight')
    plt.ylabel('HP')
    plt.title('HP vs Weight')

    plt.tight_layout()
    plt.savefig('hp_correlations.png')
    plt.close()


# Main function
def main():
    engine = get_db_connection()
    if not engine:
        return

    # Picking the data
    collect_pokemon_data(engine)
    optimize_tables(engine)

    # Load the data for the analysis
    df = load_pokemon_data(engine)
    if df is None or df.empty:
        logging.error("Failed to load data. Exiting.")
        return

    # Makes the analysis
    plot_distributions(df)
    plot_type_combinations(df)
    correlation_analysis(df)


if __name__ == "__main__":
    main()
