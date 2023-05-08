import functools
import operator
import sqlite3  # This is the package for all sqlite3 access in Python
connection = sqlite3.connect("pokemon.sqlite")
con = connection.cursor()

from pymongo import MongoClient
mongoClient = MongoClient("mongodb://localhost/pokemon")
pokemonDB = mongoClient['pokemondb']
pokemonColl = pokemonDB['pokemon_data']


result_set = con.execute("SELECT pokedex_number FROM pokemon").fetchall()
pokemonSet = functools.reduce(operator.add, result_set)
pokemonList = pokemonSet[0:]

# Import.py: take data from sqlite DB and import it into MongoDB collection/documents
for i in pokemonList:

    # pokemonRow/general query
    # Making variables for name and types
    pokemonName = con.execute("SELECT name FROM pokemon WHERE pokedex_number = " + str(i)).fetchone()
    pokemonNameString = functools.reduce(operator.add, pokemonName)

    pokemonType1 = con.execute("SELECT type1 FROM pokemon_types_view JOIN pokemon ON pokemon_types_view.name = pokemon.name WHERE pokedex_number = " + str(i)).fetchone()
    pokemonType1String = functools.reduce(operator.add, pokemonType1)

    pokemonType2 = con.execute("SELECT type2 FROM pokemon_types_view JOIN pokemon ON pokemon_types_view.name = pokemon.name WHERE pokedex_number = " + str(i)).fetchone()
    pokemonType2String = functools.reduce(operator.add, pokemonType2)

    pokemonHP = con.execute("SELECT hp FROM pokemon WHERE pokedex_number = " + str(i)).fetchone()
    pokemonHPString = functools.reduce(operator.add, pokemonHP)

    pokemonAttack = con.execute("SELECT attack FROM pokemon WHERE pokedex_number = " + str(i)).fetchone()
    pokemonAttackString = functools.reduce(operator.add, pokemonAttack)

    pokemonDefense = con.execute("SELECT defense FROM pokemon WHERE pokedex_number = " + str(i)).fetchone()
    pokemonDefenseString = functools.reduce(operator.add, pokemonDefense)

    pokemonSpeed = con.execute("SELECT speed FROM pokemon WHERE pokedex_number = " + str(i)).fetchone()
    pokemonSpeedString = functools.reduce(operator.add, pokemonSpeed)

    pokemonSpecialAttack = con.execute("SELECT sp_attack FROM pokemon WHERE pokedex_number = " + str(i)).fetchone()
    pokemonSpecialAttackString = functools.reduce(operator.add, pokemonSpecialAttack)

    pokemonSpecialDefense = con.execute("SELECT sp_defense FROM pokemon WHERE pokedex_number = " + str(i)).fetchone()
    pokemonSpecialDefenseString = functools.reduce(operator.add, pokemonSpecialDefense)

    generalquery = [pokemonNameString, i, pokemonType1String, pokemonType2String, pokemonHPString, pokemonAttackString, pokemonDefenseString, pokemonSpeedString, pokemonSpecialAttackString, pokemonSpecialDefenseString]

    # ability query
    # Importing abilities hints:
        # 1. You want to make use of the pokemon_abilities, abilities, and pokemon tables (through a join) to match pokedex_numbers to their abilities
        # 2. You should use one of the results of the general query you write in the WHERE clause of your ability query (to ensure only the abilities for the pokemon you have in mind are returned)
        # 3. The abilities query should be nested inside the for loop
    pokemonAbilities = con.execute("SELECT DISTINCT ability.name FROM pokemon JOIN pokemon_abilities ON pokemon.id = pokemon_abilities.pokemon_id JOIN ability ON pokemon_abilities.ability_id = ability.id WHERE pokemon.id = " + str(i)).fetchall()
    abilityquery = functools.reduce(operator.add, pokemonAbilities)

    # getting rid of tuples
    abilities = []
    for i in range(len(abilityquery)):
        abilities.append(abilityquery[i])


    pokemon = {
       "name": generalquery[0],
       "pokedex_number": generalquery[1],
       "types": [generalquery[2], generalquery[3]],
       "hp": generalquery[4],
       "attack": generalquery[5],
       "defense": generalquery[6],
       "speed": generalquery[7],
       "sp_attack": generalquery[8],
       "sp_defense": generalquery[9],
       "abilities": abilities
   }
    
    pokemonColl.insert_one(pokemon)
