from pymongo import MongoClient
mongoClient = MongoClient("mongodb://localhost/pokemon")
pokemonDB = mongoClient['pokemondb']
pokemonColl = pokemonDB['pokemon_data']

# Query.py: Contains the 3 queries specified in the README.md
# Write a query that returns all the Pokemon named "Pikachu". (1pt)
pikachus = []
pikachu = pokemonColl.find({"name": "Pikachu"})
for i in pikachu:
    pikachus.append(i)
print(pikachus)

# Write a query that returns all the Pokemon with an attack greater than 150. (1pt)
attack150Greater = []
attack150 = pokemonColl.find({"attack" : { "$gt": 150 }})
for i in attack150:
    attack150Greater.append(i)
print(attack150Greater)

# Write a query that returns all the Pokemon with an ability of "Overgrow" (1pt)
pokemonAbilityOvergrow = []
abilityOvergrow = pokemonColl.find({"abilities" : {"$in":["Overgrow"]}})
for i in abilityOvergrow:
    pokemonAbilityOvergrow.append(i)
print(pokemonAbilityOvergrow)
