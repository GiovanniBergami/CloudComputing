import string
from unidecode import unidecode
import timeit 

def generate_dictionary(interested_file) :
    characters_array, characters_dictionary = generate_characters_structures()
    
    characters_counter = 0

    with open(interested_file, "r", encoding="utf-8") as file:
        for line in file: # for each line of the file (which may include single sentences or paragraphs)
            for word in line.split() : # for each word of the line
                filtered_word = particular_characters(word) 
                for character in filtered_word : # for each character of the world
                    if character in characters_array :
                        characters_dictionary[character] = characters_dictionary[character] + 1
                        characters_counter = characters_counter + 1

    mapped_values = map(lambda x: x/characters_counter, characters_dictionary.values())
    final_dictionary = dict(zip(characters_dictionary.keys(), mapped_values))
    
    print(final_dictionary) 
    
    return final_dictionary

# Generate array with standard characters and dictionary with standard characters' counters
def generate_characters_structures() : 
    array = []
    dictionary = {}
    
    for character in string.ascii_lowercase :
        array.append(character)
        dictionary[character] = 0
        
    return [array, dictionary] 

# Replace particular characters in order to be considered in the counting
# The counting will consider only standard characters, so particular characters not considered in this function will not be counted
def particular_characters(string) :
    return unidecode(string)

print("Enter the interested file (Absolute path + filename): ")
interested_file = input() 

execution_time = timeit.timeit(lambda: generate_dictionary(interested_file), number=1)
print("Tempo di esecuzione "+ str(execution_time) + " secondi")