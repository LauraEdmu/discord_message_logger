import re

WORD_SUBS = {
    'color': 'colour',
    'flavor': 'flavour',
    'neighbor': 'neighbour',
    'center': 'centre',
    'theater': 'theatre',
    'of': 'o\'',
}

def brittishify(intext: str) -> tuple[str, bool]:   
    words = re.findall(r'\w+', intext)
    if WORD_SUBS.keys() & set(words):
        for word in words:
            if word in WORD_SUBS:
                intext = intext.replace(word, WORD_SUBS[word])
    
    # replace "t" with "'"
    outtext = re.sub(r't', '\'', intext, flags=re.IGNORECASE)

    outtext = re.sub(r'\'{2,}', '\'', outtext)

    changed = outtext != intext
    return outtext, changed

if __name__ == "__main__": # test cases
    intext = "The color of the theater's center is a flavor of neighbor."
    outtext = brittishify(intext)
    print(outtext)

    intext = "Can I have a botTle of water?"
    outtext = brittishify(intext)
    print(outtext)